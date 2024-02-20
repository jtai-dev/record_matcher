from collections import defaultdict, Counter
from collections.abc import Generator, Callable

from . import records
from .config import MatcherConfig


def column_match(
    x_record: dict[str, str],
    y_records: dict[int, dict[str, str]],
    x_column: str,
    y_columns: list[str],
    scorer: Callable[[str, str], int | float],
    threshold: int | float = 0,
    cutoff: bool = False,
) -> Generator[tuple[str, float]]:
    """Finds matching records from y_records that matches the key(column) in
    x_record.

    The results of matching records will depend on the scorer that is
    used. For example, an exact scorer will only return matching
    records that has an exact string match (matching_score=1.0).

    When the column of x_record is compared with multiple columns of the y_record,
    the y_record column with the highest matching score (when compared with
    a x_record) column will be taken.

    The function performs a final check to see if cutoffs==True, if so, scores
    that exceeds the given threshold is considered a potential match, otherwise
    defaults to returning anything that is greater than 0.

    Parameters
    ----------
    x_record: dict[x_column, x_val]
        A single record from x_records

    y_records: dict[y_index, dict[y_column, y_val]]
        All of y_records

    x_column: str
        A column (key) that exist in x_record to be compared with a value
        in y_column.

    y_columns: list[y_column]
        List of keys (columns) that exist in y_records to be compared with
        the value in x column.

    scorer: Callable[[x_val, y_val], matching_score]
        A function that contains two parameters x and y each corresponding
        to the value in x_column and value in y_column respectively,
        and returns zero or a positive number showing the matching score
        between x and y values.

    threshold: int or float >=0, default=0
        A number that is found within the range of values produced by the
        scorer and represents the desired score for the intended column.

    cutoff: bool, default=False
        When cutoff is set to True, the matching score produced by the
        scorer  will be subjected to the threshold, such that if the
        score is less than  threshold, it will not be considered a
        legitimate match.

    Returns
    -------
    Generator[tuple[y_index, matching_score]]
        Iterator of tuples that contains the y_index and the scores
        of the matching y_record.
    """

    # Contains all the indices and matching score of y_records to be compared
    scores = []

    for y_index, y_record in y_records.items():
        column_scores = []
        for y_column in y_columns:
            column_scores.append(
                scorer(
                    str(x_record[x_column] if x_column in x_record else ""),
                    str(y_record[y_column] if y_column in y_record else ""),
                )
            )
        # Takes only the best score out of the y_columns matched
        scores.append((y_index, max(column_scores) if column_scores else 0))

    if cutoff:
        return ((y_index, score) for y_index, score in scores if score >= threshold)
    else:
        return ((y_index, score) for y_index, score in scores if score > 0)


def records_match(
    x_records: dict[int, dict[str, str]],
    y_records: dict[int, dict[str, str]],
    columns_to_match: dict[str, list[str]],
    columns_to_group: dict[str, str],
    scorers: dict[str, Callable[[str, str], float]],
    thresholds: dict[str, int | float],
    cutoffs: dict[str, bool],
) -> tuple[int, list[tuple[int, float]], float]:
    """Finds matching records from y_records that matches all records in
    x_records

    While the column_match function calculates the score for a single column
    in x_record, this function iterates through each column in x_record and
    compute the row score (sum of the column scores). On top of what column_match
    already does, it considers and computes a few more factors such as:

          i) Number of unique values in each colum
         ii) y_records that are to be grouped by a value found in a x_column

    The return results is determined by the factors above, it consist of
    matched y_record(s) and is mapped to each index of the x_record along
    with the optimal threshold.

    Parameters
    ----------
    x_records: dict[x_index, dict[x_column, x_value]]

    y_records: dict[y_index, dict[y_column, y_value]]

    columns_to_match: dict[x_column, list[y_columns]]
        Maps columns in x_records (x_column) to multiple columns of
        y_records (y_column). The value of x_column (mapped key)
        will be compared with value of y_column (mapped value).

    columns_to_group: dict[y_column, x_column]
        Maps columns in y_records (y_column) to one column in
        x_records (x_column). The value of y_column (mapped key)
        will be compared with value of x_column (mapped value) to produce
        a subset of y_records where the value of y_column matches the
        value in x_column.

    scorers: dict[x_column, Callable[[x_value, y_value], float]]
        Maps columns in x_records (x_column) to a scorer.
        (see column_match for scorer definition)

    thresholds: dict[x_column, float]
        Maps columns in x_records (x_column) to a number that
        represents a threshold.
        (see column_match for threshold definition)

    cutoffs: dict[x_column, bool]
        Maps columns in x_records (x_column) to a boolean value that
        represents the cutoff.
        (see column_match for cutoff definition)

    Yields
    -------
    x_index: int
        The index of the x_record being matched to.

    y_matches: list[tuple[y_index, matching_score]]
        A list of matches if any, containing the y_index and the
        matching score.

    optimal_threshold: float
        A number that represents the score needed before it is
        considered an optimal match where the score is determined by the
        product of column thresholds and column uniqueness.
    """

    # Referenced outside the loop since the number of unique values within a column is fixed
    x_uniqueness = [
        (c, records.uniqueness_by_column(x_records, c)) for c in records.column_names(x_records)
    ]

    for x_index, x_record in x_records.items():
        # Columns to match are further refined by its availability in the
        # x_record and whether its value is not considered blank.

        refined_columns_to_match = {
            col for col in columns_to_match if col in x_record and x_record[col]
        }

        adjusted_u = records.adjusted_uniqueness(refined_columns_to_match, x_uniqueness)

        # If no columns to grouped, it will just return all y_records
        grouped_y_records = records.group_by(
            y_records, {y: x_record[x] for y, x in columns_to_group.items()}
        )

        y_records_scores = defaultdict(float)

        for x_column, y_columns in columns_to_match.items():
            for y_index, score in column_match(
                x_record,
                grouped_y_records,
                x_column,
                y_columns,
                scorer=scorers[x_column],
                threshold=thresholds[x_column],
                cutoff=cutoffs[x_column],
            ):
                # The score will be zero if the values in the column is
                # empty
                y_records_scores[y_index] += score * (
                    adjusted_u[x_column] if x_column in adjusted_u else 0
                )

        # Maximum score is checked so that it further reduces the ambiguity
        # of y_records matched
        y_matches = [
            (y_index, score)
            for y_index, score in y_records_scores.items()
            if score == max(y_records_scores.values())
        ]

        optimal_threshold = sum(
            thresholds[x_column]
            * (adjusted_u[x_column] if x_column in adjusted_u else 0)
            for x_column in refined_columns_to_match
        )

        yield x_index, y_matches, optimal_threshold


class TabularMatcher:
    """Applies the semantics from the results of record_match using a
    customized configuration

    This class combines semantics application of the match and accepts
    customizable configurations. The semantics (match status) is paired with
    customizable value so to fit the user's understanding of the match results.

    Additional columns will be added to show the match status, score for each
    row index it was matched with, and the matching score.


    Attributes
    ----------
    MATCH_STATUS: dict[status, status_shown]
        Semantics of the match results where the value is customizable
        to fit the user's needs.

    COLUMNS_TO_ADD: dict[column, column_shown]
        Columns that are to be added where the value is going to be what
        is appearing as a key in the record.

    x_records: dict[x_index, dict[x_column, x_value]]

    y_records: dict[y_index, dict[y_column, y_value]]

    config: config.MatcherConfig
        A class that encompasses all the configurable objects that
        determines the match results.

    required_threshold: int, float
        A number that represents the minimum required total matching
        score between an x and a y record before it is considered a match.

    duplicate_threshold: int, float
        A number that represents difference between the maximum and
        minimum of the total matching scores of two or more rows that
        passes the required threshold, after which the rows with the
        highest score will be considered the correct match, and the
        rest of the rows will be marked as not matched.

        Eg. When set to 100, the absolute difference between the score
        of two matching rows will have to exceed a 100 for it to not be
        counted as a duplicate, in this case, all the rows with the same matching
        y_index and no matter its difference, will be counted as duplicates.

        When set to 0, only the record highest matching score will be
        counted as the correct match.
    """

    MATCH_STATUS = {
        "unmatched": "UNMATCHED",
        "matched": "MATCHED",
        "ambiguous": "AMBIGUOUS",
        "review": "REVIEW",
        "duplicate": "DUPLICATE",
    }

    COLUMNS_TO_ADD = {
        "match_status": "match_status",
        "matched_with_row": "row(s)_matched",
        "match_score": "match_score",
    }

    def __init__(self, required_threshold=None, duplicate_threshold=None) -> None:
        self.required_threshold = 75.0
        self.duplicate_threshold = 0.0

        self.__config = MatcherConfig()

    @property
    def x_records(self) -> dict[int, dict[str, str]]:
        return self.__x_records.copy()

    @x_records.setter
    def x_records(self, x_records: dict[int, dict[str, str]]):
        self.__x_records = x_records
        self.__config.x_records = x_records

    @property
    def y_records(self) -> dict[int, dict[str, str]]:
        return self.__y_records.copy()

    @y_records.setter
    def y_records(self, y_records: dict[int, dict[str, str]]):
        self.__y_records = y_records
        self.__config.y_records = y_records

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, config: MatcherConfig):
        """Sets the config class and verifies the columns"""
        if (
            config.x_columns == self.__config.x_columns
            and config.y_columns == self.__config.y_columns
        ):
            self.__config = config

    def match(self, update_func: Callable = None):
        """
        Performs the match using record_match function and apply the
        configured semantics. Checks for duplicates after all
        record_match is applied.

        Parameters
        ----------
        update_func: Callable
            A callable to track the progress of the match. This is
            where a progress bar can be attached to the process.

        """

        if not self.__x_records and not self.__y_records:
            return

        # Gets a copy of the x_records to prevent the mutation of the
        # original x_records
        records_matched = self.x_records

        match_status = self.COLUMNS_TO_ADD["match_status"]
        matched_with_row = self.COLUMNS_TO_ADD["matched_with_row"]
        match_score = self.COLUMNS_TO_ADD["match_score"]

        y_index_to_x_matches = defaultdict(list)
        match_summary = Counter()

        for x_index, y_matches, optimal in records_match(
            self.__x_records,
            self.__y_records,
            self.config.columns_to_match,
            self.config.columns_to_group,
            scorers=self.config.scorers_by_column,
            thresholds=self.config.thresholds_by_column,
            cutoffs=self.config.cutoffs_by_column,
        ):
            y_matches_passed = [
                (y_index, score)
                for y_index, score in y_matches
                if score >= self.required_threshold
            ]

            if len(y_matches_passed) == 1:
                y_index, score = y_matches_passed[0]

                status = "review" if score <= optimal else "matched"

                for y_column, x_column in self.config.columns_to_get.items():
                    records_matched[x_index][x_column] = self.__y_records[y_index][
                        y_column
                    ]

                # This is used as a reference to see all the matches that are
                # associated with the particular y_index. It is particualarly
                # useful in checking for duplicates.
                y_index_to_x_matches[y_index].append((x_index, score))

            elif len(y_matches_passed) > 1:
                status = "ambiguous"

                for y_column, x_column in self.config.columns_to_get.items():
                    records_matched[x_index][x_column] = None

            else:
                status = "unmatched"

                for y_column, x_column in self.config.columns_to_get.items():
                    records_matched[x_index][x_column] = None

            records_matched[x_index][match_status] = self.MATCH_STATUS[status]

            # There may be more than one y matches which makes it ambiguous
            records_matched[x_index][matched_with_row] = ", ".join(
                # Concatenate the y-indices by mapping them as string
                map(lambda x: str(x[0]) if x else "", y_matches_passed)
            )

            records_matched[x_index][match_score] = ", ".join(
                # Concatenate the match scores by mapping them as string
                map(lambda x: str(x[1]) if x else "", y_matches_passed)
            )

            match_summary[status] += 1

            if callable(update_func):
                update_func()

        for _, x_matches in y_index_to_x_matches.items():
            if len(x_matches) > 1:
                # The distance between the highest and lowest score
                # can be used to determine whether it should be
                # considered as duplicate
                max_score = max(score for _, score in x_matches)
                min_score = min(score for _, score in x_matches)

                max_scores = [
                    x_index for x_index, score in x_matches if score == max_score
                ]

                # The presence of more than one max scores meant that there
                # are two or more equally scored rows. When there are more
                # than one maxiumum and equally scored rows, there is no
                # doubt that these rows will be duplicates.
                if len(max_scores) > 1 or (
                    abs(max_score - min_score) < self.duplicate_threshold
                ):
                    for x_index, _ in x_matches:
                        records_matched[x_index][match_status] = self.MATCH_STATUS[
                            "duplicate"
                        ]
                        match_summary["duplicate"] += 1

                else:
                    #
                    for x_index, score in x_matches:
                        # Keep the highest scoring of the x_matches, while marking
                        # the rest as unmatched.
                        if score != max_score:
                            for column in self.config.columns_to_get:
                                records_matched[x_index][column] = None

                            records_matched[x_index][match_status] = self.MATCH_STATUS[
                                "unmatched"
                            ]
                            records_matched[x_index][match_score] = ""
                            records_matched[x_index][matched_with_row] = ""
                            match_summary["unmatched"] += 1

        return records_matched, match_summary
