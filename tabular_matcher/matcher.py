from collections import defaultdict, Counter
from collections.abc import Generator, Callable

from . import records
from .config import MatcherConfig


def column_match(
    x_record: dict[str, str],
    y_records: dict[int, dict[str, str]],
    x_column: str,
    y_columns: list[str],
    scorer: Callable[[str, str], float], 
    threshold: int | float = 0,
    cutoff: bool = False,
) -> Generator[tuple[str, int | float]]:  # type: ignore
    """Finds matching records from y_records that matches the x_record
    column.

    The results of matching records will depend on the scorer that is
    used. For example, an exact scorer will only return matching
    records that has an exact string match (100 percent). If the column
    of x_record is compared with multiple columns of the y_record, the
    y_record column with the highest matching score when compared with
    a x_record column will be taken. The function performs a final
    check to see if cutoffs==True, if so, scores that exceeds the given
    threshold is considered a potential match, otherwise it defaults to
    returning anything that is greater than 0.

    Parameters
    ----------
    x_record: dict[str, str]

    y_records: dict[int, dict[str,str]]

    x_column: str
        A key (column) that exist in x_record in which its paired value
        is to be compared.

    y_columns: list
        List of key(s) (columns) that exist in y_records in which the
        paired value(s) is(are) to be compared.

    scorer: Callable[[str, str], float]
        A scorer is a callable object that takes in two parameters x
        and y each corresponding to the value in x_column and value in
        y_column respectively, and returns a real number showing the
        matching score between x and y values.

    threshold: int|float, default=0
        A threshold is a real number within the range of values
        produced by the scorer and is the minimum required matching
        score between the value of the mapped x_column and another
        value to be considered a legitimate match.

        NOTE: Only applies when cutoff==True

    cutoff: bool, default=False
        The cutoff determines if the matching score derived from the
        values matched in the selected column is subject to the
        threshold.

    Returns
    -------
    Generator[tuple[str, int|float]]
        An iterator of tuples that contains the y_index and the scores
        of the matching y_record.
    """

    ## To contain all the indices and matching score of y_records to be
    ## compared later, this although is a slightly slower approach, has
    ## better readibility than yielding from multiple if statements
    row_scores = []

    for y_index, y_record in y_records.items():

        column_scores = []
        for y_column in y_columns:
            # To prevent from having an error if the x_column or
            # y_column do not exist in  x_record or y_record
            # respectively
            column_scores.append(
                scorer(
                    str(x_record[x_column] if x_column in x_record else ""),
                    str(y_record[y_column] if y_column in y_record else ""),
                )
            )
        # column_scores might be empty so having score to 0 means no
        # matches
        row_scores.append((y_index, max(column_scores) if column_scores else 0))

        # # Combines the strings of y_columns and scores it with string of x_column
        # column_score = scorer(
        #             str(x_record[x_column] if x_column in x_record else ""),
        #             " ".join((str(y_record[y_column]) for y_column in y_columns))
        #         )
        # row_scores.append((y_index, column_score))
        

    ## Returning as iterators so other functions can further process
    ## each individual match
    if cutoff:
        return (
            (y_index, row_score)
            for y_index, row_score in row_scores
            if row_score >= threshold
        )

    else:
        return (
            (y_index, row_score) for y_index, row_score in row_scores if row_score > 0
        )


def records_match(
    x_records: dict[int, dict[str, str]],
    y_records: dict[int, dict[str, str]],
    columns_to_match: dict[str, set[str]],
    columns_to_group: dict[str, str],
    scorers: dict[str, Callable[[str, str], float]],
    thresholds: dict[str, int | float],
    cutoffs: dict[str, bool],
    required_threshold: int | float,
) -> Generator[int, list[tuple[int, float]], float]:
    """Finds matching records from y_records that matches all records in
    x_records

    This is the core algorithm that tabular matcher uses. While the
    column_match function calculates the score for a single column in
    x_record, this function iterates through each column in x_record and
    compute the total column score. It also considers and determines a
    few more factors such as:

          i) the number of unique values in each colum
         ii) the y_records that are to be grouped by the column value
             that matches the value in the x_record column
        iii) whether the total computed score passes the desired
             threshold set by the user

    The return results is determined by the factors above, it consist of
    matched y_record(s) and is mapped to each index of the x_record along
    with the optimal threshold.


    Parameters
    ----------
    x_records: dict[int, dict[str,str]]

    y_records: dict[int, dict[str,str]]

    columns_to_match: dict[str,set[str]]
        Maps columns in x_records (x_column) to the columns in y_records
        (y_column). The value in the mapped key (x_column) is to be
        compared with the values in the mapped value (y_column).

    columns_to_group: dict[str,str]
        Maps columns in y_records (y_column) to a column in x_records
        (x_column). The value of the mapped key (y_column) will be
        compared with the value of the mapped value (x_column) to
        produce grouped y_records. Grouped y_records is the subset of
        the entirety of y_records. For a y_record to be part of the
        subset, the value in the mapped y_column has to be identical
        with the value in the mapped x_column.

    scorers: dict[str, Callable[[str,str], float]]
        Maps the columns in all x_records to a scorer. A scorer is a
        callable object that takes in two parameters x and y each
        corresponding to the value in x_column and value in y_column
        respectively, and returns a real number showing the matching
        score between x and y values.

    thresholds: dict[str,int|float]
        Maps columns in x_records (x_column) to a number that
        represents a threshold. A threshold is a real number within the
        range of values produced by the scorer and is the minimum
        required  matching score between the value of the mapped
        x_column and another value to be considered a legitimate match.

    cutoffs: dict[str,bool]
        Maps columns in x_records (x_column) to a boolean value that
        represents the cutoff. The cutoff determines if the matching
        score derived from the values matched in the selected column is
        subject to the threshold. Cutoff applies when it is set to
        True.

    required_threshold: int, float
        A number that represents the minimum required total matching
        score in a record before it is considered a match.


    Yields
    -------
    x_index: int
        The index of the x_record being matched to.

    y_matches: list[tuple[int, float]]
        A list of matches (if any) containing the y_index and the
        matching score.

    optimal_threshold: float
        A number that represents the score needed before it is
        considered an optimal match where the score is determined by the
        product of column thresholds and column uniqueness.
    """

    ## Referenced outside the loop since the number of unique values
    ## within a column is fixed
    x_uniqueness = [
        (c, records.uniqueness(x_records, c)) for c in records.column_names(x_records)
    ]

    for x_index, x_record in x_records.items():
        ## Columns in x_record that are to be matched are further
        ## determined by the availability and if it is not empty
        x_columns_to_match = {
            c for c, v in x_record.items() if c in columns_to_match and v
        }

        ## Uniqueness is further adjusted that suits the columns that
        ## are to be matched, the sum of denominator should always adds
        ## up to 100, such that if the y_record is identical to the
        ## x_record, it should return 100 as the matching score
        _u_selected = [(c, u) for c, u in x_uniqueness if c in x_columns_to_match]
        _u_sum = sum(u for _, u in _u_selected)
        _u_adjusted = {c: u / _u_sum for c, u in _u_selected if _u_sum > 0}

        ## If no columns to grouped, it will just return all y_records
        y_records_grouped = records.group_by(
            y_records, {y: x_record[x] for y, x in columns_to_group.items()}
        )

        y_records_scores = defaultdict(float)

        for x_column, y_columns in columns_to_match.items():
            for y_index, score in column_match(
                x_record,
                y_records_grouped,
                x_column,
                y_columns,
                scorer=scorers[x_column],
                threshold=thresholds[x_column],
                cutoff=cutoffs[x_column],
            ):
                ## The score will be zero if the values in the column is
                ## empty
                y_records_scores[y_index] += score * (
                    _u_adjusted[x_column] if x_column in _u_adjusted else 0
                )

        ## Maximum score is checked so that it reduces the ambiguity
        ## of y_records matched
        y_matches = [
            (y_index, score)
            for y_index, score in y_records_scores.items()
            if score == max(y_records_scores.values()) and score >= required_threshold
        ]

        optimal_threshold = sum(
            thresholds[x_column]
            * (_u_adjusted[x_column] if x_column in _u_adjusted else 0)
            for x_column in x_columns_to_match
        )

        yield x_index, y_matches, optimal_threshold


class TabularMatcher:

    """Applies the semantics of the record_match results using a
    customized configuration

    This class combines semantics application of the match and accepts
    user customizable configurations. The semantics (match status) is
    paired with a value that is customizable to fit the  audience
    understanding of the match results. Additional columns will be added
    as an extra key to the records, and it is customizableto prevent key
    overwrite.


    Attributes
    ----------

    MATCH_STATUS: dict
        This is name-value pair of the semantics of the match results
        where the value is customizable to fit the audience
        understanding.

    ADD_COLUMNS: dict
        This is name-value pair of the columns where the value is going
        to be a added as a key to the records.

    x_records: dict[int, dict[str,str]]

    y_records: dict[int, dict[str,str]]

    config: config.MatcherConfig
        A class that encompasses all the configurable objects that
        determines the match results.

    required_threshold: int, float
        A number that represents the minimum required total matching
        score in a record before it is considered a match.

    duplicate_threshold: int, float
        A number that represents difference between the maximum and
        minimum of the total matching scores of two or more rows that
        passes the required threshold, after which the rows with the
        highest score will be considered the correct match, and the
        rest of the rows will be marked as not matched.


    Methods
    -------
    match(update_func=None)
        Performs the match using record_match function and apply the
        configured semantics. Checks for duplicates after the match.

    """

    MATCH_STATUS = {
        "unmatched": "UNMATCHED",
        "matched": "MATCHED",
        "ambiguous": "AMBIGUOUS",
        "review": "REVIEW",
        "duplicate": "DUPLICATE",
    }

    ADD_COLUMNS = {
        "match_status": "match_status",
        "matched_with_row": "row(s)_matched",
        "match_score": "match_score",
    }

    def __init__(
        self,
        config:MatcherConfig=None)  -> None:
        """
        Parameters
        ----------
        x_records: dict[int, dict[str,str]]
            A dictionary containing dictionaries (records) that are to
            be compared to.

        y_records: dict[int, dict[str,str]]
            A dictionary containing dictionaries (records) that are to
            be compared with.

        config: config.MatcherConfig
            A class that encompasses all the configurable objects that
            determines the match results
        """
        
        self.required_threshold = 75.0
        self.duplicate_threshold = 3.0

        self.__config = config if config else MatcherConfig()

    @property
    def x_records(self) -> dict[int, dict[str, str]]:
        """Returns a copy of the initiated x_records"""
        return self.__x_records.copy()

    @x_records.setter
    def x_records(self, x_records: dict[int, dict[str, str]]):
        """Sets the x_records and changes x_records for config"""
        self.__x_records = x_records
        self.__config.x_records = x_records

    @property
    def y_records(self) -> dict[int, dict[str, str]]:
        """Returns a copy of the initiated y_records"""
        return self.__y_records.copy()

    @y_records.setter
    def y_records(self, y_records: dict[int, dict[str, str]]):
        """Sets the y_records and changes y_records for config"""
        self.__y_records = y_records
        self.__config.y_records = y_records

    @property
    def config(self):
        """Returns the config class"""
        return self.__config

    @config.setter
    def config(self, config: MatcherConfig):
        """Sets the config class and verifies the columns"""
        if (
            config.x_columns == self.__config.x_columns
            and config.y_columns == self.__config.y_columns
        ):
            self.__config = config

    def match(self, update_func: Callable=None):
        """
        Performs the match using record_match function and apply the
        configured semantics. Checks for duplicates after all
        record_match is applied.

        Parameters
        ----------
        update_func: Callable
            This is a callable to track the progress of the match

        """

        ## Gets a copy of the x_records to prevent the mutation of the
        ## original x_records

        if not self.__x_records and not self.__y_records:
            return

        _x_records = self.x_records

        match_status = self.ADD_COLUMNS["match_status"]
        matched_with_row = self.ADD_COLUMNS["matched_with_row"]
        match_score = self.ADD_COLUMNS["match_score"]

        y_index_to_x_matches = defaultdict(dict)
        match_summary = Counter()

        for x_index, y_matches, optimal in records_match(
            self.__x_records,
            self.__y_records,
            self.config.columns_to_match,
            self.config.columns_to_group,
            scorers=self.config.scorers_by_column,
            thresholds=self.config.thresholds_by_column,
            cutoffs=self.config.cutoffs_by_column,
            required_threshold=self.required_threshold,
        ):
            if len(y_matches) == 1:
                ## This should not have error since the length is first
                ## checked
                y_index, score = next(iter(y_matches))
                status = "review" if score <= optimal else "matched"

                for y_column, x_column in self.config.columns_to_get.items():
                    _x_records[x_index][x_column] = self.__y_records[y_index][y_column]

                ## This map y_index to the x_index used for checking
                ## duplicates later
                y_index_to_x_matches[y_index].update({x_index: score})

            elif len(y_matches) > 1:
                status = "ambiguous"

                for y_column, x_column in self.config.columns_to_get.items():
                    _x_records[x_index][x_column] = None

            else:
                status = "unmatched"

                for y_column, x_column in self.config.columns_to_get.items():
                    _x_records[x_index][x_column] = None

            _x_records[x_index][match_status] = self.MATCH_STATUS[status]
            ## String and concatenate the y indices
            _x_records[x_index][matched_with_row] = ", ".join(
                map(lambda x: str(x[0]) if x else "", y_matches)
            )
            ## String and concatenate the match scores
            _x_records[x_index][match_score] = ", ".join(
                map(lambda x: str(x[1]) if x else "", y_matches)
            )

            match_summary[status] += 1

            if callable(update_func):
                update_func()

        for x_matches in y_index_to_x_matches.values():
            if len(x_matches) > 1:
                ## The distance between the highest and lowest score
                ## can be used to determine whether it should be
                ## considered as duplicate
                max_score = max(x_matches.values())
                min_score = min(x_matches.values())

                ## The presence of more than one max scores will rule
                ## all matches as duplicates
                check = [
                    x_index
                    for x_index, score in x_matches.items()
                    if score == max_score
                ]

                ## The higher the threshold, the higher the number of
                ## rows marked as duplicate.
                if len(check) > 1 or (
                    abs(max_score - min_score) < self.duplicate_threshold
                ):
                    for x_index in x_matches:
                        _x_records[x_index][match_status] = self.MATCH_STATUS[
                            "duplicate"
                        ]
                        match_summary["duplicate"] += 1

                else:
                    ## This distinguishes the max scores from the min
                    ## scores, to reduce  unnecessary duplicates and
                    ## marking min scores as unmatched
                    for x_index, score in x_matches.items():
                        if score != max_score:
                            for column in self.config.columns_to_get:
                                _x_records[x_index][column] = None

                            _x_records[x_index][match_status] = self.MATCH_STATUS[
                                "unmatched"
                            ]
                            _x_records[x_index][match_score] = ""
                            _x_records[x_index][matched_with_row] = ""
                            match_summary["unmatched"] += 1

        return _x_records, match_summary
