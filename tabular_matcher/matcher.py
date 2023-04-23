
from collections import defaultdict, Counter
from collections.abc import Generator, Callable

from . import records
from .config import MatcherConfig


def column_match(x_record: dict[str,str], 
                 y_records: dict[int, dict[str,str]],
                 x_column: str,
                 y_columns: list[str], 
                 scorer: Callable[[str,str], float],
                 threshold: int|float=0,
                 cutoff: bool=False,
                 ) -> Generator[tuple[str, int|float]]:
    
    """Finds matching records from y_records that matches the x_record column.

    The results of matching records will depend on the scorer that is used.
    For example, an exact matching scorer will only return matching records 
    that has an exact string match, in other words, a 100 percent complete 
    string match. If the column of x_record is compared with multiple columns 
    of the y_record, the y_record column with the highest matching score when 
    compared with a x_record column will be taken. The function performs
    a final check to see if cutoffs==True, if so, scores that exceeds the 
    given threshold is considered a potential match, otherwise it defaults to
    returning anything that is greater than 0.

    
    Parameters
    ----------
    x_record: dict[str, str]
        A single dictionary record that is to be compared to.
    
    y_records: dict[int, dict[str,str]]
        A dictionary containing multiple dictionary records that are to be 
        compared with.
    
    x_column: str
        Name of key that exist in x_record in which the value is to be 
        compared.

    y_columns: list
        List of key(s) that exist in y_records in which the value is to be 
        compared.
    
    scorer: Callable[[str, str], float]
        A callable object that takes in two parameters x and y and returns
        a float showing the similarity of x and y.

    threshold: int|float, default=0
        A number that determines what is the minimum required matching score
        in order to be considered a match. Only applies if cutoffs is set to True.

    cutoff: bool, default=False
        If set to True, threshold will be enforced, otherwise matching records will
        be any matching score that exceeds 0.


    Returns
    -------
    Generator[tuple[str, int|float]]
        An iterator of tuples containing the y_index and the matching scores of the matched
        y_record.
    """

    # To contain all the indices and matching score of y_records to be compared later,
    # this although is a slightly slower approach, has better readibility than yielding 
    # from multiple if statements
    row_scores = []

    for y_index, y_record in y_records.items():
        column_scores = []
        for y_column in y_columns:
            # To prevent from having an error if the x_column or y_column do not exist in 
            # x_record or y_record respectively
            column_scores.append(scorer(str(x_record[x_column] if x_column in x_record else ''), 
                                        str(y_record[y_column] if y_column in y_record else '')))
        # column_scores might be empty so having score to 0 means no matches
        row_scores.append((y_index, max(column_scores) if column_scores else 0))
    
    # Returning as iterators so other functions can further process each individual match
    if cutoff:
        return ((y_index, row_score) for y_index, row_score in row_scores if row_score >= threshold)
    
    else:
        return ((y_index, row_score) for y_index, row_score in row_scores if row_score > 0)


def records_match(x_records: dict[int, dict[str,str]],
                  y_records: dict[int, dict[str,str]],
                  columns_to_match: dict[str,set[str]],
                  columns_to_group: dict[str,str],
                  scorers: dict[str, Callable[[str,str], float]],
                  thresholds: dict[str,int|float|complex],
                  cutoffs: dict[str,bool],
                  required_threshold: int|float|complex
                  ) -> Generator[int, list[tuple[int, float]], float]:
    
    """Finds matching records from y_records that matches all records in x_records

    This is the core algorithm that tabular matcher uses. While the column_match 
    function calculates  the score for a single column in x_record, this function 
    iterates through each column in x_record and compute the total column score. It 
    also considers and determines a few more factors such as:

          i) the number of unique values in each colum
         ii) the y_records that are to be grouped by the column value that matches
             the value in the x_record column
        iii) whether the total computed score passes the desired threshold 
             set by the user

    The return results is determined by the factors above, it consist of matched 
    y_record(s) and is mapped to each index of the x_record along with the optimal 
    threshold.


    Parameters
    ----------
    x_records: dict[int, dict[str,str]]
        A dictionary containing dictionaries (records) that are to be compared to.

    y_records: dict[int, dict[str,str]]
        A dictionary containing dictionaries (records) that are to be compared with.

    columns_to_match: dict[str,set[str]]
        Maps the columns in all x_records that are to be match with the columns 
        in all y_records One column in x_record may be matched with multiple columns 
        in a y_record.

        eg. {x_column_1: {y_column_1, y_column_2}, 
             x_column_2: {y_column_3, y_column_4}}

    columns_to_group: dict[str,str]
        Maps the columns in all y_records that can be grouped by its value found in 
        the column of a x_record. Uses the group_by function in records.py.
    
        eg. {y_column_1: x_column_1, y_column_2: x_column_2}

    scorers: dict[str, Callable[[str,str], float]]
        Maps the columns in all x_records to a scorer (see column_match scorer param 
        description).

    thresholds: dict[str,int|float|complex]
        Maps the columns in all x_records to a number that represent threshold (see 
        column_match threshold param description).

    cutoffs: dict[str,bool]
        Maps the columns in all x_records to a boolean that  if the selected column
        will need to pass a threshold in order to be considered a match (see 
        column_match cutoff param description).

    required_threshold: int, float
        A number that determines what is the minimum required total column score in 
        order to be considered a match.

        
    Yields
    -------
    x_index: int
        The index of the x_record being matched to.

    y_matches: list[tuple[int, float]]
        A list of matches (if any) containing the y_index and the matching score.

    optimal_threshold: float
        A number that represents what is the score to acquire in order to be 
        considered an optimal match as determined by the product of thresholds and 
        column uniqueness.
    """


    # Referenced outside the loop since the number of unique values within a column is fixed
    x_uniqueness = [(c, records.uniqueness(x_records, c)) for c in records.column_names(x_records)]

    for x_index, x_record in x_records.items():

        # Columns in x_record that are to be matched are further determined by the availability and
        # if it is not empty
        x_columns_to_match = {c for c, v in x_record.items() if c in columns_to_match and v}

        # Uniqueness is further adjusted that suits the columns that are to be matched, the 
        # sum of denominator should always adds up to 100, such that if the y_record is identical
        # to the x_record, it should return 100 as the matching score
        _u_selected = [(c, u) for c, u in x_uniqueness if c in x_columns_to_match]
        _u_sum = sum(u for _, u in _u_selected)
        _u_adjusted = {c: u/_u_sum for c, u in _u_selected if _u_sum > 0}


        # If no columns to grouped, it will just return all y_records
        y_records_grouped = records.group_by(
                                    y_records,
                                    {y: x_record[x] for y, x in columns_to_group.items()})
            
        y_records_scores = defaultdict(float)

        for x_column, y_columns in columns_to_match.items():
            for y_index, score in column_match(x_record, 
                                               y_records_grouped,
                                               x_column, 
                                               y_columns,
                                               scorer=scorers[x_column],
                                               threshold=thresholds[x_column],
                                               cutoff=cutoffs[x_column]
                                               ):
                # The score will be zero if the values in the column is empty
                y_records_scores[y_index] += score * (_u_adjusted[x_column] if x_column in _u_adjusted else 0)

        # Maximum score is checked so that it reduces the ambiguity of y_records matched
        y_matches = [(y_index, score) for y_index, score in y_records_scores.items()
                                                if score == max(y_records_scores.values()) and 
                                                   score >= required_threshold
                    ]

        optimal_threshold = sum(thresholds[x_column] * (_u_adjusted[x_column] if x_column in _u_adjusted else 0)
                                for x_column in x_columns_to_match)
   
        yield x_index, y_matches, optimal_threshold


class TabularMatcher:

    MATCH_STATUS = {'unmatched': 'UNMATCHED',
                    'matched': 'MATCHED',
                    'ambiguous': 'AMBIGUOUS',
                    'review': 'REVIEW',
                    'duplicate': 'DUPLICATE'}
    
    ADD_COLUMNS = {'match_status': 'match_status',
                   'matched_with_row': 'row(s)_matched',
                   'match_score': 'match_score'}

    def __init__(self, 
                 x_records: dict[int, dict[str,str]], 
                 y_records: dict[int, dict[str,str]], 
                 config: MatcherConfig) -> None:

        self.x_records = x_records
        self.y_records = y_records
        self.__config = config

    @property
    def x_records(self) -> dict[int, dict[str,str]]:
        return self.__x_records.copy()
    
    @x_records.setter
    def x_records(self, x_records: dict[int, dict[str,str]]):
        self.__x_records = x_records
        self.__config.x_records = x_records

    @property
    def y_records(self) -> dict[int, dict[str,str]]:
        return self.__y_records.copy()
    
    @y_records.setter
    def y_records(self, y_records: dict[int, dict[str,str]]):
        self.__y_records = y_records
        self.__config.y_records = y_records

    @property
    def config(self):
        return self.__config
    
    @config.setter
    def config(self, config: MatcherConfig):
        if (config.x_columns == self.__config.x_columns and
            config.y_columns == self.__config.y_columns):
            self.__config = config

    def match(self, p_bar: Callable=None):

        _x_records = self.x_records

        match_status = self.ADD_COLUMNS['match_status']
        matched_with_row = self.ADD_COLUMNS['matched_with_row']
        match_score = self.ADD_COLUMNS['match_score']

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
            required_threshold=self.config.required_threshold
        ):
            
            if len(y_matches) == 1:
                y_index, score = next(iter(y_matches))
                status = 'review' if score <= optimal else 'matched'

                for column in self.config.columns_to_get:
                    _x_records[x_index][column] = self.__y_records[y_index][column]

                y_index_to_x_matches[y_index].update({x_index: score})

            elif len(y_matches) > 1:
                status = 'ambiguous'

            else:
                status = 'unmatched'

            _x_records[x_index][match_status] = self.MATCH_STATUS[status]
            _x_records[x_index][matched_with_row] = ', '.join(map(lambda x: str(x[0]) if x else '',
                                                                  y_matches))
            _x_records[x_index][match_score] = ', '.join(map(lambda x: str(x[1]) if x else '',
                                                             y_matches))

            match_summary[status] += 1

            if p_bar:
                p_bar()


        for x_matches in y_index_to_x_matches.values():

            if len(x_matches) > 1:
                max_score = max(x_matches.values())
                min_score = min(x_matches.values())

                check = [x_index for x_index, score in x_matches.items() if score == max_score]

                if len(check) > 1 or (max_score - min_score < self.config.duplicate_threshold):
                    for x_index in x_matches:
                        _x_records[x_index][match_status] = self.MATCH_STATUS['duplicate']
                        match_summary['duplicate'] += 1 
        
                else:
                    for x_index, score in x_matches.items():

                        if score != max_score:

                            for column in self.config.columns_to_get:
                                _x_records[x_index][column] = None

                            _x_records[x_index][match_status] = self.MATCH_STATUS['unmatched']
                            _x_records[x_index][match_score] = ''
                            _x_records[x_index][matched_with_row] = ''
                            match_summary['unmatched'] += 1 

        return _x_records, match_summary