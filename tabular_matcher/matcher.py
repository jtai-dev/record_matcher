
from collections import defaultdict
from collections.abc import Generator, Callable
from tabular_matcher import records
from tabular_matcher.config import MatcherConfig


def column_match(x_record: dict[str,str], 
                 y_records: dict[int, dict[str,str]],
                 x_column: str,
                 y_columns: list[str], 
                 scorer: Callable[[str,str],float],
                 threshold: int|float=0,
                 cutoff: bool=False,
                 ) -> dict[str,int|float]:
    
    row_scores = []

    for y_index, y_record in y_records.items():
        column_scores = []
        for y_column in y_columns:
            column_scores.append(scorer(str(x_record[x_column]), 
                                        str(y_record[y_column])))
            
        row_scores.append((y_index, max(column_scores)))

    if cutoff:
        return ((y_index, score) for y_index, score in row_scores if score >= threshold)
    
    else:
        return ((y_index, score) for y_index, score in row_scores if score > 0)


def records_match(x_records: dict[int, dict[str,str]],
                  y_records: dict[int, dict[str,str]],
                  columns_to_match: dict[str, set[str]],
                  columns_to_group: dict[str,str],
                  scorers: dict[str, Callable[[str,str], float]],
                  thresholds: dict[str,int|float|complex],
                  cutoffs: dict[str,bool],
                  required_threshold: int|float|complex
                  ) -> Generator[int, list[tuple[int,float]], float]:
    
    x_uniqueness = [(c, records.uniqueness(x_records, c)) for c in records.column_names(x_records)]

    for x_index, x_record in x_records.items():

        x_columns_to_match = {c for c, v in x_record.items() if c in columns_to_match and v}

        _u_selected = [(c, u) for c, u in x_uniqueness if c in x_columns_to_match]
        _u_sum = sum(u for _, u in _u_selected)
        _u_adjusted = {c: u/_u_sum for c, u in _u_selected if _u_sum > 0}
        
        y_records_grouped = records.group_by(
                                    y_records,
                                    {y: x_record[x] for y, x in columns_to_group.items()})
            
        y_records_scores = defaultdict(float)

        for x_column, y_columns in columns_to_match.items():

            y_column_score = column_match(x_record,
                                          y_records_grouped,
                                          x_column,
                                          y_columns,
                                          scorer=scorers[x_column],
                                          threshold=thresholds[x_column],
                                          cutoff=cutoffs[x_column]
                                          )

            for y_index, score in y_column_score:
                y_records_scores[y_index] += score * (_u_adjusted[x_column] if x_column in _u_adjusted else 0)
         
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

    @property
    def y_records(self) -> dict[int, dict[str,str]]:
        return self.__y_records.copy()
    
    @y_records.setter
    def y_records(self, y_records: dict[int, dict[str,str]]):
        self.__y_records = y_records

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

            elif len(y_matches) > 1:
                status = 'ambiguous'

            else:
                status = 'unmatched'

            _x_records[x_index][match_status] = self.MATCH_STATUS[status]
            _x_records[x_index][matched_with_row] = ', '.join(map(lambda x: str(x[0]) if x else '',
                                                                  y_matches))
            _x_records[x_index][match_score] = ', '.join(map(lambda x: str(x[1]) if x else '',
                                                             y_matches))

            if p_bar:
                p_bar()

        for record in records.duplicated(_x_records, matched_with_row):
            record[match_status] = self.MATCH_STATUS['duplicate']

        return _x_records