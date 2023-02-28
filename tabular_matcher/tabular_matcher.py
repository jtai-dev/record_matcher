
from collections import defaultdict
from .config import MatcherConfig


class TabularMatcher:

    MATCH_STATUS = {0: 'unmatched',
                    1: 'matched',
                    2: 'ambiguous',
                    3: 'review',
                    4: 'duplicate'}

    def __init__(self, x_records, y_records, config=None) -> None:
        
        self.config = MatcherConfig(None, None) if not config else config
        self.x_records = x_records
        self.y_records = y_records

    @property
    def x_records(self):
        return self.__x_records.copy()

    @x_records.setter
    def x_records(self, records:list):
        self.__x_records = records
        columns = {header for record in records for header in record.keys()}

        self.config.x_columns = list(columns)
        self.config.clear_all()
        self.config.populate_columns_to_match()

    @property
    def y_records(self):
        return self.__y_records.copy()

    @y_records.setter
    def y_records(self, records:list):
        self.__y_records = records
        columns = {header for record in records for header in record.keys()}

        self.config.y_columns = list(columns)
        self.config.clear_all()
        self.config.populate_columns_to_match()

    def match(self):
        
        for x_index, x_record in enumerate(self.__x_records):
            
            # only match the column in x that is not just a blank space and also exist in columns_to_match
            x_columns_to_match = [k for k, v in x_record.items() if k in self.config.columns_to_match.keys() 
                                                                    and str(v).strip()]
            # gets the variability score (uniqueness) for each column, the more distinguish the values, the higher the variability score                                                                    
            u = adjusted_uniqueness(self.__y_records, x_columns_to_match)
            # narrow down the list of y_records containing certain column values that exists in the current x_record
            grouped_y_records = group_records_by_value(enumerate(self.__y_records), x_record, self.config.columns_to_group)
            
            # this counters tracks the total scores for each y_record by summing up matched column scores
            y_score_counter = defaultdict(float)

            for x_column, y_columns in self.config.columns_to_match.items():

                y_index_scores = column_match(x_record, 
                                              grouped_y_records,
                                              x_column, 
                                              y_columns, 
                                              scorer=self.config.scorer_by_column[x_column],
                                              cutoff=self.config.cutoffs_by_column[x_column],
                                              threshold=self.config.threshold_by_column[x_column])

                for y_index, score in y_index_scores:
                    y_score_counter[y_index] += score * u[x_column] if x_column in u.keys() else 0

            # only get the best y_record match and matches that passes the total required threshold
            y_matches = {k: v for k, v in y_score_counter.items() if v == max(y_score_counter.values()) 
                                                                  and v >= self.config.total_required_threshold}

            # will be marked as 'unmatched' if there are no matches
            if not y_matches:
                yield (x_index, y_matches, 0)

            # will be marked as 'ambiguous' if there are more than one matches
            elif len(y_matches) > 1:
                yield (x_index, y_matches, 2)

            else:
                optimal_threshold = sum([self.config.threshold_by_column[column] * u[column] for column in x_columns_to_match])
                i = next(iter(y_matches))
                # marked as 'review' if it does not meet the optimized threshold
                if y_matches[i] <= optimal_threshold:
                    yield (x_index, y_matches, 3)
                # marked as 'matched' if it is the only match and higher than the optimized threshold
                else:
                    yield (x_index, y_matches, 1)


    def apply_matches(self, index_increment=0, p_bar=None):

        _x_records = self.x_records

        for x_index, y_matches, status in self.match():

            if y_matches and (status==1 or status==3):
                y_index = next(iter(y_matches))
                for column in self.config.columns_to_get:

                    if column in self.config.x_columns:
                        _x_records[x_index][f'_{column}_'] = self.__y_records[y_index][column]
                    else:
                        _x_records[x_index][column] = self.__y_records[y_index][column]

            _x_records[x_index]['match_status'] = TabularMatcher.MATCH_STATUS[status].upper()
            _x_records[x_index]['matched_with_row'] = ', '.join(map(lambda x: str(x + index_increment), 
                                                                    y_matches.keys()))
            _x_records[x_index]['match_score'] = ', '.join(map(lambda x: str(x)), y_matches.values()) \
                                                           if status == 2 else next(iter(y_matches.values()))

            if p_bar:
                p_bar.update(1)

        dupes = get_duplicates_by_column(_x_records, 'matched_with_row')

        for dupe in dupes:
            _x_records[dupe]['match_status'] = TabularMatcher.MATCH_STATUS[4].upper()

        return _x_records

    
    def __len__(self):
        return len(self.__x_records)


def column_match(x_record:dict, 
                 y_records:list,
                 x_column:str,
                 y_columns:list, 
                 scorer,
                 cutoff=False,
                 threshold=0,
                 enum=False):

    # scores contains the matching score for x_record column that is matched with each y_record columns;
    # the maximum matching score out of all the columns 
    if enum:
        scores = [max([scorer(x_record[x_column], 
                              y_record[y_column], 
                              score_cutoff=threshold if cutoff else 0) 
                                    for y_column in y_columns])
                        for y_index, y_record in enumerate(y_records)]
    
    else:
        scores = [max([scorer(x_record[x_column], 
                              y_record[y_column], 
                              score_cutoff=threshold if cutoff else 0) 
                                    for y_column in y_columns])
                        for y_index, y_record in y_records]       

    return [(y_index, score) for y_index, score in scores if score > 0] 


def uniqueness(records:list, column):

    items = {record[column] for record in records if record[column]}
    return len(items)/len(records) if len(records) > 0 else 0


def adjusted_uniqueness(records, columns):

    u = {column: uniqueness(records, column) for column in columns}
    return {column: value/sum(u.values()) for column, value in u.items() if sum(u.values())}


def group_records_by_value(y_records:list, x_record:dict, columns:dict):
    """
    y_records: [(0, {column_1: value_1, column_2: value_2})
                (1, {column_1: value_1, column_2: value_2})
                ...]
    """

    def _group():
        y_column = list(columns.keys()).pop()
        x_column = columns.pop(y_column)
        for y_index, y_record in y_records:
            if y_record[y_column] == x_record[x_column]:
                yield y_index, y_record

    if not columns:
        return y_records

    else:
        return group_records_by_value(list(_group()), x_record, columns)


def records_slice(records, *columns):

    return [{column:record[column] for record in records for column in columns}]


def get_duplicates_by_column(records, column):

    counter = defaultdict(list)

    for index, record in enumerate(records):
        counter[record[column]].append(index)

    d = set()

    for _, indices in counter.items():
        if len(indices) > 1:
            d = d.union(indices)

    return d