from rapidfuzz import fuzz


SCORER_MODULE = 'fuzz'
SCORERS_TO_EXCLUDE  = {'partial_ratio_alignment'}
SCORERS = [attr for attr in dir(fuzz) 
                if not attr.startswith('_')
                and attr not in SCORERS_TO_EXCLUDE]

DEFAULT_THRESHOLD = 75.0
DEFAULT_RAPIDFUZZ_SCORER = 'WRatio'               


class MatcherConfig:

    def __init__(self, x_columns:list, y_columns:list) -> None:

        self.x_columns = x_columns or []
        self.y_columns = y_columns or []

        self.columns_to_match = _ColumnsToMatch(self)
        self.threshold_by_column = _ThresholdByColumn(self)
        self.scorer_by_column = _ScorerByColumn(self)
        self.columns_to_get = _ColumnsToGet(self)
        self.columns_to_group = _ColumnsToGroup(self)

        self.total_required_threshold = DEFAULT_THRESHOLD

    def clear_all(self):

        self.columns_to_match.clear()
        self.threshold_by_column.clear()
        self.scorer_by_column.clear()
        self.columns_to_get.clear()
        self.columns_to_group.clear()

    def populate_columns_to_match(self):
        
        self.columns_to_match.clear()

        for column in sorted(set(self.x_columns).intersection(
                             set(self.y_columns))):

            self.columns_to_match.add(column, column)   


class _ColumnsToMatch(dict):
    def __init__(self, config:MatcherConfig):

        self.__config = config

    def add(self, x_column, *y_columns):

        if x_column in self.__config.x_columns:

            if x_column not in self.keys():
                self[x_column] = []
                self.__config.threshold_by_column.add(x_column)
                self.__config.scorer_by_column.add(x_column)

            if y_columns:
                for y_column in set(y_columns).intersection(self.__config.y_columns):
                    self[x_column].append(y_column)

            return True

        return False

    def remove(self, x_column, *y_columns):

        if x_column in self.keys():

            for y_column in set(y_columns).intersection(self.__config.y_columns):
                self[x_column].remove(y_column)

            if not y_columns or not self[x_column]:
                del self[x_column]
                self.__config.threshold_by_column.remove(x_column)
                self.__config.scorer_by_column.remove(x_column)
        
            return True

        return False


class _ThresholdByColumn(dict):

    def __init__(self, config:MatcherConfig):
        self.__config = config

    def add(self, x_column, threshold=None):
    
        if x_column in self.__config.x_columns:
            if threshold:
                if 0 <= threshold <= 100:
                    self[x_column] = threshold
                    return True
            else:
                self[x_column] = DEFAULT_THRESHOLD
                return True

        return False

    def remove(self, x_column):
        if x_column in self.keys():
            del self[x_column]


class _ScorerByColumn(dict):

    def __init__(self, config:MatcherConfig):
        self.__config = config

    def add(self, x_column, scorer=None):
    
        if x_column in self.__config.x_columns:
            if scorer:
                if scorer in SCORERS:
                    self[x_column] = eval(f"{SCORER_MODULE}.{scorer}")
                    return True
            else:
                self[x_column] = eval(f"{SCORER_MODULE}.{DEFAULT_RAPIDFUZZ_SCORER}")
                return True

        return False

    def remove(self, x_column):
        if x_column in self.keys():
            del self[x_column]


class _ColumnsToGet(list):
    def __init__(self, config:MatcherConfig):
        self.__config = config

    def add(self, *y_columns):
        for y_column in y_columns:
            if y_column in self.__config.y_columns:
                self.append(y_column)


class _ColumnsToGroup(list):
    def __init__(self, config:MatcherConfig):
        self.__config = config

    def add(self, *columns):
        for column in columns:
            if (column in self.__config.x_columns and
                column in self.__config.y_columns):
                    self.append(column)
