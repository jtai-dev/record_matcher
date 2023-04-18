

class MatcherConfig:

    REQUIRED_THRESHOLD = 75

    def __init__(self, x_records:dict[int,dict[str,str]], y_records:dict[int,dict[str,str]]) -> None:

        self.x_records = x_records
        self.y_records = y_records
        
        self.columns_to_match = ColumnsToMatch(self)
        self.columns_to_get = ColumnsToGet(self)
        self.columns_to_group = ColumnsToGroup(self)

        self.scorers_by_column = ScorersByColumn(self)
        self.thresholds_by_column = ThresholdsByColumn(self)
        self.cutoffs_by_column = CutoffsByColumn(self)

        self.required_threshold = self.REQUIRED_THRESHOLD

    @staticmethod
    def column_names(records:dict[int,dict[str,str]]):
        return {c for i in records for c in records[i]}

    @property
    def x_records(self):
        pass
    
    @x_records.setter
    def x_records(self, records:dict[int,dict[str,str]]):
        self.__x_columns = self.column_names(records)

    @property
    def y_records(self):
        pass
    
    @y_records.setter
    def y_records(self, records:dict[int,dict[str,str]]):
        self.__y_columns = self.column_names(records)

    @property
    def x_columns(self):
        return self.__x_columns.copy()
    
    @property
    def y_columns(self):
        return self.__y_columns.copy()
    
    def reset(self):
        self.columns_to_match.clear()
        self.columns_to_get.clear()
        self.columns_to_group.clear()
        self.scorers_by_column.clear()
        self.thresholds_by_column.clear()
        self.cutoffs_by_column.clear()

    def populate(self):
        for column in self.__x_columns.intersection(self.__y_columns):
            self.columns_to_match[column] = column


class ColumnsToMatch(dict):

    def __init__(self, config:MatcherConfig):
        self.config = config

    def __setitem__(self, __x:str, *__y:str) -> None:
        if isinstance(next(iter(__y)), tuple):
            self[__x].update(set(*__y).intersection(self.config.y_columns))
        else:
            self[__x].update(set(__y).intersection(self.config.y_columns))

        self.config.scorers_by_column[__x] = None
        self.config.thresholds_by_column[__x] = None
        self.config.cutoffs_by_column[__x] = None
        
    def __missing__(self, __x:str):
        if __x not in self.config.x_columns:
            return set()
        super().__setitem__(__x, set())
        return super().__getitem__(__x)
    
    def __delitem__(self, __x:str) -> None:
        super().__delitem__(__x)
        del self.config.scorers_by_column[__x]
        del self.config.thresholds_by_column[__x]
        del self.config.cutoffs_by_column[__x]


class ColumnsToGet(set):

    def __init__(self, config:MatcherConfig):
        self.config = config

    def add(self, *y_columns:str):
        for y_column in y_columns:
            if y_column in self.config.y_columns:
                super().add(y_column)


class ColumnsToGroup(dict):
    
    def __init__(self, config:MatcherConfig):
        self.config = config

    def __setitem__(self, __y:str, __x:str=None) -> None:
        if __y in self.config.y_columns:
            if __x in self.config.x_columns:
                super().__setitem__(__y, __x)

            elif __y in self.config.x_columns:
                super().__setitem__(__y, __y)


class ScorersByColumn(dict):

    SCORERS = {'exact_match': lambda x, y: 100.0 if x==y else 0.0}
    DEFAULT_SCORER = 'exact_match' 

    def __init__(self, config:MatcherConfig):
        self.config = config
        self.default = self.DEFAULT_SCORER

    def __setitem__(self, __x:str, scorer:str=None) -> None:
        if __x in self.config.x_columns:
            if scorer in self.SCORERS:
                super().__setitem__(__x, self.SCORERS[scorer])
            elif scorer==None:
                super().__setitem__(__x, self.default)

    def __delitem__(self, __x) -> None:
        if __x in self and __x not in self.config.columns_to_match:
            return super().__delitem__(__x)

    @property
    def default(self):
        return self.__default
    
    @default.setter
    def default(self, scorer:str):
        if scorer in self.SCORERS:
            self.__default = self.SCORERS[scorer]
        else:
            if 'exact_match' not in self.SCORERS:
                self.SCORERS.update({'exact_match': lambda x, y: 100.0 if x==y else 0.0})
            self.__default = self.SCORERS['exact_match']


class ThresholdsByColumn(dict):

    DEFAULT_THRESHOLD = 75.0

    def __init__(self, config:MatcherConfig):
        self.config = config
        self.default = self.DEFAULT_THRESHOLD

    def __setitem__(self, __x, threshold:float=None) -> None:
        if __x in self.config.x_columns:
            if isinstance(threshold, (int, float, complex)):
                if 0 <= threshold <= 100:
                    super().__setitem__(__x, threshold)
            else:
                super().__setitem__(__x, self.default)

    def __delitem__(self, __x) -> None:
        if __x in self and __x not in self.config.columns_to_match:
            return super().__delitem__(__x)
        
    @property
    def default(self):
        return self.__default
    
    @default.setter
    def default(self, threshold:float):
        self.__default = 75.0
        if isinstance(threshold, (int, float, complex)):
            if 0 <= threshold <= 100:
                self.__default = threshold
        

class CutoffsByColumn(dict):

    DEFAULT_CUTOFF = False

    def __init__(self, config:MatcherConfig):
        self.config = config
        self.default = self.DEFAULT_CUTOFF

    def __setitem__(self, __x, cutoff:bool=None) -> None:
        if __x in self.config.x_columns:
            if isinstance(cutoff, bool):
                super().__setitem__(__x, cutoff)
            elif cutoff==None:
                super().__setitem__(__x, self.default)

    def __delitem__(self, __x) -> None:
        if __x in self and __x not in self.config.columns_to_match:
            return super().__delitem__(__x)
        
    @property
    def default(self):
        return self.__default
    
    def default(self, cutoff:bool):
        self.__default = False
        if isinstance(cutoff, bool):
            self.__default = cutoff
