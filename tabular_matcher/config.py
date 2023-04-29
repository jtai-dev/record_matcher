

class MatcherConfig:
    
    """A class that encapsulates the configurations that pertains to 
    the columns in x_records and y_records.

    
    Attributes
    ----------
    x_records: dict[int, dict[str,str]]

    y_records: dict[int, dict[str,str]]

    columns_to_match: ColumnsToMatch(dict)
        Maps columns in x_records (x_column) to the columns in y_records 
        (y_column).

    columns_to_get: ColumnsToGet(set)
        Maps columns in y_records (y_column) to an existing column
        (x_column) or non-existing column in x_records. 
    
    columns_to_group: ColumnsToGroup(dict)
        Maps columns in y_records (y_column) to a column in x_records
        (x_column).

    scorers_by_column: ScorersByColumn(dict)
        Maps columns in x_records (x_column) to a scorer.

    thresholds_by_column: ThresholdByColumn(dict)
        Maps columns in x_records (x_column) to a number that represents
        a threshold.

    cutoffs_by_column: CutoffsByColumn(dict)
        Maps columns in x_records (x_column) to a boolean value that 
        represents the cutoff.

    """

    def __init__(self, x_records:dict[int,dict[str,str]], y_records:dict[int,dict[str,str]]) -> None:

        self.columns_to_match = ColumnsToMatch(self)
        self.columns_to_get = ColumnsToGet(self)
        self.columns_to_group = ColumnsToGroup(self)
        self.scorers_by_column = ScorersByColumn(self)
        self.thresholds_by_column = ThresholdsByColumn(self)
        self.cutoffs_by_column = CutoffsByColumn(self)

        self.x_records = x_records
        self.y_records = y_records

    @staticmethod
    def column_names(records:dict[int,dict[str,str]]):
        return {c for i in records for c in records[i]}

    @property
    def x_records(self):
        pass
    
    @x_records.setter
    def x_records(self, records:dict[int,dict[str,str]]):
        self.__x_columns = self.column_names(records)
        self.reset()

    @property
    def y_records(self):
        pass
    
    @y_records.setter
    def y_records(self, records:dict[int,dict[str,str]]):
        self.__y_columns = self.column_names(records)
        self.reset()

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

    """Maps columns in x_records (x_column) to the columns in y_records 
    (y_column).
    
    The value in the mapped key (x_column) is to be compared with the
    values in the mapped value (y_column). 
    
    This class is a dictionary object with the following constraints:
        i) Verifies both key and value and rejects if it does not exist
           in config.x_columns and config.y_columns respectively. 
       ii) Adds a key to ScorersByColumn, ThresholdsByColumn and 
           CutoffsByColumn when a key is added.

    eg. {x_column_1: {y_column_1, y_column_2...}, 
         x_column_2: {y_column_3, y_column_4...}
         ...}


    Attributes
    ----------
    config: MatcherConfig
        A class that encapsulates the configurations that pertains to 
        the columns in x_records and y_records.
    """

    def __init__(self, config:MatcherConfig):

        """
        Parameters
        ----------
        config: MatcherConfig
            A class that encapsulates the configurations that pertains 
            to the columns in x_records and y_records.
        """
       
        self.config = config

    def __setitem__(self, __x:str, *__y:str) -> None:

        ## __y will be a tuple no matter how many inputs is given
        ## Eg: 
        ## self['a'] = 1 -> __y = (1, ); *__y = 1
        ## self['b'] = 3,4 -> __y = ((3,4),) ; *__y = (3,4)

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


class ColumnsToGet(dict):

    """Maps columns in y_records (y_column) to an existing column
    (x_column) or non-existing column in x_records. 
    
    The value of the mapped key (y_column) will be taken as a result of 
    a match between the x_record and the y_record. If mapped to an
    existing key (x_column), the taken value will overwrite the value 
    if any, the value in x_column. Otherwise, a non-existing column 
    will be added to accomodate the value obtained from y_record.

    This class is a dictionary object with the following constraints:
        i) Verifies the key and rejects if it does not exist in
           config.y_columns
       ii) Ensure that key is mapped to a unique value and is not an 
           existing value within this dictionary.
      iii) Depending on allow_overwrite, may reject key if it currently
           exist in config.x_columns

    eg. {y_column_1: new_x_column,
         y_column_2: x_column_1,
         y_column_3: x_column_2}


    Attributes
    ----------
    config: MatcherConfig
        A class that encapsulates the configurations that pertains to 
        the columns in x_records and y_records.

    allow_overwrite: bool
        If set to True, it will not raise an error when attempting
        to map the y_column to an existing column in x_records.
    """

    def __init__(self, config:MatcherConfig):

        """
        Parameters
        ----------
        config: MatcherConfig
            A class that encapsulates the configurations that pertains 
            to the columns in x_records and y_records.
        """

        self.config = config
        self.allow_overwrite = False

    def __setitem__(self, __y:str, __x:str=None):
        if __y in self.config.y_columns:
            if self.allow_overwrite:
                ## __x represents the x_column and cannot exist twice
                ## If it exist twice, it will overwrite the preceding
                ## value.
                if __x not in self.values():
                    super().__setitem__(__y, __x)
            else:
                if __x not in self.config.x_columns:
                    if __x not in self.values():
                        super().__setitem__(__y, __x)


class ColumnsToGroup(dict):

    """Maps columns in y_records (y_column) to a column in x_records
    (x_column).
    
    The value of the mapped key (y_column) will be compared with the 
    value of the mapped value (x_column) to produce grouped y_records. 
    Grouped y_records is the subset of the entirety of y_records. 
    For a y_record to be part of the subset, the value in the mapped 
    y_column has to be identical with the value in the mapped x_column.

    This class is a dictionary object with the following constraints:
        i) Verifies both key and value and rejects if it does not exist
           in config.x_columns and config.y_columns respectively. 

    eg. {y_column_1: x_column_2,
         y_column_2: x_column_1,
         y_column_3: x_column_1}


    Attributes
    ----------
    config: MatcherConfig
        A class that encapsulates the configurations that pertains to 
        the columns in x_records and y_records.
    """

    def __init__(self, config:MatcherConfig):

        """
        Parameters
        ----------
        config: MatcherConfig
            A class that encapsulates the configurations that pertains 
            to the columns in x_records and y_records.
        """

        self.config = config

    def __setitem__(self, __y:str, __x:str) -> None:
        if __y in self.config.y_columns:
            if __x in self.config.x_columns:
                super().__setitem__(__y, __x)


class ScorersByColumn(dict):

    """Maps columns in x_records (x_column) to a scorer. 
    
    A scorer is a callable object that takes in two parameters x and y 
    each corresponding to the value in x_column and value in y_column 
    respectively, and returns a real number showing the matching score 
    between x and y values.

    This class is a dictionary object with the following constraints:
        i) Verifies the key (x_column) and rejects if it does not exist 
           in config.x_columns
       ii) Rejects assignment of a value (name of scorer) to the key
           (x_column) if the name of scorer is not referenced in
           SCORERS.

    eg. {x_column_1: 'fuzzy_match',
         x_column_2: 'exact_match',
         x_column_3: 'token_match'}


    Attributes:
    -----------
    SCORERS: dict[str: Callable[str, str, float]]
        Contains a reference of scorer names to a scorer.

    DEFAULT_SCORER: str
        The name of the scorer that this class will default to when a
        scorer is not provided.

    config: MatcherConfig
        A class that encapsulates the configurations that pertains to 
        the columns in x_records and y_records.
    
    default: str
        The name of the scorer that the instance of this class will 
        default to when a scorer is not provided or is not referenced 
        in the SCORERS dictionary.
    """

    SCORERS = {'exact_match': lambda x, y: 100.0 if x==y else 0.0}
    DEFAULT_SCORER = 'exact_match' 

    def __init__(self, config:MatcherConfig):

        """
        Parameters
        ----------
        config: MatcherConfig
            A class that encapsulates the configurations that pertains 
            to the columns in x_records and y_records.
        """

        self.config = config
        self.default = self.DEFAULT_SCORER

    def __setitem__(self, __x:str, scorer:str=None) -> None:
        if __x in self.config.x_columns:
            if scorer in ScorersByColumn.SCORERS:
                super().__setitem__(__x, ScorersByColumn.SCORERS[scorer])
            elif scorer==None:
                super().__setitem__(__x, ScorersByColumn.SCORERS[self.default])

    def __delitem__(self, __x) -> None:
        if __x in self and __x not in self.config.columns_to_match:
            return super().__delitem__(__x)
        
    @property
    def default(self):
        return self.__default
    
    @default.setter
    def default(self, scorer:str):
        self.default = ScorersByColumn.SCORERS[ScorersByColumn.DEFAULT_SCORER]
        if scorer in ScorersByColumn.SCORERS:
            self.__default = self.SCORERS[scorer]


class ThresholdsByColumn(dict):

    """Maps columns in x_records (x_column) to a number that represents
    a threshold.
    
    A threshold is a real number within the range of values produced by
    the scorer and is the minimum required matching score between the 
    value of the mapped x_column and another value to be considered a 
    legitimate match.

    This class is a dictionary object with the following constraints:
        i) Verifies the key (x_column) and rejects if it does not exist in
           config.x_columns
       ii) Only accepts values that are real numbers.

    eg. {x_column_1: 80,
         x_column_2: 72.0}


    Attributes
    ----------
    DEFAULT_THRESHOLD: int, float
        The threshold that this class defaults to when threshold is not
        provided.
    
    config: MatcherConfig
        A class that encapsulates the configurations that pertains to 
        the columns in x_records and y_records.
    
    default: int, float
        The threshold that an instance of this class will default to 
        when threshold is not provided during assignment to a key 
        (x_column).
    """

    DEFAULT_THRESHOLD = 75.0

    def __init__(self, config:MatcherConfig):

        """
        Parameters
        ----------
        config: MatcherConfig
            A class that encapsulates the configurations that pertains 
            to the columns in x_records and y_records.
        """

        self.config = config
        self.default = self.DEFAULT_THRESHOLD

    def __setitem__(self, __x, threshold:float=None) -> None:
        if __x in self.config.x_columns:
            if isinstance(threshold, (int, float)):
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
        self.__default = ThresholdsByColumn.DEFAULT_THRESHOLD
        if isinstance(threshold, (int, float)):
            self.__default = threshold
        

class CutoffsByColumn(dict):

    """Maps columns in x_records (x_column) to a boolean value that 
    represents the cutoff.
    
    The cutoff determines if the matching score derived from the 
    values matched in the selected column is subject to the threshold.
    Cutoff applies when it is set to True. 

    This class is a dictionary object with the following constraints:
        i) Verifies the key (x_column) and rejects if it does not exist 
           in config.x_columns
       ii) Only accepts boolean values.

    eg. {x_column_1: True,
         x_column_2: False}


    Attributes
    ----------
    DEFAULT_CUTOFF: bool
        The cutoff that the instance of this class defaults to during 
        the instantiation of the class and serves as a fallback value 
        for the instance.
    
    config: MatcherConfig
        A class that encapsulates the configurations that pertains to 
        the columns in x_records and y_records.
    
    default: bool
        The cutoff that an instance of this class will default to 
        when cutoff is not provided during assignment to a x_column.
    """
    
    DEFAULT_CUTOFF = False

    def __init__(self, config:MatcherConfig):
        
        """
        Parameters
        ----------
        config: MatcherConfig
            A class that encapsulates the configurations that pertains 
            to the columns in x_records and y_records.
        """

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
    
    @default.setter
    def default(self, cutoff:bool):
        self.__default = CutoffsByColumn.DEFAULT_CUTOFF
        if isinstance(cutoff, bool):
            self.__default = cutoff
