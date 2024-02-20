from tabular_matcher import records


def test_get_column_names_from_records():
    test_data = {0: {'a': 1, 'b': 2}, 
                 1: {'a': 3, 'b': 4, 'c':5}}
    
    expected_columns = {'a','b','c'}

    assert records.column_names(test_data) == expected_columns


def test_group_records_by_column_and_value():
    test_data = {0: {'a': 1, 'b': 2},
                 1: {'a': 4, 'b': 3},
                 2: {'a': 3, 'b': 2},
                 3: {'a': 5, 'b': 4}}
    
    expected_records = {0: {'a': 1, 'b': 2},
                        2: {'a': 3, 'b': 2}}
    
    assert records.group_by(test_data, {'b': 2}) == expected_records


def test_records_uniqueness_by_column():
    test_data = {0:{'a':1, 'b':1},
                 1:{'a':2, 'b':2},
                 2:{'a':3, 'b':2},
                 3:{'a':4, 'b':1},
                 4:{'a':5, 'b':2},
                 5:{'a':6, 'b':2},
                 6:{'a':7, 'b':2},
                 7:{'a':4, 'b':1}}
    
    expected_uniqueness_of_column_a = 7/8
    expected_uniqueness_of_column_b = 2/8

    assert records.uniqueness_by_column(test_data, 'a') == expected_uniqueness_of_column_a
    assert records.uniqueness_by_column(test_data, 'b') == expected_uniqueness_of_column_b


def test_find_duplicated_records_by_column():
    test_data = {0:{'a':6, 'b':2, 'c':104},
                 1:{'a':1, 'b':1, 'c':104},
                 2:{'a':2, 'b':2, 'c':101},
                 3:{'a':3, 'b':2, 'c':102},
                 4:{'a':4, 'b':1, 'c':100},
                 5:{'a':5, 'b':2, 'c':103},
                 6:{'a':7, 'b':2, 'c':105},
                 7:{'a':4, 'b':1, 'c':102}}

    expected_duplicated_records_by_column_c = {0:{'a':6, 'b':2, 'c':104},
                                               1:{'a':1, 'b':1, 'c':104},
                                               3:{'a':3, 'b':2, 'c':102},
                                               7:{'a':4, 'b':1, 'c':102}}
    
    for record in records.duplicated(test_data, 'c'):
        assert record in expected_duplicated_records_by_column_c.values()