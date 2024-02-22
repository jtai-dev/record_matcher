import pytest
from record_matcher import matcher


@pytest.fixture
def x_records():
    return {0: {'a': 'ab', 'b': 'cd', 'c': 'z', 'd': 'gh', 'e': 'ij'},
            1: {'a': 'kl', 'b': 60, 'c': 'z', 'd': 'qr', 'e': 'st' },
            2: {'a': 34, 'b': 56, 'c': 'z', 'd': 90, 'e':12},
            3: {'a': 78, 'b': 90, 'c': 'z', 'd': 34, 'e':56},
            4: {'a': 56, 'b': 78, 'c': 'z', 'd': 12, 'e':34},
            5: {'a': 90, 'b': 12, 'c': 'z', 'd': 56, 'e':78},
            6: {'a': 23, 'b': 39, 'c': 'z', 'd': 43, 'e':100},
            7: {'a': 27, 'b': 123, 'c': 'z', 'd': 85, 'e':102},
            8: {'a': 12, 'b': 34, 'c': 'z', 'd': 78, 'e':90},
            9: {'a': 12, 'b': 34, 'c': 'z', 'd': 78, 'e':90}
            }


@pytest.fixture
def y_records():
    return {0: {'a': 12, 'b': 34, 'c': 56, 'd': 78, 'e':90},
            1: {'a': 34, 'b': 56, 'c': 78, 'd': 90, 'e':12},
            2: {'a': 56, 'b': 78, 'c': 90, 'd': 12, 'e':34},
            3: {'a': 78, 'b': 90, 'c': 12, 'd': 34, 'e':56},
            4: {'a': 90, 'b': 12, 'c': 34, 'd': 56, 'e':78},
            5: {'a': 56, 'b': 78, 'c': 90, 'd': 12, 'e':34},
            6: {'a': 90, 'b': 12, 'c': 34, 'd': 56, 'e':78},
            7: {'a': 73, 'b': 60, 'c': 82, 'd': 91, 'e':52},
            8: {'a': 23, 'b': 45, 'c': 67, 'd': 89, 'e':100},
            9: {'a': 27, 'b': 49, 'c': 63, 'd': 85, 'e':102}
            }


@pytest.fixture
def exact_column_match_results():
    return {0: {'a': [],
                'b': [],
                'c': [],
                'd': [],
                'e': []
                }, 
                1: {
                'a': [],
                'b': [(7, 100.0)],
                'c': [],
                'd': [],
                'e': [],
                },
            2: {'a': [(1, 100.0)],
                'b': [(1, 100.0)],
                'c': [],
                'd': [(1, 100.0)],
                'e': [(1, 100.0)],
                },
            3: {'a': [(3, 100.0)],
                'b': [(3, 100.0)],
                'c': [],
                'd': [(3, 100.0)],
                'e': [(3, 100.0)],
                },
            4: {'a': [(2, 100.0), (5, 100.0)],
                'b': [(2, 100.0), (5, 100.0)],
                'c': [],
                'd': [(2, 100.0), (5, 100.0)],
                'e': [(2, 100.0), (5, 100.0)],
                },
            5: {'a': [(4, 100.0), (6, 100.0)],
                'b': [(4, 100.0), (6, 100.0)],
                'c': [],
                'd': [(4, 100.0), (6, 100.0)],
                'e': [(4, 100.0), (6, 100.0)],
                },
            6: {'a': [(8, 100.0)],
                'b': [],
                'c': [],
                'd': [],
                'e': [(8, 100.0)],
                },
            7: {'a': [(9, 100.0)],
                'b': [],
                'c': [],
                'd': [(9, 100.0)],
                'e': [(9, 100.0)],
                },
            8: {'a': [(0, 100.0)],
                'b': [(0, 100.0)],
                'c': [],
                'd': [(0, 100.0)],
                'e': [(0, 100.0)],
                },
            9: {'a': [(0, 100.0)],
                'b': [(0, 100.0)],
                'c': [],    
                'd': [(0, 100.0)],
                'e': [(0, 100.0)],
                }
            } 


@pytest.fixture
def uniqueness_of_x_records_as_reference():
    return {'a': 9/10, 'b': 9/10, 'c': 1/10, 'd': 9/10, 'e': 9/10}


@pytest.fixture
def adjusted_uniqueness_of_x_records_as_reference():
    return {'a': 0.09/0.37, 'b':0.09/0.37, 'c': 0.01/0.37, 'd': 0.09/0.37, 'e':0.09/0.37}
            

@pytest.fixture
def exact_record_match_results():
    return {0: ([], 100.0),
            1: ([(7, 1*9/0.37)], 100.0),
            2: ([(1, 4*9/0.37)], 100.0),
            3: ([(3, 4*9/0.37)], 100.0),
            4: ([(2, 4*9/0.37), 
                 (5, 4*9/0.37)], 100.0),
            5: ([(4, 4*9/0.37), 
                 (6, 4*9/0.37)], 100.0),
            6: ([(8, 2*9/0.37)], 100.0),
            7: ([(9, 3*9/0.37)], 100.0),
            8: ([(0, 4*9/0.37)], 100.0),
            9: ([(0, 4*9/0.37)], 100.0)
            }


def test_column_match_to_get_y_index_and_score(x_records, y_records, exact_column_match_results):

    columns_to_match = {'a': ['a'],
                        'b': ['b'],
                        'c': ['c'],
                        'd': ['d'],
                        'e': ['e']}
    
    for x_index, x_record in x_records.items():
        for x_column, y_columns in columns_to_match.items():
            match_count = 0
            for y_index, row_score in matcher.column_match(x_record=x_record,
                                                            y_records=y_records,
                                                            x_column=x_column,
                                                            y_columns=y_columns,
                                                            scorer=lambda x, y: 100.0 if x==y else 0.0,
                                                            threshold=100.0,
                                                            cutoff=False):
                match_count += 1
                assert (y_index, row_score) in exact_column_match_results[x_index][x_column]

            assert len(exact_column_match_results[x_index][x_column]) == match_count


def test_record_match_to_get_x_index_and_y_matches_and_optimal_threshold(x_records, y_records, exact_record_match_results):

    exact_match_scorer = lambda x, y: 100.0 if x==y else 0.0

    columns_to_match = {'a': ['a'],
                        'b': ['b'],
                        'c': ['c'],
                        'd': ['d'],
                        'e': ['e']
                        }
    
    scorers = {'a': exact_match_scorer, 
               'b': exact_match_scorer, 
               'c': exact_match_scorer, 
               'd': exact_match_scorer, 
               'e': exact_match_scorer
               }
    
    thresholds = {'a': 100.0,
                  'b': 100.0,
                  'c': 100.0,
                  'd': 100.0,
                  'e': 100.0}
    
    cutoffs = {'a': False,
               'b': False,
               'c': False,
               'd': False,
               'e': False}

    for x_index, y_indices_and_scores, optimal_threshold in matcher.records_match(x_records, 
                                                                       y_records,
                                                                       columns_to_match=columns_to_match,
                                                                       columns_to_group={},
                                                                       scorers=scorers,
                                                                       thresholds=thresholds,
                                                                       cutoffs=cutoffs,
                                                                       required_threshold=0):
        

        expected_y_indices_and_scores, expected_optimal_threshold = exact_record_match_results[x_index]
        
        assert len(y_indices_and_scores) == len(expected_y_indices_and_scores)

        expected_y_indices_and_scores_dict = {}

        if expected_y_indices_and_scores:
            for index, score in expected_y_indices_and_scores:
                expected_y_indices_and_scores_dict[index] = score

        if y_indices_and_scores:
            for y_index, y_score in y_indices_and_scores:
                assert y_index in expected_y_indices_and_scores_dict
                assert y_score - expected_y_indices_and_scores_dict[y_index] < 0.001

        assert optimal_threshold == expected_optimal_threshold
