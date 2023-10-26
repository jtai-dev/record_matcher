import pytest
from tabular_matcher.config import MatcherConfig


@pytest.fixture
def test_data():

    x_records = {0: {'a': 1, 'b': 2, 'c': 3, 'd': 10},
                 1: {'a': 4, 'b': 5, 'c': 6},
                 2: {'a': 7, 'b': 8, 'c': 9, 'd': 11}}

    y_records = {0: {'a': 1, 'b': 2, 'c': 3},
                 1: {'a': 4, 'b': 5, 'c': 6},
                 2: {'a': 7, 'b': 8, 'c': 9, 'e': 12}}

    return x_records, y_records


@pytest.fixture
def test_config(test_data):
    x_records, y_records = test_data
    return MatcherConfig(x_records, y_records)


def test_get_column_names(test_config: MatcherConfig):
    assert (test_config.x_columns == {'a', 'b', 'c', 'd'} and
            test_config.y_columns == {'a', 'b', 'c', 'e'})


def test_x_y_intersected_columns(test_config: MatcherConfig):
    test_config.populate()
    assert {'a', 'b', 'c'}.intersection(test_config.columns_to_match)


def test_column_to_match_adding_existent_x_column_and_existent_y_column(test_config: MatcherConfig):
    test_config.columns_to_match['d'] = 'e'
    assert test_config.columns_to_match == {'d': {'e'}}


def test_column_to_match_adding_existent_x_column_and_nonexistent_y_column(test_config: MatcherConfig):
    test_config.columns_to_match['c'] = 'd'
    assert test_config.columns_to_match == {'c': set()}


def test_column_to_match_adding_nonexistent_x_column_and_existent_y_column(test_config: MatcherConfig):
    test_config.columns_to_match['e'] = 'c'
    assert test_config.columns_to_match == {}


def test_column_to_match_adding_nonexistent_x_column_and_nonexistent_y_column(test_config: MatcherConfig):
    test_config.columns_to_match['e'] = 'd'
    assert test_config.columns_to_match == {}


def test_column_to_match_adding_and_removal_of_x_column(test_config: MatcherConfig):
    test_config.columns_to_match['d'] = 'e'
    assert test_config.columns_to_match == {'d': {'e'}}
    del test_config.columns_to_match['d']
    assert test_config.columns_to_match == {}


def test_column_to_match_adding_and_removal_of_y_column(test_config: MatcherConfig):
    test_config.columns_to_match['d'] = 'e'
    assert test_config.columns_to_match == {'d': {'e'}}
    test_config.columns_to_match['d'].remove('e')
    assert test_config.columns_to_match == {'d': set()}


def test_automatic_adding_to_threshold_scorer_and_cutoffs_by_column(test_config: MatcherConfig):

    def func(x, y): return 0
    test_config.scorers_by_column.SCORERS['test_match'] = func

    test_config.thresholds_by_column.default = 75
    test_config.scorers_by_column.default = 'test_match'
    test_config.cutoffs_by_column.default = False

    test_config.columns_to_match['a'] = 'b'
    test_config.columns_to_match['e'] = 'b'

    assert test_config.thresholds_by_column == {'a': 75}
    assert test_config.scorers_by_column == {'a': func}
    assert test_config.cutoffs_by_column == {'a': False}


def test_automatic_removal_of_threshold_scorer_and_cutoffs_by_column(test_config: MatcherConfig):

    def func(x, y): return 0
    test_config.scorers_by_column.SCORERS['test_match'] = func

    test_config.thresholds_by_column.default = 75
    test_config.scorers_by_column.default = 'test_match'
    test_config.cutoffs_by_column.default = False

    test_config.columns_to_match['a'] = 'b'
    test_config.columns_to_match['e'] = 'b'

    del test_config.columns_to_match['a']

    assert test_config.thresholds_by_column == {}
    assert test_config.scorers_by_column == {}
    assert test_config.cutoffs_by_column == {}


def test_threshold_by_column_adding_and_removal_of_x_column(test_config: MatcherConfig):
    test_config.thresholds_by_column['a'] = 80
    assert test_config.thresholds_by_column == {'a': 80}
    del test_config.thresholds_by_column['a']
    assert test_config.thresholds_by_column == {}


def test_scorers_by_column_adding_and_removal_of_x_column(test_config: MatcherConfig):

    def func(x, y): return 0
    test_config.scorers_by_column.SCORERS['test_match'] = func

    test_config.scorers_by_column['a'] = 'test_match'
    assert test_config.scorers_by_column == {'a': func}
    del test_config.scorers_by_column['a']
    assert test_config.scorers_by_column == {}


def test_cutoffs_by_column_adding_and_removal_of_x_column(test_config: MatcherConfig):
    test_config.cutoffs_by_column['a'] = True
    assert test_config.cutoffs_by_column == {'a': True}
    del test_config.cutoffs_by_column['a']
    assert test_config.cutoffs_by_column == {}


def test_scorers_by_column_rejecting_scorer_with_no_ref_in_scorers_dict(test_config: MatcherConfig):
    test_config.scorers_by_column.SCORERS.clear()
    test_config.scorers_by_column['b'] = 'i_have_no_ref_scorer'
    assert test_config.scorers_by_column == {}


def test_cutoffs_by_column_rejecting_values_not_boolean(test_config: MatcherConfig):
    test_config.cutoffs_by_column['c'] = 'True'
    assert test_config.cutoffs_by_column == {}
    test_config.cutoffs_by_column['c'] = 'False'
    assert test_config.cutoffs_by_column == {}
    test_config.cutoffs_by_column['c'] = 1
    assert test_config.cutoffs_by_column == {}
    test_config.cutoffs_by_column['c'] = 0
    assert test_config.cutoffs_by_column == {}


def test_columns_to_get_adding_and_removal_of_y_columns(test_config: MatcherConfig):
    test_config.columns_to_get.add('a')
    assert test_config.y_columns.intersection(
        test_config.columns_to_get) == {'a'}

    test_config.columns_to_get.add('b')
    assert test_config.y_columns.intersection(
        test_config.columns_to_get) == {'a', 'b'}

    test_config.columns_to_get.remove('a')
    assert test_config.y_columns.intersection(
        test_config.columns_to_get) == {'b'}

    test_config.columns_to_get.remove('b')
    assert test_config.y_columns.intersection(
        test_config.columns_to_get) == set()


def test_columns_to_get_rejecting_nonexistent_y_columns(test_config: MatcherConfig):
    test_config.columns_to_get.add('d')
    assert len(test_config.columns_to_get) == 0
    test_config.columns_to_get.add('s')
    assert len(test_config.columns_to_get) == 0


def test_columns_to_group_adding_existent_y_column_and_existent_x_column(test_config: MatcherConfig):
    test_config.columns_to_group['e'] = 'd'
    assert test_config.columns_to_group == {'e': 'd'}


def test_columns_to_group_adding_existent_y_column_and_nonexistent_x_column(test_config: MatcherConfig):
    test_config.columns_to_group['c'] = 'e'
    assert test_config.columns_to_group == {'c': 'c'}


def test_columns_to_group_adding_nonexistent_y_column_and_existent_x_column(test_config: MatcherConfig):
    test_config.columns_to_group['d'] = 'c'
    assert test_config.columns_to_group == {}


def test_columns_to_group_adding_nonexistent_y_column_and_nonexistent_x_column(test_config: MatcherConfig):
    test_config.columns_to_group['d'] = 'e'
    assert test_config.columns_to_group == {}


def test_columns_to_group_adding_and_removal_of_y_column(test_config: MatcherConfig):
    test_config.columns_to_group['e'] = 'd'
    assert test_config.columns_to_group == {'e': 'd'}
    del test_config.columns_to_group['e']
    assert test_config.columns_to_group == {}
