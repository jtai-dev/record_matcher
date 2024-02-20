import pytest
from tabular_matcher.config import MatcherConfig


@pytest.fixture
def x_records():
    return {
        0: {"col_1": 1, "col_2": 2, "col_3": 3, "col_4": 4},
        1: {"col_1": 5, "col_2": 6, "col_3": 7, "col_4": 8},
        2: {"col_1": 9, "col_2": 10, "col_3": 11, "col_4": 12},
    }


@pytest.fixture
def y_records():
    return {
        0: {"col_1": 1, "col_2": 2, "col_3": 3, "col_5": 4},
        1: {"col_1": 5, "col_2": 6, "col_3": 7, "col_5": 8},
        2: {"col_1": 9, "col_2": 10, "col_3": 11, "col_5": 12},
    }


@pytest.fixture
def matcher_config(x_records, y_records):
    config = MatcherConfig()
    config.x_records = x_records
    config.y_records = y_records
    return config


def test_get_x_column_names(matcher_config: MatcherConfig):
    assert matcher_config.x_columns == {"col_1", "col_2", "col_3", "col_4"}


def test_get_y_column_names(matcher_config: MatcherConfig):
    assert matcher_config.y_columns == {"col_1", "col_2", "col_3", "col_5"}


def test_x_y_intersected_columns(matcher_config: MatcherConfig):
    matcher_config.populate()
    assert {"col_1", "col_2", "col_3"}.intersection(matcher_config.columns_to_match)


def test_column_to_match_add_x_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_match["col_3"] = "col_3"
    assert matcher_config.columns_to_match == {"col_3": ["col_3"]}


def test_column_to_match_remove_x_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_match["col_1"] = "col_1"
    del matcher_config.columns_to_match["col_1"]
    assert matcher_config.columns_to_match == {}


def test_column_to_match_add_a_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_match["col_2"] = "col_2"
    assert matcher_config.columns_to_match == {"col_2": ["col_2"]}


def test_column_to_match_remove_a_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_match["col_2"] = "col_2"
    matcher_config.columns_to_match["col_2"].remove("col_2")
    assert matcher_config.columns_to_match == {"col_2": []}


def test_column_to_match_add_multiple_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_match["col_2"] = "col_2", "col_3"
    assert matcher_config.columns_to_match == {"col_2": ["col_2", "col_3"]}


def test_column_to_match_remove_multiple_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_match["col_2"] = "col_2", "col_3"
    matcher_config.columns_to_match["col_2"].remove("col_2")
    assert matcher_config.columns_to_match == {"col_2": ["col_3"]}
    matcher_config.columns_to_match["col_2"].remove("col_3")
    assert matcher_config.columns_to_match == {"col_2": []}


def test_column_to_match_adding_existent_x_column_and_existent_y_column(
    matcher_config: MatcherConfig,
):
    matcher_config.columns_to_match["col_2"] = "col_1"
    assert matcher_config.columns_to_match == {"col_2": ["col_1"]}


def test_column_to_match_adding_existent_x_column_and_nonexistent_y_column(
    matcher_config: MatcherConfig,
):
    matcher_config.columns_to_match["col_4"] = "col_4"
    assert matcher_config.columns_to_match == {"col_4": []}


def test_column_to_match_adding_nonexistent_x_column_and_existent_y_column(
    matcher_config: MatcherConfig,
):
    try:
        matcher_config.columns_to_match["col_5"] = "col_5"
    except Exception as e:
        assert type(e).__name__ == "TBConfigColumnNotFound"

    assert matcher_config.columns_to_match == {}


def test_column_to_match_adding_nonexistent_x_column_and_nonexistent_y_column(
    matcher_config: MatcherConfig,
):
    try:
        matcher_config.columns_to_match["col_5"] = "col_4"
    except Exception as e:
        assert type(e).__name__ == "TBConfigColumnNotFound"

    assert matcher_config.columns_to_match == {}


def test_columns_to_get_add_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_get["col_5"] = "col_5"
    assert matcher_config.columns_to_get == {"col_5": "col_5"}


def test_columns_to_get_remove_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_get["col_5"] = "col_5"
    del matcher_config.columns_to_get["col_5"]
    assert matcher_config.columns_to_get == {}


def test_columns_to_get_reject_nonexistent_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_get["col_4"] = "col_6"
    assert matcher_config.columns_to_get == {}


def test_columns_to_get_accept_overwrite(matcher_config: MatcherConfig):
    matcher_config.columns_to_get.allow_overwrite = True
    matcher_config.columns_to_get["col_3"] = "col_3"
    assert matcher_config.columns_to_get == {"col_3": "col_3"}


def test_columns_to_get_reject_overwrite(matcher_config: MatcherConfig):
    matcher_config.columns_to_get.allow_overwrite = False
    try:
        matcher_config.columns_to_get["col_3"] = "col_3"
    except Exception as e:
        assert type(e).__name__ == "TBConfigOverwriteError"
    assert matcher_config.columns_to_get == {}


def test_columns_to_group_add_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_group["col_2"] = "col_1"
    assert matcher_config.columns_to_group == {"col_2": "col_1"}


def test_columns_to_group_remove_y_column(matcher_config: MatcherConfig):
    matcher_config.columns_to_group["col_2"] = "col_1"
    del matcher_config.columns_to_group["col_2"]
    assert matcher_config.columns_to_group == {}


def test_columns_to_group_adding_existent_y_column_and_existent_x_column(
    matcher_config: MatcherConfig,
):
    matcher_config.columns_to_group["col_5"] = "col_4"
    assert matcher_config.columns_to_group == {"col_5": "col_4"}


def test_columns_to_group_adding_existent_y_column_and_nonexistent_x_column(
    matcher_config: MatcherConfig,
):
    try:
        matcher_config.columns_to_group["col_5"] = "col_5"
    except Exception as e:
        assert type(e).__name__ == "TBConfigColumnNotFound"
    assert matcher_config.columns_to_group == {}


def test_columns_to_group_adding_nonexistent_y_column_and_existent_x_column(
    matcher_config: MatcherConfig,
):
    try:
        matcher_config.columns_to_group["col_4"] = "col_4"
    except Exception as e:
        assert type(e).__name__ == "TBConfigColumnNotFound"
    assert matcher_config.columns_to_group == {}


def test_columns_to_group_adding_nonexistent_y_column_and_nonexistent_x_column(
    matcher_config: MatcherConfig,
):
    try:
        matcher_config.columns_to_group["col_4"] = "col_5"
    except Exception as e:
        assert type(e).__name__ == "TBConfigColumnNotFound"
    assert matcher_config.columns_to_group == {}


def test_scorers_by_column_add_x_column(matcher_config: MatcherConfig):
    def func(x, y):
        return 0

    matcher_config.scorers_by_column.SCORERS["test_match"] = func
    matcher_config.scorers_by_column["a"] = "test_match"
    assert matcher_config.scorers_by_column == {"a": func}
    del matcher_config.scorers_by_column["a"]
    assert matcher_config.scorers_by_column == {}


# def test_scorers_by_column_reject_nonexistent_scorer(
#     matcher_config: MatcherConfig,
# ):
#     matcher_config.scorers_by_column.SCORERS.clear()
#     matcher_config.scorers_by_column["b"] = "i_have_no_ref_scorer"
#     assert matcher_config.scorers_by_column == {}


# def test_automatic_adding_to_threshold_scorer_and_cutoffs_by_column(
#     matcher_config: MatcherConfig,
# ):
#     def func(x, y):
#         return 0

#     matcher_config.scorers_by_column.SCORERS["test_match"] = func

#     matcher_config.thresholds_by_column.default = 75
#     matcher_config.scorers_by_column.default = "test_match"
#     matcher_config.cutoffs_by_column.default = False

#     matcher_config.columns_to_match["a"] = "b"
#     matcher_config.columns_to_match["e"] = "b"

#     assert matcher_config.thresholds_by_column == {"a": 75}
#     assert matcher_config.scorers_by_column == {"a": func}
#     assert matcher_config.cutoffs_by_column == {"a": False}


# def test_automatic_removal_of_threshold_scorer_and_cutoffs_by_column(
#     matcher_config: MatcherConfig,
# ):
#     def func(x, y):
#         return 0

#     matcher_config.scorers_by_column.SCORERS["test_match"] = func

#     matcher_config.thresholds_by_column.default = 75
#     matcher_config.scorers_by_column.default = "test_match"
#     matcher_config.cutoffs_by_column.default = False

#     matcher_config.columns_to_match["a"] = "b"
#     matcher_config.columns_to_match["e"] = "b"

#     del matcher_config.columns_to_match["a"]

#     assert matcher_config.thresholds_by_column == {}
#     assert matcher_config.scorers_by_column == {}
#     assert matcher_config.cutoffs_by_column == {}


# def test_threshold_by_column_adding_and_removal_of_x_column(matcher_config: MatcherConfig):
#     matcher_config.thresholds_by_column["a"] = 80
#     assert matcher_config.thresholds_by_column == {"a": 80}
#     del matcher_config.thresholds_by_column["a"]
#     assert matcher_config.thresholds_by_column == {}


# def test_cutoffs_by_column_adding_and_removal_of_x_column(matcher_config: MatcherConfig):
#     matcher_config.cutoffs_by_column["a"] = True
#     assert matcher_config.cutoffs_by_column == {"a": True}
#     del matcher_config.cutoffs_by_column["a"]
#     assert matcher_config.cutoffs_by_column == {}


# def test_cutoffs_by_column_rejecting_values_not_boolean(matcher_config: MatcherConfig):
#     matcher_config.cutoffs_by_column["c"] = "True"
#     assert matcher_config.cutoffs_by_column == {}
#     matcher_config.cutoffs_by_column["c"] = "False"
#     assert matcher_config.cutoffs_by_column == {}
#     matcher_config.cutoffs_by_column["c"] = 1
#     assert matcher_config.cutoffs_by_column == {}
#     matcher_config.cutoffs_by_column["c"] = 0
#     assert matcher_config.cutoffs_by_column == {}
