from collections import Counter
from collections.abc import Generator


"""
This module is the core foundation of record_matcher which is based on
the concept of records. Records are data structures that represents 
tabular data, where each record is a dictionary mapping column names to
values. The table is represented as a dictionary, where the keys are row
indices (integers) and the values are dictionaries representing individual
rows.

eg.
    A table containing rows and columns:
    
        firstname    lastname      country
        ---------    --------    ----------
    0     Reuben      Garett         USA
    1     Alicia      Mason          UK
    2     Jane       van Doe     Netherlands
    
    will translate to records data structure as shown below:
    
    records = {
        0: {'firstname': 'Reuben', 'lastname': 'Garett', 'country': 'USA'}
        1: {'firstname': 'Alicia', 'lastname': 'Mason', 'country': 'UK' }
        2: {'firstname': 'Jane', 'lastname': 'van Doe', 'country': 'Netherlands'}
    }

"""


def column_names(records: dict[int, dict[str, str]]) -> set[str]:
    """Gets the columns through all the records.

    Parameters
    ----------
    records : dict[int, dict[str, str]]
        (See module docstring for definition)

    Returns
    -------
    set[str]
        All column names return are unique
    """
    return {c for i in records for c in records[i]}


def uniqueness_by_column(records: dict[int, dict[str, str]], column: str) -> float:
    """Calculates the uniqueness (frequency ratio) of values for each column.

    Parameters
    ----------
    records : dict[int, dict[str, str]]
        (See module docstring for definition)

    column : str
        Select a column to calculate the uniqueness for

    Returns
    -------
    float
        A number representing the frequency ratio of each values. The greater
        the value, the greater the distinction between each values in the columnn.
    """
    items = {r[column] for r in records.values() if r[column]}
    return len(items) / len(records) if len(records) > 0 else 0


def adjusted_uniqueness(
    selected_columns, columns_uniqueness=None, records=None
) -> dict[str, float]:
    """_summary_

    Parameters
    ----------
    selected_columns : _type_
        _description_
    columns_uniqueness : _type_, optional
        _description_, by default None
    records : _type_, optional
        _description_, by default None

    Returns
    -------
    _type_
        _description_
    """
    if not columns_uniqueness:
        assert any(records)

        columns_uniqueness = [
            (c, uniqueness_by_column(records, c)) for c in column_names(records)
        ]
    selected_u = [(c, u) for c, u in columns_uniqueness if c in selected_columns]
    u_sum = sum(u for _, u in selected_u)

    return {c: u / u_sum for c, u in selected_u if u_sum > 0}


def group_by(
    records: dict[int, dict[str, str]], column_map: dict[str, str]
) -> dict[int, dict[str, str]]:
    """_summary_

    Parameters
    ----------
    records : dict[int, dict[str, str]]
        _description_
    column_map : dict[str, str]
        _description_

    Returns
    -------
    dict[int, dict[str, str]]
        _description_
    """
    grouped = {}

    for index, record in records.items():
        matched = [
            record.get(column, "") == value for column, value in column_map.items()
        ]
        if all(matched):
            grouped[index] = record

    return grouped


def duplicated_by_column(
    records: dict[int, dict[str, str]], column: str
) -> Generator[dict[int, dict[str, str]]]:
    """_summary_

    Parameters
    ----------
    records : dict[int, dict[str, str]]
        _description_
    column : str
        _description_

    Returns
    -------
    _type_
        _description_

    Yields
    ------
    Generator[dict[int, dict[str, str]]]
        _description_
    """
    counter = Counter(r[column] for r in records.values() if r[column])
    return (r for r in records.values() if counter[r[column]] > 1)
