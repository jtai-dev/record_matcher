from collections import Counter
from collections.abc import Generator


"""
This module is the core of record_matcher which is based on
the concept of records. Records are data structures that represents 
tabular data, where each record is a dictionary mapping column names to
values. The table is represented as a dictionary, where the keys are row
indices (integers) and the values are dictionaries representing individual
rows.

eg.
    A table containing rows and columns:
    
        firstname    lastname      country
        ---------    --------    ----------
    0     Reuben      Miller      USA
    1     Alicia      Thornton    UK
    2     Jane        van Doe     Netherlands
    
    will translate to records data structure as shown below:
    
    records = {
        0: {'firstname': 'Reuben', 'lastname': 'Miller', 'country': 'USA'}
        1: {'firstname': 'Alicia', 'lastname': 'Thornton', 'country': 'UK' }
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
    selected_columns: list, columns_uniqueness: list = None, records: dict[int, dict[str, str]]=None
) -> dict[str, float]:
    """Adjust the uniqueness (frequency ratio) of values according to the columns
    selected.

    Parameters
    ----------
    selected_columns : list
        These will be the columns that will be included in the calculation of the 
        new uniqueness score.
    columns_uniqueness : list, optional
        A list of containing the computed scores for the each column in each record, 
        the purpose here is to reduce the number of computations when adjusting uniqueness
        to each an individual row, by default None
    records : _type_, optional
        Required when columns_uniqueness is not provided
        (See module docstring for definition of records), by default None

    Returns
    -------
    dict[str, float]
        A dictionary containing uniqueness referencing each of the selected columns
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
    """Group records by values of multiple columns

    Parameters
    ----------
    records : dict[int, dict[str, str]]
        (See module docstring for definition)
    column_map : dict[str, str]
        A mapping of the columns to the values that the records will be grouped by

    Returns
    -------
    dict[int, dict[str, str]]
        Grouped records
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
    """Find records that are duplicated by the value of a single column

    Parameters
    ----------
    records : dict[int, dict[str, str]]
        (See module docstring for definition)
    column : str
        The column to where the duplicated values should be found in

    Yields
    ------
    Generator[dict[int, dict[str, str]]]
        Records where the value in the column have existed more than once
    """
    counter = Counter(r[column] for r in records.values() if r[column])
    return (r for r in records.values() if counter[r[column]] > 1)
