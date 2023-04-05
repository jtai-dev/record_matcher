
from collections import Counter
from collections.abc import Generator


def uniqueness(records: dict[int, dict[str,str]], column: str) -> float:
    items = {r[column] for r in records.values() if r[column]}
    return len(items)/len(records) if len(records) > 0 else 0


def column_names(records: dict[int, dict[str,str]]) -> set[str]:
    return {c for i in records for c in records[i]}


def group_by(records: dict[int, dict[str,str]], column_map: dict[str,str]) -> dict[int, dict[str,str]]:

    grouped = {}

    for i, r in records.items():
        matched = [r.get(column, '') == value for column, value in column_map.items()]
        if all(matched):
            grouped[i] = r

    return grouped


def duplicated(records: dict[int, dict[str,str]], column: str) -> Generator[dict[int, dict[str,str]]]:
    counter = Counter(r[column] for r in records.values() if r[column])
    return (r for r in records.values() if counter[r[column]] > 1)
