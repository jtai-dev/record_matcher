# Record Matcher

Record Matcher is a tool that find matches between two sets of records. It provides a flexible and customizable framework to identify and link related 
records through a configurable scoring algorithms and criterias. 


Suppose you have **records X** that looks like this:

| ID    | First Name | Last Name | Country        | Sex | Age |
|-------|------------|-----------|----------------|-----|-----|
| 10244 | Rube       | Miller    | United States  | M   | 27  |
| 23012 | Kim        | Thornton  | United Kingdom | F   | 39  |
| 46882 | Jane       | van Doe   | Netherlands    | F   | 16  |
| 51459 | Luca       | Schmidt   | Germany        | F   | 8   |


And, you want to compare with **records Y** to find a match:

| ID     | First Name | Nickname | Last Name | Country  | Sex    | Age |
|--------|------------|----------|-----------|----------|--------|-----|
| A1X012 | Reuben     | Rube     | Miller    | USA      | Male   | 27  |
| B0C105 | Kimberly   | Kim      | Thornton  | UK       | Female | 39  |
| C4L092 | Jane       | Jane     | van Doe   | NL       | Female | 16  |
| D2P451 | Jonathan   | Jon      | Schmidt   | Germany  | Male   | 45  |



Records are data structures representing tabular data, where each record is a dictionary mapping column names to values. 

```python
records_x = {
    0: {'id': 10244, 'firstname': 'Reuben', 'lastname': 'Miller', 'country': 'USA', 'sex': 'M'},
    1: {'id': 23012, 'firstname': 'Kimberly', 'lastname': 'Thornton', 'country': 'UK', 'sex': 'F'},
    2: {'id': 46882, 'firstname': 'Jane', 'lastname': 'van Doe', 'country': 'Netherlands', 'sex': 'F'},
    3: {'id': 51459, 'firstname': 'Luca', 'lastname': 'Schmidt', 'country': 'Germany', 'sex': 'F'}
    }

records = {
    0: {'id': 'A1X012', 'firstname': 'Reuben', 'nickname': 'Rube', 'lastname': 'Miller', 'country': 'USA', 'sex': 'Male', 'age': 27},
    1: {'id': 'B0C105', 'firstname': 'Kimberly', 'nickname': 'Kim', 'lastname': 'Thornton', 'country': 'UK', 'sex': 'Female', 'age': 39},
    2: {'id': 'C4L092', 'firstname': 'Jane', 'nickname': 'Jane', 'lastname': 'van Doe', 'country': 'NL', 'sex': 'Female', 'age': 16},
    3: {'id': 'D2P451', 'firstname': 'Jonathan', 'nickname': 'Jon', 'lastname': 'Schmidt', 'country': 'Germany', 'sex': 'Male', 'age': 45},
}

```


