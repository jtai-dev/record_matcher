class TBConfigXColumnNotFound(Exception):
    def __init__(self, column, x_columns):
        super().__init__(
            f"x_column:{column} cannot be found in x_records. Only these "
            f"x_columns may be used: {', '.join(x_columns)}"
        )


class TBConfigYColumnNotFound(Exception):
    def __init__(self, column, y_columns):
        super().__init__(
            f"y_column:{column} cannot be found in y_records. Only these "
            f"y_columns may be used: {', '.join(y_columns)}"
        )


class TBConfigXUniqueConstraint(Exception):
    def __init__(self, column, config_classname) -> None:
        super().__init__(
            f"x_column:{column} already exists in {config_classname} values."
        )


class TBConfigOverwriteError(Exception):
    def __init__(self, column):
        super().__init__(
            f"x_column:{column} is a column in x_records. "
            f"Set allow_overwrite==True to allow for overwriting."
        )


class TBConfigScorerNotFound(Exception):
    def __init__(self, scorer, scorers):
        super().__init__(
            f"Scorer name: '{scorer}' is not found in scorers. "
            f"Only select from the following scorers: "
            f"{', '.join(scorers)}"
        )


class TBConfigColumnToMatchLock(Exception):
    def __init__(self, column):
        super().__init__(
            f"x_column:{column} is a column to be matched. Please remove it "
            f"from ColumnsToMatch first."
        )
