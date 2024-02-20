class TBConfigColumnNotFound(Exception):
    def __init__(self, column, columns):
        super().__init__(
            f"Column \'{column}\' cannot be found. Only these "
            f"columns can be used: {', '.join(columns)}"
        )

class TBConfigXUniqueConstraint(Exception):
    def __init__(self, column, config_dict) -> None:
        super().__init__(
            f"Column \'{column}\' already exists in {config_dict} values."
        )

class TBConfigOverwriteError(Exception):
    def __init__(self, column):
        super().__init__(
            f"\'{column}\' already exist. "
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
