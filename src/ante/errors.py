class AnteError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}" if message else code)


class UnroutableError(AnteError):
    """A mandatory publish in confirm mode could not be routed."""

    def __init__(self, messages) -> None:
        self.messages = messages
        super().__init__("UNROUTABLE", "mandatory publish could not be routed")
