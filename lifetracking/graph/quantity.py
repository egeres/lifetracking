class Quantity:

    def __init__(self, value: int):
        assert isinstance(value, int)
        self.value = value

    def __repr__(self) -> str:
        return f"Q({self.value})"
