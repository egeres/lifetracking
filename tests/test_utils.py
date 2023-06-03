from lifetracking.utils import hash_method


def test_hash_method():
    a = lambda x: x  # noqa: E731
    b = lambda x: x  # noqa: E731
    assert hash_method(a) == hash_method(b)

    c = lambda x: x + 1  # noqa: E731
    assert hash_method(a) != hash_method(c)

    def deffed_a(x):
        return x

    def deffed_b(x):
        return x

    assert hash_method(deffed_a) == hash_method(deffed_b)

    def deffed_c(x):
        return x + 1

    assert hash_method(deffed_a) != hash_method(deffed_c)

    a = lambda x: x["app"] in [  # noqa: E731
        "CivilizationVI.exe",
        "Deathloop.exe",  # A comment
        "factorio.exe",  # Stay away from me
        # Another comment
        "Baba Is You.exe",
        "Cities.exe",  # More comments
    ]
    b = lambda x: x["app"] in [  # noqa: E731
        "CivilizationVI.exe",
        "Deathloop.exe",
        "factorio.exe",
        "Baba Is You.exe",
        "Cities.exe",
    ]
    assert hash_method(a) == hash_method(b)
