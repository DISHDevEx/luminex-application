#from .conftest import add

def test_add():
    a = (1,2,3)
    b = 7
    assert sum(a,b) == 13
    assert sum(a,7) == 13