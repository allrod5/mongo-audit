from types import FunctionType


def copy_func(f):
    # type: (FunctionType) -> FunctionType
    """
    Returns a function with same code, globals, defaults, closure, and
    name
    """
    fn = FunctionType(
        f.__code__, f.__globals__, f.__name__, f.__defaults__, f.__closure__)
    fn.__dict__.update(f.__dict__)
    return fn
