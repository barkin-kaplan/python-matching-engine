import os


def check_and_get_variable(var_name: str) -> str:
    var = os.environ.get(var_name)
    if var is None:
        raise Exception(f"Environment variable {var_name} is not set.")
    
    return var