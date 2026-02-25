import pandas as pd

# two table merging
def combine_two_tables(person: pd.DataFrame, address: pd.DataFrame) -> pd.DataFrame:
    # output = pd.DataFrame()
    # print(person)
    output = person.merge(address, on = ["personId"], how="left")
    # output = output[["firstName", "lastName", "city", "state"]]
    # print(output)
    return output[["firstName", "lastName", "city", "state"]]

import pandas as pd

def combine_two_tables(person: pd.DataFrame, address: pd.DataFrame) -> pd.DataFrame:
    # output = pd.DataFrame()
    # print(person)
    output = person.merge(address, on = ["personId"], how="left")
    # output = output[["firstName", "lastName", "city", "state"]]
    # print(output)
    return output[["firstName", "lastName", "city", "state"]]
