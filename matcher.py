import utils
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pandas as pd
from address_parser import Address

today = utils.todayhk()
today_str = today.strftime("%Y%m%d")

table_names = ["century21", "chungsen", "aa"]

dfs = {k: pd.read_csv(f"./data/{today_str}/listings-{k}.csv") for k in table_names}


def parse_address_helper(address):
    """
    Wraps the ParseAddress instance method into a function that plays with with executor.map
    executor.map is used for parallel processing
    """
    return Address(address).ParseAddress()


@utils.timeit
def build_address_map(df):
    """
    Wrapper function to help parallelize the requests sent to the API.
    """

    # number of workers to paralleize requests over. Can increase to speed up building the address map.
    # If there are too many workers, the API might throttle requests
    MAX_WORKERS = 6

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(parse_address_helper, list(df.address.values))
        return {k: v for k, v in zip(df.index, results)}


# Store the address JSON for a given listing key and row_number
# Address JSONs can be accessed as follows: address_map[TABLE_NAME][ROW_NUMBER]
address_map = {k: build_address_map(dfs[k]) for k in table_names}


def get_building_name(address):
    """
    Extract and return the chinese building name from the address JSON
    Return None if there is no BuildingName field
    """
    return address["chi"].get("BuildingName", None)


def get_estate_name(address):
    """
    Extract and return the chinese estate name from the address JSON.
    Return None if there is no EstateName field
    """
    return address["chi"].get("ChiEstate", {}).get("EstateName", None)


def is_match(left_address, right_address):
    """
    Takes 2 addresses and returns True if the address matches given the matching criterion
    The matching criteria for 2 addresses to match is that they have both the same EstateName
    OR they both have the same BuildingName
    Otherwise, we consider the two addresses a mismatch
    """
    left_building_name = get_building_name(left_address)
    left_estate_name = get_estate_name(left_address)

    right_building_name = get_building_name(right_address)
    right_estate_name = get_estate_name(right_address)

    if (
        left_building_name
        and right_building_name
        and left_building_name == right_building_name
    ):
        return True

    if left_estate_name and right_estate_name and left_estate_name == right_estate_name:
        return True

    return False


def find_matches(left_table_name, right_table_name):
    """
    Return a list of potential matches between two tables.
    The returned list will have items of the form (left_table_name, left_idx, right_table_name, right_idx).
    The list should have no duplicates and should be in sorted order
    """
    potential_matches = list()
    for left_idx, left_address in address_map[left_table_name].items():
        for right_idx, right_address in address_map[right_table_name].items():
            if is_match(left_address, right_address) is True:
                potential_matches.append(
                    (left_table_name, left_idx, right_table_name, right_idx)
                )

    # remove duplicates
    potential_matches = list(set(potential_matches))

    return sorted(potential_matches)


# merge all the pair wise matches
matches = (
    find_matches(table_names[0], table_names[1])
    + find_matches(table_names[0], table_names[2])
    + find_matches(table_names[1], table_names[2])
)

# build data for the DataFrame
df_rows = []
for (left_table_name, left_idx, right_table_name, right_idx) in matches:
    # In addition to the left/right keys in indices, we also
    # want to put the address into the DataFrame for easy reference
    df_rows.append(
        (
            left_table_name,
            left_idx,
            f"{dfs[left_table_name].iloc[left_idx].address}",
            right_table_name,
            right_idx,
            f"{dfs[right_table_name].iloc[right_idx].address}",
        )
    )

# Create the DataFrame from the DataFrame rows
df_matches = pd.DataFrame(
    df_rows,
    columns=[
        "table_from",
        "row_from",
        "address_from",
        "table_to",
        "row_to",
        "address_to",
    ],
)

# write the DataFrame out to a File
today = utils.todayhk()
today_str = today.strftime("%Y%m%d")

out_dir = Path(f"./data/{today_str}")
out_dir.mkdir(parents=True, exist_ok=True)

filename = "matches.csv"
df_matches.to_csv(out_dir / filename, index_label="row_number")

filename = "matches.xlsx"
df_matches.to_excel(out_dir / filename, index_label="row_number")
