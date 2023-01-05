import urllib.request
from bs4 import BeautifulSoup
import re
from dataclasses import dataclass, asdict
import pandas as pd
import utils
from pathlib import Path

rows = []

url = f"http://www.aaproperty.com.hk/aa/week_list.php"
print(f"Grabbing from {url}")
s = BeautifulSoup(urllib.request.urlopen(url), features="html.parser")
# The property listings have class tag "fl" or "fl2"
rows = s.find_all(class_=re.compile("^fl2?$"))


def parse_address(card):
    c = card.find_all("a")[0]
    return c.text.split("(")[0].strip()


def parse_property_id(row):
    c = row.find_all("a")[0]
    return c.text.split(":")[-1].strip().rstrip(")")


def parse_price(row):
    c = row.find_all("td")[1].text
    return c


@dataclass
class Listing:
    address: str
    property_id: str
    price: int


def parse_row(row):
    return Listing(
        address=parse_address(row),
        property_id=parse_property_id(row),
        price=parse_price(row),
    )


def parse_price_to_number(price_str):
    m = re.match("^(\d|,)*萬$", price_str)
    if m:
        return int(m.group(0).rstrip("萬").replace(",", ""))
    m = re.match("^自由(\d|,)*萬", price_str)
    if m:
        return int(m.group(0).rstrip("萬")[2:].replace(",", ""))
    return -1


listings = [parse_row(row) for row in rows]

# Convert Listings into a DataFrame
df = pd.DataFrame.from_records([asdict(l) for l in listings])

# clean the data
df["price_as_number"] = df.price.apply(parse_price_to_number)
df = df[["property_id", "address", "price_as_number", "price"]]
df.sort_values("price_as_number", inplace=True, ignore_index=True)

# write the data to file
today = utils.todayhk()
today_str = today.strftime("%Y%m%d")

out_dir = Path(f"./data/{today_str}")
out_dir.mkdir(parents=True, exist_ok=True)
filename = "listings-aa.csv"

df.to_csv(out_dir / filename, index_label="row_number")
