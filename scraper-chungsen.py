import urllib.request
from bs4 import BeautifulSoup
import re
from dataclasses import dataclass, asdict
import pandas as pd
import utils
from pathlib import Path

rows = []

url = f"http://www.chungsen.com.hk/tc/auction_comlist.php"
print(f"Grabbing from {url}")
s = BeautifulSoup(urllib.request.urlopen(url), features="html.parser")
table = s.find("table")
rows = table.find_all("tr")


def parse_address(row):
    c = row.find_all("td")[0]
    address, _ = c.get_text(strip=True, separator="\n").splitlines()
    return address.rstrip("。")


def parse_property_id(row):
    c = row.find_all("td")[0]
    _, property_id = c.get_text(strip=True, separator="\n").splitlines()

    if ":" in property_id:
        return property_id.split(":")[1].strip()
    else:
        m = re.search(r"(\d|-)+", property_id)
        if m:
            return m.group(0)
        else:
            return None


def parse_floor_area(row):
    c = row.find_all("td")[1]
    return c.text


def parse_salable_area(row):
    c = row.find_all("td")[2]
    return c.text


def parse_price(row):
    c = row.find_all("td")[3]
    return c.text


@dataclass
class Listing:
    address: str
    property_id: str
    floor_area: str
    salable_area: str
    price: str


def parse_row(row):
    return Listing(
        address=parse_address(row),
        property_id=parse_property_id(row),
        floor_area=parse_floor_area(row),
        salable_area=parse_salable_area(row),
        price=parse_price(row),
    )


def parse_price_to_number(price_str):
    """
    Function to attempt to convert the price as a string into a number.
    Returns -1 if it fails to parse the price string
    """
    m = re.match("^(\d|,|\.)*$", price_str)
    if m:
        return float(m.group(0).replace(",", ""))
    if "自由" in price_str:
        return parse_price_to_number(" ".join(price_str.split("自由")[1:]))
    if "億" in price_str and "萬" in price_str:
        m = re.match("(\d*)億(\d*)萬", price_str)
        if m:
            yic_part, man_part = int(m.group(1)), int(m.group(2))
            return 10_000 * yic_part + man_part
        else:
            return -1
    if "億" in price_str and "萬" not in price_str:
        m = re.match("(\d*)億", price_str)
        if m:
            yic_part = int(m.group(1))
            return 10_000 * yic_part
        else:
            return -1
    if "自由" not in price_str and "綠表" in price_str:
        return parse_price_to_number(" ".join(price_str.split("綠表")[1:]))

    return -1


listings = [parse_row(row) for row in rows[1:]]

# Convert listings to dataframe
df = pd.DataFrame.from_records([asdict(l) for l in listings])
df["price_as_number"] = df.price.apply(parse_price_to_number)
df.insert(0, "property_id", df.pop("property_id"))
df.insert(2, "price_as_number", df.pop("price_as_number"))
df.sort_values("price_as_number", inplace=True, ignore_index=True)

today = utils.todayhk()
today_str = today.strftime("%Y%m%d")

out_dir = Path(f"./data/{today_str}")
out_dir.mkdir(parents=True, exist_ok=True)
filename = "listings-chungsen.csv"

df.to_csv(out_dir / filename, index_label="row_number")
