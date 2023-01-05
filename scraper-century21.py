import urllib.request
from bs4 import BeautifulSoup
import re
from dataclasses import dataclass, asdict
import pandas as pd
import utils
from pathlib import Path

rows = []
for idx in range(1, 100):
    url = f"https://www.c21.hk/BankListProperty/Refresh?selectedPage={idx}&minPageNo=1&maxPageNo=0"
    print(f"Grabbing from {url}")
    s = BeautifulSoup(urllib.request.urlopen(url), features="html.parser")
    rs = s.find_all("div", class_="mb-2")
    print(f"Found {len(rs)} listings")
    if not rs:
        break
    rows += rs


def parse_property_id(row):
    block = row.find("p", class_="propertyid")
    t = block.text
    m = re.search(r"DP/.*/.*", t)
    if m:
        return m.group(0).strip()
    else:
        m = re.search(r"DP/.*", t)
        if m:
            return m.group(0).strip()


def parse_posting_date(row):
    block = row.find("p", class_="propertyid")
    t = block.text
    m = re.search(r"\d*-\d*-\d*", t)
    if m:
        return m.group(0).strip()


def parse_address(row):
    block = row.find("p", class_="address")
    t = block.text
    lines = t.strip().split("\n")
    return (lines[0].strip(), lines[-1].strip())


def parse_prices(row):
    block = row.find_all("p", class_="contact")[0]
    t = block.text
    return t.strip()


def parse_area(row):
    block = row.find_all("p", class_="area")[1]
    t = block.text
    return t.strip()


def parse_sold(row):
    block = row.find("p", class_="sold")
    t = block.text
    return t.strip()


def parse_contact_info(row):
    block = row.find_all("p", class_="contact")[-1]
    t = block.text
    return " ".join(t.split()).title()


@dataclass
class Listing:
    property_id: str
    posting_date: str
    address_1: str
    address_2: str
    prices: str
    area: str
    sold: str
    contact: str


def parse_row(row):
    address_1, address_2 = parse_address(row)
    return Listing(
        property_id=parse_property_id(row),
        posting_date=parse_posting_date(row),
        address_1=address_1,
        address_2=address_2,
        prices=parse_prices(row),
        area=parse_area(row),
        sold=parse_sold(row),
        contact=parse_contact_info(row),
    )


def sold_to_number(sold_str):
    return int(sold_str.replace("$", "").replace("萬", "").replace(",", ""))


listings = [parse_row(row) for row in rows]

# Convert the listings into a DataFrame
df = pd.DataFrame.from_records([asdict(l) for l in listings])

# Clean the Data
df["sold_as_number"] = df.sold.apply(sold_to_number)
df["address"] = df.address_1 + df.address_2
df.drop(["address_1", "address_2", "contact"], inplace=True, axis=1)
df.insert(2, "address", df.pop("address"))
df.insert(3, "sold_as_number", df.pop("sold_as_number"))

df.sort_values("sold_as_number", inplace=True)
df.reset_index(drop=True, inplace=True)

# Write the data to a file
today = utils.todayhk()
today_str = today.strftime("%Y%m%d")

out_dir = Path(f"./data/{today_str}")
out_dir.mkdir(parents=True, exist_ok=True)

filename = "listings-century21.csv"
df.to_csv(out_dir / filename, index_label="row_number")

filename = "listings-century21.xlsx"
df.to_excel(out_dir / filename, index_label="row_number")
