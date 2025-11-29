import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

base_url = "https://www.hcpcsdata.com"
main_url = f"{base_url}/Codes"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}

res = requests.get(main_url, headers=headers)

soup = BeautifulSoup(res.text, "html.parser")

#  print all <a> tags

all_a_tags = soup.find_all("a")
all_a_tags1 = soup.find_all("h1")
all_a_tags2 = soup.find_all("h5")

category_links = []

for a in soup.find_all("a"):
    href = a.get("href")
    # filitering href like /Codes/A
    if href and href.startswith("/Codes/") and len(href) == len("/Codes/X"):
        category_links.append(base_url + href)

all_tables = []
print("Extracting Category Links:")

for link in category_links:
    print("Reading:", link)
    response = requests.get(link, headers=headers)

    if response.status_code == 200:

        # Extract h1 and h5 headers

        soup = BeautifulSoup(response.text, "html.parser")

        h1 = soup.find("h1")
        h1_small_text = None
        if h1:
            small = h1.find("small")
            if small:
                h1_small_text = small.get_text(strip=True)  # store small text
                small.decompose()  # remove <small>

            h1_clean = h1.get_text(strip=True)  # clean h1 text
        else:
            h1_clean = None

        h5 = soup.find("h5")

        h1_text = h1.get_text(strip=True) if h1 else None
        h5_text = h5.get_text(strip=True) if h5 else None

        #print("→ h1:", h1_text[7:8])
        #print("→ h5:", h5_text)

        # Extract tables

        tables = pd.read_html(StringIO(response.text))

        if tables:
            df = tables[0]

            # Add header info as columns
            df["Group_Code"] = h1_text[7:8]
            df["Category_name"] = h5_text

            all_tables.append(df)

    else:
        print("Failed:", response.status_code)


# Combine all tables
print("Appending All Pages")
df = pd.concat(all_tables)
df.to_csv("Main_List_HCPCS.csv", index=False) #file name
print("File Created")
