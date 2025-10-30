import os
import re
import requests
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import snowflake.connector

# === 0️⃣ Configuration ===
API_KEY = os.getenv("OCR_SPACE_API_KEY", "K84470994788957")

connection_parameters = {
    "account": "OE13355",
    "user": "AlexTanui",
    "password": "34178202@aLEXMARTIN",
    "role": "DEVELOPER",
    "warehouse": "COMPUTE_WH",
    "database": "LHI",
    "schema": "SANDBOX_BRONZE",
}

STAGE_NAME = "@LHI.SANDBOX_BRONZE.NAB_ONLINE_STAGE"
TABLE3_NAME = "LHI.SANDBOX_BRONZE.NAB_ONLINE_TABLE3_RAW"
TABLE4_NAME = "LHI.SANDBOX_BRONZE.NAB_ONLINE_TABLE4_RAW"
PAGE_TO_EXTRACT = 4

# === 1️⃣ Find & Download Latest NAB ORSI PDF ===
print("🔍 Searching for the latest NAB Online Retail Sales Index PDF...")
index_url = "https://business.nab.com.au/category/online-retail-sales-index/"
resp = requests.get(index_url, timeout=30)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")
pdf_links = []

for a in soup.find_all("a", href=True):
    href = a["href"]
    if href.lower().endswith(".pdf") and "retail" in href.lower():
        full_url = urljoin(index_url, href)
        pdf_links.append(full_url)

if not pdf_links:
    raise Exception("❌ Could not find any NAB ORSI PDF links on the page.")

# Pick the most recent link (usually last in sorted order)
pdf_url = sorted(pdf_links)[-1]
pdf_name = pdf_url.split("/")[-1]

print(f"📥 Downloading latest NAB ORSI PDF: {pdf_name}")
r = requests.get(pdf_url, timeout=30)
r.raise_for_status()
with open(pdf_name, "wb") as f:
    f.write(r.content)
print("✅ PDF downloaded successfully.")

# === 2️⃣ Extract Page 4 ===
print(f"✂️ Extracting page {PAGE_TO_EXTRACT} from PDF ...")
reader = PdfReader(pdf_name)
if PAGE_TO_EXTRACT > len(reader.pages):
    raise Exception(f"❌ PDF has only {len(reader.pages)} pages, cannot extract page {PAGE_TO_EXTRACT}.")
writer = PdfWriter()
writer.add_page(reader.pages[PAGE_TO_EXTRACT - 1])
single_page_pdf = "NAB_Page4.pdf"
with open(single_page_pdf, "wb") as out_pdf:
    writer.write(out_pdf)
print("✅ Page extracted successfully.")

# === 3️⃣ OCR Page via OCR.Space ===
print("📡 Sending page to OCR.Space ...")
with open(single_page_pdf, "rb") as f:
    payload = {
        "apikey": API_KEY,
        "language": "eng",
        "OCREngine": 2,
        "isOverlayRequired": False,
        "scale": True,
        "detectOrientation": True,
    }
    resp = requests.post("https://api.ocr.space/parse/image", files={"file": f}, data=payload)
    resp.raise_for_status()

result = resp.json()
if not result.get("ParsedResults"):
    raise Exception("❌ No OCR results returned. Check API key or file.")
page_text = result["ParsedResults"][0]["ParsedText"]
print("✅ OCR completed successfully.")

# === 4️⃣ Extract Table 3 & 4 Text ===
t3 = re.search(r"(Table\s*3[\s\S]*?)(?=Table\s*4|$)", page_text, re.I)
t4 = re.search(r"(Table\s*4[\s\S]*?)(?=About|$)", page_text, re.I)
table3_text = t3.group(1).strip() if t3 else ""
table4_text = t4.group(1).strip() if t4 else ""

os.makedirs("output", exist_ok=True)
open("output/NAB_Table3_raw.txt", "w", encoding="utf-8").write(table3_text)
open("output/NAB_Table4_raw.txt", "w", encoding="utf-8").write(table4_text)
print("💾 OCR text blocks saved to /output directory.")

# === 5️⃣ Convert to CSV ===
def to_csv_df(block):
    lines = [re.sub(r"\s{2,}", ",", l.strip()) for l in block.split("\n") if l.strip()]
    return pd.DataFrame(lines, columns=["RAW_TEXT"])

df3 = to_csv_df(table3_text)
df4 = to_csv_df(table4_text)
timestamp = datetime.now().strftime("%Y_%m")
csv3 = f"output/NAB_Table3_OCR_{timestamp}.csv"
csv4 = f"output/NAB_Table4_OCR_{timestamp}.csv"
df3.to_csv(csv3, index=False)
df4.to_csv(csv4, index=False)
print(f"✅ CSVs created: {csv3}, {csv4}")

# === 6️⃣ Upload to Snowflake Stage ===
print("☁️ Connecting to Snowflake ...")
conn = snowflake.connector.connect(**connection_parameters)
cur = conn.cursor()

for csv_file in [csv3, csv4]:
    file_name = os.path.basename(csv_file)
    put_sql = f"PUT file://{csv_file} {STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    print(f"⬆️ Uploading {file_name} ...")
    cur.execute(put_sql)
    print(f"✅ Uploaded {file_name} to {STAGE_NAME}")

# === 7️⃣ COPY INTO Bronze Tables ===
print("📥 Refreshing Bronze tables ...")
copy_commands = [
    f"""
    COPY INTO {TABLE3_NAME}
    FROM {STAGE_NAME}
    FILES = ('{os.path.basename(csv3)}.gz')
    FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    ON_ERROR='CONTINUE';
    """,
    f"""
    COPY INTO {TABLE4_NAME}
    FROM {STAGE_NAME}
    FILES = ('{os.path.basename(csv4)}.gz')
    FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    ON_ERROR='CONTINUE';
    """,
]
for sql in copy_commands:
    cur.execute(sql)
    print("✅ Table refreshed successfully.")

cur.close()
conn.close()
print("🚀 NAB ORSI → Snowflake pipeline completed successfully!")
