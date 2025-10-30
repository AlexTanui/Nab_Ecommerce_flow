import os
import re
import requests
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
from datetime import datetime
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

# === 1️⃣ Download the latest NAB ORSI PDF ===
print("🔍 Searching for the latest NAB Online Retail Sales Index PDF...")
index_page = requests.get("https://business.nab.com.au/category/online-retail-sales-index/").text
match = re.search(
    r'href="(https://business\.nab\.com\.au/wp-content/uploads/[^"]*NAB-Online-Retail-Sales-Index-[^"]*\.pdf)"',
    index_page, re.I)
if not match:
    raise Exception("❌ Could not find the latest NAB ORSI PDF.")
pdf_url = match.group(1)
pdf_name = pdf_url.split("/")[-1]
print(f"📥 Downloading {pdf_name} ...")
r = requests.get(pdf_url)
r.raise_for_status()
with open(pdf_name, "wb") as f:
    f.write(r.content)
print("✅ PDF downloaded successfully.")

# === 2️⃣ Extract page 4 ===
PAGE_TO_EXTRACT = 4
reader = PdfReader(pdf_name)
writer = PdfWriter()
writer.add_page(reader.pages[PAGE_TO_EXTRACT - 1])
single_page_pdf = "NAB_Page4.pdf"
with open(single_page_pdf, "wb") as out_pdf:
    writer.write(out_pdf)
print("✂️ Extracted page 4 successfully.")

# === 3️⃣ OCR ===
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

# === 4️⃣ Extract Table 3 & 4 text blocks ===
t3 = re.search(r"(Table\s*3[\s\S]*?)(?=Table\s*4|$)", page_text, re.I)
t4 = re.search(r"(Table\s*4[\s\S]*?)(?=About|$)", page_text, re.I)
table3_text = t3.group(1).strip() if t3 else ""
table4_text = t4.group(1).strip() if t4 else ""
os.makedirs("output", exist_ok=True)
open("output/NAB_Table3_raw.txt", "w", encoding="utf-8").write(table3_text)
open("output/NAB_Table4_raw.txt", "w", encoding="utf-8").write(table4_text)
print("✅ OCR complete and text saved.")

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
print("💾 CSVs saved in /output/")

# === 6️⃣ Upload to Snowflake ===
print("☁️ Connecting to Snowflake ...")
conn = snowflake.connector.connect(**connection_parameters)
cur = conn.cursor()

for csv_file in [csv3, csv4]:
    file_name = os.path.basename(csv_file)
    put_sql = f"PUT file://{csv_file} {STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    print(f"⬆️ Uploading {file_name} ...")
    cur.execute(put_sql)
    print(f"✅ Uploaded {file_name} to {STAGE_NAME}")

# === 7️⃣ COPY INTO Bronze tables ===
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
    """
]
for sql in copy_commands:
    cur.execute(sql)
    print("✅ Table updated successfully.")

cur.close()
conn.close()
print("🚀 All tasks completed successfully!")
