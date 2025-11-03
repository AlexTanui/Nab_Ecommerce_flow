import snowflake.connector
import os

# Clear proxies (useful if on VPN)
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

# --- Snowflake connection parameters ---
connection_parameters = {
    "account": "TFYZALX-OE13355",     # ✅ Full org + account locator
    "user": "ALEXTANUI",              # ✅ All caps usually fine
    "authenticator": "externalbrowser",  # ✅ Uses browser SSO
    "role": "DEVELOPER",
    "warehouse": "XS_WH",
    "database": "LHI",
    "schema": "SANDBOX_BRONZE",
}

print("☁️ Connecting to Snowflake using externalbrowser authentication...")

try:
    # This will open a browser window for login
    conn = snowflake.connector.connect(**connection_parameters)
    cur = conn.cursor()
    print("✅ Connection successful!\n")

    cur.execute("SELECT CURRENT_VERSION(), CURRENT_REGION(), CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_USER();")
    result = cur.fetchall()[0]

    print("🔍 Connection details:")
    print(f"  ❯ Snowflake Version : {result[0]}")
    print(f"  ❯ Region             : {result[1]}")
    print(f"  ❯ Account            : {result[2]}")
    print(f"  ❯ Role               : {result[3]}")
    print(f"  ❯ User               : {result[4]}")

    cur.close()
    conn.close()
    print("\n🚀 Test complete — externalbrowser login working correctly.")
except Exception as e:
    print("\n❌ Connection failed.")
    print(f"Error: {e}")
