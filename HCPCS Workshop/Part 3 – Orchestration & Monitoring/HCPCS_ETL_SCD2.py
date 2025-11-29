import pandas as pd
import pymysql
from datetime import datetime
import traceback
import subprocess
import sys

CSV_PATH = "/mnt/c/Users/USER/PycharmProjects/PythonProject/hcpcs/HCPCS/Main_List_HCPCS.csv"   # getting data from Windows path to linux 

# Email alert command using msmtp example
def send_alert(subject, message):
    try:
        subprocess.run(
            ["msmtp", "rahulrogz13@gmail.com"],
            input=f"Subject: {subject}\n\n{message}".encode(),
            check=True
        )
    except Exception as e:
        print("Failed to send alert:", e)


try:
    df = pd.read_csv(CSV_PATH)
    print("CSV loaded successfully")
except Exception as e:
    err = f"CSV Load Failed: {str(e)}"
    print(err)
    send_alert("HCPCS Pipeline Failed - CSV Error", err)
    sys.exit(1)  # cron will detect failure

#mysql connection part
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root',
    database='hcpcs',
    cursorclass=pymysql.cursors.DictCursor
)

errors = []   # to collect row-level errors


try:
    with connection.cursor() as cursor:
        for index, row in df.iterrows():
            try:
                code = row["Code"]
                desc = row["Description"]
                grp = row["Group_Code"]
                cat = row["Category_name"]

                # 1. Exact match → skip
                cursor.execute("""
                    SELECT 1 FROM hcpcs_codes
                    WHERE hcpcs_code = %s
                      AND long_description = %s
                      AND is_current = 'Y'
                """, (code, desc))
                if cursor.fetchone():
                    continue

                # 2. Same code but description changed → SCD2
                cursor.execute("""
                    SELECT id FROM hcpcs_codes
                    WHERE hcpcs_code = %s
                      AND long_description <> %s
                      AND is_current = 'Y'
                """, (code, desc))
                old = cursor.fetchone()

                if old:
                    cursor.execute("""
                        UPDATE hcpcs_codes
                        SET end_date = CURRENT_DATE,
                            is_current = 'N'
                        WHERE id = %s
                    """, (old['id'],))

                    cursor.execute("""
                        INSERT INTO hcpcs_codes (
                            group_code, category_name, hcpcs_code,
                            long_description, effective_date, end_date, is_current
                        )
                        VALUES (%s, %s, %s, %s, CURRENT_DATE, NULL, 'Y')
                    """, (grp, cat, code, desc))
                    continue

                # 3. Same description + new code → insert as new
                cursor.execute("""
                    SELECT id FROM hcpcs_codes
                    WHERE long_description = %s
                      AND hcpcs_code <> %s
                """, (desc, code))
                if cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO hcpcs_codes (
                            group_code, category_name, hcpcs_code,
                            long_description, effective_date, end_date, is_current
                        )
                        VALUES (%s, %s, %s, %s, CURRENT_DATE, NULL, 'Y')
                    """, (grp, cat, code, desc))
                    continue

                # 4. Completely new
                cursor.execute("""
                    INSERT INTO hcpcs_codes (
                        group_code, category_name, hcpcs_code,
                        long_description, effective_date, end_date, is_current
                    )
                    VALUES (%s, %s, %s, %s, CURRENT_DATE, NULL, 'Y')
                """, (grp, cat, code, desc))

            except Exception as row_error:
                errors.append(
                    f"Row {index} failed: {str(row_error)}\n{traceback.format_exc()}"
                )

    connection.commit()
    print("Data inserted to table successfully")

except Exception as e:
    # major DB failure
    err_msg = f"Pipeline Failed: {str(e)}\n{traceback.format_exc()}"
    print(err_msg)
    send_alert("HCPCS Pipeline Failed (DB Error)", err_msg)
    sys.exit(1)

finally:
    connection.close()
    print("MySQL connection closed")

# Final step: Row-level errors alert
if errors:
    error_report = "\n\n".join(errors)
    print("Some rows failed. Sending alert...")
    send_alert("HCPCS Pipeline Completed with Row Errors", error_report)
    sys.exit(2)  # partial failure

# Success
send_alert("HCPCS Pipeline Success", "Pipeline ran successfully with 0 errors.")
print("Success alert sent.")
sys.exit(0)