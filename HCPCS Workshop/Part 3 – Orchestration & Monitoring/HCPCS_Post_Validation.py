import pymysql
import traceback
import subprocess
import sys

#  alert message
def send_alert(subject, message):
    """Send email using msmtp"""
    try:
        subprocess.run(
            ["msmtp", "rahulrogz13@gmail.com"],
            input=f"Subject: {subject}\n\n{message}".encode(),
            check=True
        )
    except Exception as e:
        print("Failed to send alert:", e)



#  Run validation function

def run_query(cursor, query, description, fails):
    cursor.execute(query)
    result = cursor.fetchall()
    if result:
        fails.append(f"❌ {description} FAILED:\n{result}\n")
    else:
        print(f"✔ {description} passed")
    return fails



#  Main validation execution

def main():
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='root',
            database='hcpcs',
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        msg = f"DB Connection Failed:\n{e}\n{traceback.format_exc()}"
        print(msg)
        send_alert("HCPCS Validation Failed - DB Error", msg)
        sys.exit(1)

    fails = []

    try:
        with connection.cursor() as cursor:

            # 1. Active duplicates
            run_query(cursor, """
                SELECT hcpcs_code, COUNT(*) AS cnt
                FROM hcpcs_codes
                WHERE is_current = 'Y'
                GROUP BY hcpcs_code
                HAVING COUNT(*) > 1;
            """, "Active duplicate hcpcs_code check", fails)

            # 2. Null checks
            run_query(cursor, """
                SELECT *
                FROM hcpcs_codes
                WHERE hcpcs_code IS NULL
                   OR long_description IS NULL
                   OR group_code IS NULL
                   OR category_name IS NULL;
            """, "Null / required field check", fails)

            # 3. effective_date <= end_date
            run_query(cursor, """
                SELECT *
                FROM hcpcs_codes
                WHERE end_date IS NOT NULL
                  AND effective_date > end_date;
            """, "Effective date <= end date check", fails)

            # 4. Future effective date
            run_query(cursor, """
                SELECT *
                FROM hcpcs_codes
                WHERE effective_date > CURRENT_DATE;
            """, "Effective date in future check", fails)

            # 5. is_current=Y but end_date not null
            run_query(cursor, """
                SELECT *
                FROM hcpcs_codes
                WHERE is_current = 'Y'
                  AND end_date IS NOT NULL;
            """, "Current record shouldn't have end_date", fails)

            # 6. is_current=N but end_date null
            run_query(cursor, """
                SELECT *
                FROM hcpcs_codes
                WHERE is_current = 'N'
                  AND end_date IS NULL;
            """, "Closed records must have end_date", fails)

            # 7. same code with multiple descriptions but proper SCD2 history
            run_query(cursor, """
                SELECT hcpcs_code
                FROM hcpcs_codes
                GROUP BY hcpcs_code
                HAVING COUNT(DISTINCT long_description) > 1   -- Versioning happened
                  AND MAX(CASE WHEN is_current = 'Y' THEN 1 ELSE 0 END) = 0;;
            """, "Multiple descriptions but proper SCD2 close", fails)

    except Exception as e:
        msg = f"Validation Script Failed:\n{e}\n{traceback.format_exc()}"
        print(msg)
        send_alert("HCPCS Validation Failed - Runtime Error", msg)
        sys.exit(1)

    finally:
        connection.close()

   
    #  Send results
    
    if fails:
        fail_message = "\n".join(fails)
        print("Validation Failed!")
        send_alert("HCPCS Data Quality Check Failed", fail_message)
        sys.exit(2)

    print("All validations passed.")
    send_alert("HCPCS Validation Success", "All validation checks passed successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()