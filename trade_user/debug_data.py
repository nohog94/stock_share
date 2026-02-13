
import pymysql
import pandas as pd

def check_data_integrity():
    connection = pymysql.connect(host='localhost', user='root', password='P@ssw0rd', db='investar', charset='utf8')
    cursor = connection.cursor()
    
    # 1. Check valid pairs in daily_index_test
    print("Checking daily_index_test integrity...")
    sql = """
    SELECT code, trade_date, IDX_ID, COUNT(*) as cnt
    FROM daily_index_test
    GROUP BY code, trade_date, IDX_ID
    HAVING cnt != 2
    LIMIT 10;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    if rows:
        print("Found incomplete index pairs (not 2 records):")
        for r in rows:
            print(r)
    else:
        print("All grouped records have count 2 (Good).")

    # 2. Check sample data values
    print("\nSample Data (Val):")
    sql = "SELECT * FROM daily_index_test LIMIT 5"
    cursor.execute(sql)
    for r in rows:
        print(r)
        
    # 3. Check if any data satisfies recommendation condition roughly
    # Just check if we have any data at all for the target dates
    print("\nData count for 2025-12-01 ~ 2026-01-30:")
    sql = "SELECT COUNT(*) FROM daily_index_test WHERE trade_date BETWEEN '2025-12-01' AND '2026-01-30'"
    cursor.execute(sql)
    print(cursor.fetchone()[0])

    connection.close()

if __name__ == "__main__":
    check_data_integrity()
