import pymysql
import pandas as pd

def check_indices():
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd', autocommit=True)
    cursor = connection.cursor()
    
    print("Checking daily_index_test counts by IDX_ID and idx_term...")
    sql = "SELECT IDX_ID, idx_term, count(*) FROM daily_index_test GROUP BY IDX_ID, idx_term ORDER BY IDX_ID, idx_term"
    cursor.execute(sql)
    for row in cursor.fetchall():
        print(f"IDX_ID: {row[0]}, idx_term: {row[1]}, count: {row[2]}")
        
    print("\nChecking daily_index_test sample values...")
    sql = "SELECT * FROM daily_index_test LIMIT 5"
    cursor.execute(sql)
    for row in cursor.fetchall():
        print(row)

    print("\nChecking five_minute_index_test counts by IDX_ID...")
    sql = "SELECT IDX_ID, idx_term, count(*) FROM five_minute_index_test GROUP BY IDX_ID, idx_term ORDER BY IDX_ID, idx_term"
    cursor.execute(sql)
    for row in cursor.fetchall():
        print(f"IDX_ID: {row[0]}, idx_term: {row[1]}, count: {row[2]}")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    check_indices()
