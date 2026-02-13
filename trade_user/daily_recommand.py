import pymysql
from datetime import datetime
import pandas as pd
import copy


# queue를 사용할때랑 아닐때 구분 필요
# queue를 사용할때랑 아닐때 구분 필요
def check_buy_new_recommand(test_date=None, index_list=None, test_index=None, test_index_pos=None, test_index_kind=None, df_daily_index_val=None):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    local_index_list = copy.deepcopy(index_list)
    if test_date is None:
        test_date = datetime.now()
    else:
        test_date = datetime.strptime(test_date, '%Y-%m-%d').date()
    test_date = test_date.strftime('%Y-%m-%d')

    results = [[] for _ in range(len(test_index))]
    recommand = [[] for _ in range(len(test_index))]

    # back_test 중 check_buy_new_recommand를 테스트하지 않는 경우
    if test_index_kind != 3:
        for j, test_value in enumerate(test_index):
            cursor.execute(f"SELECT code from daily_recommand where trade_date = '{test_date}';")
            for code_num in cursor:
                code = code_num[0]
                results[j].append([code, 'N'])
        
        # Should probably return here or skip the optimization logic block
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        return results
        
    # back_test 중 check_buy_new_recommand를 테스트하는 경우
    else:
        sql_code_info = """
                SELECT code, market_cap 
                FROM company_info 
                WHERE industry NOT LIKE %s 
                AND industry NOT LIKE %s 
                AND products NOT LIKE %s 
                AND products NOT LIKE %s
                AND products NOT LIKE %s
                AND company NOT LIKE %s    
                AND market_cap > 300000000000 
                AND del_yn = 'N'
                ORDER BY market_cap DESC;
                """
        params = ('%의약품%', '%의료%', '%지주%', '%화장품%', '%리츠%', '%리츠%')
        df_code_info = pd.read_sql(sql_code_info, connection, params=params)
        df_code_info_sorted = df_code_info.sort_values(by='market_cap', ascending=False)

        cursor = connection.cursor()
        
        # Calculate df_sig_max locally
        sql_max_daily_index = f"SELECT code, idx_id, ABS(MAX(val)) as max_val " \
            f"FROM daily_index_test " \
            f"WHERE idx_term = 1 AND IDX_ID IN (46, 113)"\
            f"GROUP BY code, idx_id"        
        df_sig_max = pd.read_sql(sql_max_daily_index, connection)

        if df_daily_index_val is None:
            sql_daily_index_test = f"SELECT code, IDX_ID, val, idx_term " \
                f"FROM daily_index_test " \
                f"WHERE trade_date = '{test_date}'"
            df_daily_index_val = pd.read_sql(sql_daily_index_test, connection)

        for j, test_value in enumerate(test_index):
            local_index_list = copy.deepcopy(index_list)
            local_index_list[test_index_pos[0]][test_index_pos[1]] = test_value
        
        # Optimize filtering
        # Pre-filter df_sig_max and df_daily_index_val by codes in df_code_info_sorted?
        # Actually standard loop is fine if dfs are indexed or small enough.
        
    # Debug counters
    stats = {'processed': 0, 'no_index': 0, 'cond1': 0, 'cond2': 0, 'cond3': 0, 'cond4': 0, 'accepted': 0}
    
    for index, row in df_code_info_sorted.iterrows():
        stats['processed'] += 1
        result = {46: [], 97: [], 113: []}
        daily_code = row['code']
        no_index_flag = False
        for key, _ in local_index_list.items():                
            if key == 46 or key == 113:
                max_abs_value = df_sig_max[(df_sig_max["code"] == daily_code) & (df_sig_max["idx_id"] == key)]['max_val'].values
                if len(max_abs_value) == 0:
                    no_index_flag = True
                    break  # 해당 조건의 데이터가 없는 경우 건너뛰기
                max_abs_value = max_abs_value[0]
                sig_max = max_abs_value+1e-5
            elif key == 97:
                sig_max = 100
            filtered_daily_index_val_df = df_daily_index_val[(df_daily_index_val["code"] == daily_code) & (df_daily_index_val["IDX_ID"] == key)]

            if filtered_daily_index_val_df.shape[0] != 2:
                no_index_flag = True
                break
            
            idx = filtered_daily_index_val_df[filtered_daily_index_val_df["idx_term"] == 0]['val'].values[0]
            sig = filtered_daily_index_val_df[filtered_daily_index_val_df["idx_term"] == 1]['val'].values[0]                
            result[key].append(idx / sig_max)
            result[key].append(sig / sig_max)
        
        # if stats['processed'] % 20 == 0:
        #      print(f"Stats check: {stats}", flush=True)

        if no_index_flag:
            stats['no_index'] += 1
            continue
        
        # print(f"Checking {daily_code} Result: {result}", flush=True)

        for j, test_value in enumerate(test_index):
            if len(recommand[j]) >= 20:
                continue
            local_index_list = copy.deepcopy(index_list)
            local_index_list[test_index_pos[0]][test_index_pos[1]] = test_value
            
            cond1 = all(result[key][0] >= local_index_list[key][0] for key, _ in result.items())
            cond2 = all(result[key][0] < local_index_list[key][1] for key, _ in result.items())
            cond3 = all(result[key][1] >= local_index_list[key][2] for key, _ in result.items())
            cond4 = all(result[key][1] < local_index_list[key][3] for key, _ in result.items())
            
            if not cond1: stats['cond1'] += 1
            if not cond2: stats['cond2'] += 1
            if not cond3: stats['cond3'] += 1
            if not cond4: stats['cond4'] += 1
            
            if cond1 and cond2 and cond3 and cond4:
                results[j].append([daily_code, 'N'])
                recommand[j].append(daily_code)
                stats['accepted'] += 1
            else:
                pass 
                # if stats['processed'] < 5: # Sample print
                #    print(f"Rejected {daily_code}: c1({cond1}) c2({cond2}) c3({cond3}) c4({cond4}) Result: {result} vs Param: {local_index_list}")

            if len(recommand[j]) > 0:                    
                cursor.execute(f"DELETE FROM daily_recommand_test WHERE trade_date  = '{test_date}' and index_order = {j};")
                for code in recommand[j]:
                    cursor.execute(f"INSERT INTO daily_recommand_test (trade_date, code, index_order) VALUES ('{test_date}', '{code}', {j});")
    
    # print(f"Date: {test_date} Stats: {stats}", flush=True)
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    return results
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    return results


def check_buy_hold_recommand(test_date, code_results, filtered_stock_hold, index_list=None, test_index=None,
                             test_index_pos=None, test_index_kind=None, df_daily_index_val=None):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    local_index_list = {46: [1, 1, 1, 1], 97: [1, 1, 1, 1], 113: [1, 1, 1, 1]}
    query = """
            SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,  
                   idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high,  
                   idx3_idx_low, idx3_idx_high, idx3_sig_low, idx3_sig_high 
            FROM test_index_list
            WHERE test_index_kind = 0;
            """
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        idx1_values = row[0:4]
        idx2_values = row[4:8]
        idx3_values = row[8:12]
        local_index_list[46] = list(idx1_values)
        local_index_list[97] = list(idx2_values)
        local_index_list[113] = list(idx3_values)

    sql_max_daily_index = f"SELECT code, idx_id, val " \
        f"FROM daily_index_test " \
        f"WHERE idx_term = 1 AND val IS NOT NULL"
    df_sig_max = pd.read_sql(sql_max_daily_index, connection)

    if df_daily_index_val is None:
        sql_daily_index_text = f"SELECT * " \
            f"FROM daily_index_test " \
            f"WHERE trade_date = '{test_date}'"
        df_daily_index_val = pd.read_sql(sql_daily_index_text, connection)

    for j, test_value in enumerate(test_index):
        if test_index_kind == 0:
            local_index_list = copy.deepcopy(index_list)
            local_index_list[test_index_pos[0]][test_index_pos[1]] = test_value
        # if local_index_list[test_index_pos[0]][0] > local_index_list[test_index_pos[0]][1] \
        #         or local_index_list[test_index_pos[0]][2] > local_index_list[test_index_pos[0]][3]:
        #     continue
        result = {key: [] for key in local_index_list.keys()}

        for daily_code in filtered_stock_hold[j]:
            no_index_flag = False
            for key, _ in local_index_list.items():
                # 신규로 불러온 index_list가 특정 날짜, 특정 코드, 특정 index가 저장된 게 있으면 저장된 값 불러옴
                filtered_sig_max_df = df_sig_max[(df_sig_max["code"] == daily_code) & (df_sig_max["idx_id"] == key)]
                if not filtered_sig_max_df.empty:
                    filtered_sig_max_df = filtered_sig_max_df.copy()  # 복사본 생성
                    filtered_sig_max_df["abs_val"] = filtered_sig_max_df["val"].abs()  # abs 값 계산
                    max_abs_value = filtered_sig_max_df["abs_val"].max()
                else:
                    max_abs_value = None
                if max_abs_value is None:
                    no_index_flag = True
                    break
                sig_max = max_abs_value
                if key == 97:
                    sig_max = 100
                filtered_daily_index_val_df = df_daily_index_val[
                    (df_daily_index_val["code"] == daily_code) & (df_daily_index_val["IDX_ID"] == key)]
                if filtered_daily_index_val_df.shape[0] != 2:
                    no_index_flag = True
                    break
                filtered_daily_index_val_df = filtered_daily_index_val_df.sort_values(by='idx_term')
                idx = filtered_daily_index_val_df['val'].iloc[0]
                sig = filtered_daily_index_val_df['val'].iloc[1]
                result[key].append(idx / sig_max)
                result[key].append(sig / sig_max)
            if no_index_flag:
                continue
            if all(result[key][0] >= local_index_list[key][0] for key, _ in result.items()) and \
                    all(result[key][0] < local_index_list[key][1] for key, _ in result.items()) and \
                    all(result[key][1] >= local_index_list[key][2] for key, _ in result.items()) and \
                    all(result[key][1] < local_index_list[key][3] for key, _ in result.items()):
                code_results[j].append([daily_code, 'H'])
    if cursor:
        cursor.close()
    if connection:
        connection.close()
