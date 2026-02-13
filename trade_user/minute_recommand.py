import time
from datetime import datetime
import multiprocessing
import pandas as pd
import copy
import pymysql

def check_minute_recommand(last_date, test_date, code_results, index_list=None, test_index=None, test_index_pos=None,
                           test_index_kind=None, df_five_minute_price=None):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    local_index_list = {97: [0.5, 0.7, 0.3, 0.6, 0.5, 0.7, 0.3, 0.6]}
    query = """
                    SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,
                    idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high                             
                    FROM test_index_list
                    WHERE test_index_kind = 2;
                    """
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        idx1_values = row[0:8]
        local_index_list[97] = list(idx1_values)


    
    test_date_formatted = pd.to_datetime(test_date)
    last_date_formatted = pd.to_datetime(last_date)
    
    df_minute_index_val = None
    target_codes = set()
    for j in range(len(test_index)):
        for item in code_results[j]:
            target_codes.add(item[0])
    
    if target_codes:
        formatted_codes = "', '".join(target_codes)
        sql = f"SELECT * FROM five_minute_index_test WHERE trade_date = '{test_date}' AND code IN ('{formatted_codes}')"
        df_minute_index_val = pd.read_sql(sql, connection)
    else:
        df_minute_index_val = pd.DataFrame()

    # 내일 날짜 기준 5분봉 실제 값 조회
    if df_five_minute_price is None:
        if 'target_codes' not in locals():
            target_codes = set()
            for j in range(len(test_index)):
                for item in code_results[j]:
                    target_codes.add(item[0])
                    
        if target_codes:
            formatted_codes = "', '".join(target_codes)
            sql_five_minute_price = f"SELECT code, close, trade_date FROM five_minute_price WHERE trade_date between '{test_date} 09:00:00' and '{test_date} 15:30:00' AND code IN ('{formatted_codes}')"
            df_five_minute_price = pd.read_sql(sql_five_minute_price, connection)
        else:
             df_five_minute_price = pd.DataFrame()

    # 전날 bolinger band value 조회
    sql_stock_band_index = f"SELECT code, idx_term, val FROM daily_index_test where trade_date = '{last_date}' and IDX_ID = 1;"
    df_stock_band_index = pd.read_sql(sql_stock_band_index, connection)

    # 전날 종가 조회
    sql_daily_price = f"SELECT code, trade_date, close FROM daily_price where trade_date between '{last_date}' and '{test_date}';"
    df_daily_price = pd.read_sql(sql_daily_price, connection)
    df_daily_price["trade_date"] = pd.to_datetime(df_daily_price["trade_date"])

    

    daily_total_cnt = 77
    trade_signal_list = [[] for _ in range(len(test_index))]

    for j, test_value in enumerate(test_index):
        if test_index_kind == 2:
            local_index_list = copy.deepcopy(index_list)
            local_index_list[test_index_pos[0]][test_index_pos[1]] = test_value

        for code_result in code_results[j]:
            daily_code = code_result[0]
            base = {key: [] for key in local_index_list.keys()} 
            if 97 not in base: base[97] = []

            band_val_series = df_stock_band_index[(df_stock_band_index["code"] == daily_code) & (df_stock_band_index["idx_term"] == 0)]["val"]
            if band_val_series.empty:
                # print(f"Missing band_val for {daily_code}")
                continue
            last_band_val = band_val_series.values[0]
            
            band_sig_series = df_stock_band_index[(df_stock_band_index["code"] == daily_code) & (df_stock_band_index["idx_term"] == 1)]["val"]
            if band_sig_series.empty:
                # print(f"Missing band_sig for {daily_code}")
                continue
            last_band_sig = band_sig_series.values[0]

            close_series = df_daily_price[(df_daily_price["code"] == daily_code) & (df_daily_price["trade_date"] == test_date_formatted)]["close"]
            if close_series.empty:
                # print(f"Missing close for {daily_code} on {test_date}")
                continue
            close = close_series.values[0]

            last_close_series = df_daily_price[(df_daily_price["code"] == daily_code) & (df_daily_price["trade_date"] == last_date_formatted)]["close"]
            if last_close_series.empty:
                # print(f"Missing last_close for {daily_code} on {last_date}")
                continue
            last_close = last_close_series.values[0]

            mid_point = (last_band_val + last_band_sig) / 2            

            # H : 보유, N : 신규
            action = code_result[1]
            no_index_flag = False
            # index_list에는 signal선이 있는 지표로 계산            
            sig_max = 100
            filtered_minute_index_val_df = df_minute_index_val[(df_minute_index_val["code"] == daily_code) & (df_minute_index_val["idx_id"] == 97)]
            if filtered_minute_index_val_df.shape[0] != 154:
                no_index_flag = True
                break
            for minute in sorted(filtered_minute_index_val_df['minute_order'].unique()):
                temp = []
                # 해당 minute_order에 대한 모든 idx_term 값을 순회
                for idx_term in sorted(filtered_minute_index_val_df['idx_term'].unique()):
                    val = filtered_minute_index_val_df[(filtered_minute_index_val_df['minute_order'] == minute)
                                                & (filtered_minute_index_val_df['idx_term'] == idx_term)]['val']
                    if not val.empty:
                        temp.append(val.values[0])
                    else:
                        temp.append(None)  # idx_term 값이 없을 경우 None으로 처리
                base[97].append(temp)
            if no_index_flag:
                continue

            trade_signal = []
            # i가 1인 경우가 9시00분부터 9시05분 사이의 5분봉임 i가 77인 3시20분부터 3시30분 사이의 데이터는 무시
            for i in range(0, daily_total_cnt-1):
                result = {key: [] for key in local_index_list.keys()}
                for key, _ in local_index_list.items():
                    # result = {MACD : [[term1, term2, signal](과거), [term1, term2, signal](최근)... ]}.
                    idx = base[key][i][0]
                    sig = base[key][i][1]
                    result[key].append(idx / sig_max)
                    result[key].append(sig / sig_max)

                
                # 보유 종목인 경우
                if action == 'H':                                                            
                    # 전날 데이터가 (sig, (sig+idx)/2) 구간에 있는 경우                    
                    if sig < last_close < mid_point and sig < df_five_minute_price[df_five_minute_price["code"] == daily_code].iloc[i]["close"] < mid_point:                            
                        pass
                    # 전날 데이터가 ((sig+idx)/2, idx) 구간에 있는 경우
                    elif mid_point < last_close < idx and mid_point < df_five_minute_price[df_five_minute_price["code"] == daily_code].iloc[i]["close"] < idx:
                        pass
                    else:
                        trade_signal.append([i, 'S'])                
                    continue
                # 과매수, 과매도 구간에서는 매수/매도 안하도록
                if all(result[key][0] >= local_index_list[key][0] for key, _ in result.items()) and \
                        all(result[key][0] < local_index_list[key][1] for key, _ in result.items()) and \
                        all(result[key][1] >= local_index_list[key][2] for key, _ in result.items()) and \
                        all(result[key][1] < local_index_list[key][3] for key, _ in result.items()):
                    trade_signal.append([i, 'B'])                
            # 하나의 index에 대한 signal list
            trade_signal_list[j].append([daily_code, trade_signal])
    return trade_signal_list

if __name__ == '__main__':
    start_time = time.time()
    queue = multiprocessing.Queue()
    check_minute_recommand(queue, "2024-05-20", ["A000100", 0])
    end_time = time.time()
    elapsed_time = end_time - start_time  # 실행 시간 계산
    print(f"실행 시간: {elapsed_time} 초")


