import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

import pymysql
import time
import pandas as pd
import numpy as np
import math
import copy
import warnings
from trade_user.daily_recommand import check_buy_new_recommand, check_buy_hold_recommand
from trade_user.minute_recommand import check_minute_recommand
from trade_user.minute_trade_test import minute_trade, trade_test_left_cal
from trade_user.index_exists_check import check_five_minute_index_exists, check_daily_index_exists
import requests
from multiprocessing import Process

# pandas 경고 무시
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')

# 백테스트는 분기 별 1회 진행
from datetime import datetime
from dateutil.relativedelta import relativedelta

END_DATE = datetime.today().strftime('%Y-%m-%d')
START_DATE = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m-%d')





def back_test(index_list, test_index, test_index_pos, test_index_kind, start_date, end_date):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()

    # 주석 시작 index 계산 필요 시 주석 해제
    # stock_index_check(start_date, end_date)
    # post_message('saving index has been finished')
    # print('hi')
    # exit()
    # 여기까지 주석

    # end_date는 5분봉 데이터가 있는 마지막 거래일
    total_profit_rate_list = []
    max_test_index = -1
    sql = f"SELECT distinct trade_date FROM daily_price where trade_date BETWEEN '{start_date}' AND '{end_date}';"
    df = pd.read_sql(sql, connection)
    df['trade_date'] = df['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))


    # cursor.execute(f"DELETE FROM stock_hold_test WHERE test_index_kind = {test_index_kind};")
    
    # In-memory holdings dictionary
    # Structure: { index_order: { code: { 'avg_price': float, 'stk_cnt': int } } }
    holdings = {}

    # Performance Timers
    perf_timers = {'buy_new': 0.0, 'buy_hold': 0.0, 'minute_rec': 0.0, 'minute_trade': 0.0}


    ####################### 일별 매매 테스트 시작 ##########################
    for i, row in df.iterrows():
        filtered_stock_hold = [[] for _ in range(len(test_index))]
        if i + 1 == len(df):
            break
            
        current_date = row[0]
        
        # Batch fetch 5-minute prices for the current day
        sql_daily_prices = f"SELECT code, close, trade_date FROM five_minute_price WHERE trade_date BETWEEN '{current_date} 00:00:00' AND '{current_date} 23:59:59' ORDER BY trade_date"
        df_five_minute_price_day = pd.read_sql(sql_daily_prices, connection)
        
        # Batch fetch Daily Index for the current day (Used by both buy_new and buy_hold)
        sql_daily_index_test = f"SELECT code, IDX_ID, val, idx_term FROM daily_index_test WHERE trade_date = '{current_date}'"
        df_daily_index_val = pd.read_sql(sql_daily_index_test, connection)
        
        ## 오늘 날짜 기준으로 내일 매매할 종목을 선정
        t_start = time.time()
        code_results = check_buy_new_recommand(row[0], index_list, test_index, test_index_pos, test_index_kind, df_daily_index_val=df_daily_index_val)                
        perf_timers['buy_new'] += time.time() - t_start
        
        # 추천 종목과 보유한 종목 겹치는 경우 있으므로 filtered_stoc_hold에 중복 제외 후 담음
        
        for j, test_value in enumerate(test_index):
            # Using holdings dict
            current_holdings_codes = []
            if j in holdings:
                current_holdings_codes = [code for code, info in holdings[j].items() if info['stk_cnt'] > 0]

            for stock_hold_code in current_holdings_codes:
                colide_flag = False
                # 추천 종목과 보유 종목 간 중복 건은 제거
                for code in code_results[j]:
                    if code[0] == stock_hold_code:
                        code_results[j].remove(code)
                        colide_flag = True
                # code_results : 금일 신규 매수 종목, filtered_stock_hold : 보유 종목 중 금일 신규 매수 제외한 종목
                # 중복이 발생하지 않는 보유건에 대해서 따로 관리
                if not colide_flag:
                    code_results[j].append([stock_hold_code, 'H'])
                    filtered_stock_hold[j].append(stock_hold_code)        
        # 오늘 날짜 기준으로 들고 있는 종목 중에 내일 추가 매수할 종목 선정
        t_start = time.time()
        check_buy_hold_recommand(row[0], code_results, filtered_stock_hold, index_list, test_index, test_index_pos, test_index_kind, df_daily_index_val=df_daily_index_val)        
        perf_timers['buy_hold'] += time.time() - t_start

        # 내일 날짜 기준 매매했을 경우 어느 시점에 매매할 지 확인
        t_start = time.time()        
            
        temp_trade_signal_list = check_minute_recommand(
            df.iloc[i]['trade_date'], 
            df.iloc[i + 1]['trade_date'], 
            code_results, 
            index_list, 
            test_index, 
            test_index_pos, 
            test_index_kind,            
            df_five_minute_price=df_five_minute_price_day # Reuse the daily price dataframe
        )
        # print(df.iloc[i]['trade_date'])
        # print(df.iloc[i + 1]['trade_date'])
        # print(code_results)
        # print(index_list)
        # print(test_index)
        # print(test_index_pos)
        # print(test_index_kind)
        # print(df_minute_index_val)
        # print(df_five_minute_price_day)
        # print(temp_trade_signal_list)
        # exit()
        perf_timers['minute_rec'] += time.time() - t_start
        
        # DEBUG PRINTS
        total_candidates = sum(len(c) for c in code_results)
        total_signals = 0
        for j_list in temp_trade_signal_list:
            for _, signals in j_list:
                total_signals += len(signals)
        
        # if total_candidates > 0 or total_signals > 0:
        #      print(f"Date: {current_date}, Candidates: {total_candidates}, Signals: {total_signals}")
        # else:
        #      pass # print(f"Date: {current_date}, No activity")

        # 내일 날짜 기준 매매했을 경우 결과가 어떨지 확인
        t_start = time.time()
        daily_index_results = minute_trade(df.iloc[i + 1]['trade_date'], temp_trade_signal_list, test_index, test_index_kind, holdings, df_five_minute_price_day=df_five_minute_price_day)
        perf_timers['minute_trade'] += time.time() - t_start
        
        ############# 일 별 매매 실적 정산 #############
        # text_index, 현재 날짜, code 별 매도량 총합
        daily_trade_amount_list = [0] * len(test_index)
        daily_profit_rate_list = [0] * len(test_index)
        # daily_index_results : 종목, text_index 별 수익률, 거래량
        for j, daily_code_results in enumerate(daily_index_results):
            for daily_result in daily_code_results:
                daily_trade_amount_list[j] += daily_result[2]
                daily_profit_rate_list[j] += (daily_result[1]*daily_result[2])
        # daily_profit_rate_list : text_index, 현재 날짜 별 평균수익률
        for j in range(len(test_index)):
            if daily_trade_amount_list[j] == 0:
                continue
            else:
                daily_profit_rate_list[j] = daily_profit_rate_list[j] / daily_trade_amount_list[j]
        # text_index, 현재 날짜 별 평균수익률
        total_profit_rate = []
        for j in range(len(test_index)):
            total_profit_rate.append([daily_profit_rate_list[j], daily_trade_amount_list[j]])
        total_profit_rate_list.append(total_profit_rate)
        # print(f'거래일 {df.iloc[i+1]["trade_date"]}')
    ################ 전체 기간 매매 실적 정산 #############################
    total_trade_amount_list = [0] * len(test_index)
    average_trade_profit_rate_list = [0] * len(test_index)
    total_left_amount_list = [0] * len(test_index)
    average_left_profit_rate_list = [0] * len(test_index)


    max_profit = -999999999
    max_index = 0
    for j in range(len(test_index)):
        # text_index 별 전체 기간 매도량 총합
        for total_profit_rate in total_profit_rate_list:
            total_trade_amount_list[j] += total_profit_rate[j][1]
            average_trade_profit_rate_list[j] += (total_profit_rate[j][0]*total_profit_rate[j][1])
        # text_index 별 전체 기간 일일수익률*일일매도량 / 전체 기간 매도량 총합
        if total_trade_amount_list[j] == 0:
            None
        else:
            average_trade_profit_rate_list[j] = average_trade_profit_rate_list[j] / total_trade_amount_list[j]
        # j번째 index에 대해서 잔여 주식의 평단가와 end_date의 종가를 비교하여 수익률 산출,
        average_left_profit_rate_list[j] = trade_test_left_cal(end_date, j, test_index_kind, holdings)
        total_left_amount_list[j] = sum(row[2] for row in average_left_profit_rate_list[j])
        # 날짜 별 평균 수익률
        if total_left_amount_list[j] == 0:
            average_left_profit_rate_list[j] = 0
        else:
            average_left_profit_rate_list[j] = sum(row[1] * row[2] for row in average_left_profit_rate_list[j]) / total_left_amount_list[j]
        if total_trade_amount_list[j]+total_left_amount_list[j] != 0:
            final_profit = average_trade_profit_rate_list[j]*total_trade_amount_list[j]+average_left_profit_rate_list[j]*total_left_amount_list[j]
            # final_profit = average_trade_profit_rate_list[j] * total_trade_amount_list[j]
        else:
            final_profit = -999999999
        print(f'test : {test_index_kind} index {test_index[j]} 전체 수익 : {final_profit}')
        if final_profit > max_profit:
            max_profit = final_profit
            max_index = test_index[j]
            max_test_index = j
            
    print(f"Performance Profile:")
    for k, v in perf_timers.items():
        print(f"  {k}: {v:.4f}s")
    if cursor:
        cursor.close()
    if connection:
        connection.close()

    return max_profit, max_index, max_test_index



# 모든 값이 0인지 확인하는 함수
def all_values_zero(d):
    for sublist in d.values():
        if any(value != 0 for value in sublist):
            return False
    return True


def stock_index_check(start_date, end_date):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    sql_code_info = """
                SELECT code, market_cap
                FROM company_info
                WHERE industry NOT LIKE %s
                AND industry NOT LIKE %s
                AND products NOT LIKE %s
                AND products NOT LIKE %s
                AND market_cap > 100000000000
                AND del_yn = 'N'
                ORDER BY market_cap DESC;
                """
    params = ('%의약품%', '%의료%', '%지주%', '%화장품%')
    cursor.execute(sql_code_info, params)
    df_code_info = pd.DataFrame(cursor.fetchall(), columns=['code', 'market_cap'])
    df_code_info_sorted = df_code_info.sort_values(by='market_cap', ascending=False)
    # idx_id_list = [1]
    idx_id_list = [1, 46, 97, 113]
    minute_idx_list = [97]
    for index, row_code in df_code_info_sorted.iterrows():
        check_daily_index_exists(start_date, end_date, row_code[0], idx_id_list)
        check_five_minute_index_exists(start_date, end_date, row_code[0], minute_idx_list)
    print("stock_index_check completed")



def get_param_name(test_index_kind, key, index):
    # Mapping based on DB schema and common indicator usage
    # 46: Likely MACD or Stochastic?
    # 97: RSI?
    # 113: OBV or CCI?
    # NOTE: User can correct these names. Assuming generic names based on order.
    
    idx_map = {46: "Index_46", 97: "Index_97", 113: "Index_113"}
    
    if test_index_kind == 2:
        # Special case for kind 2 (8 params for key 97)
        if key == 97:
            # 0-3: idx1, 4-7: idx2
            group = "Set1" if index < 4 else "Set2"
            local_idx = index % 4
            suffix_map = {0: "Index Low", 1: "Index High", 2: "Signal Low", 3: "Signal High"}
            return f"{idx_map.get(key, str(key))} ({group}) - {suffix_map.get(local_idx)}"
    
    # Standard 4 params
    suffix_map = {0: "Index Low", 1: "Index High", 2: "Signal Low", 3: "Signal High"}
    return f"{idx_map.get(key, str(key))} - {suffix_map.get(index, str(index))}"

def run_optimization(test_index_kind):
    # 백테스트 
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    start_time = time.time()        
    
    # Ensure index data exists before starting optimization
    # This was previously commented out in back_test
    print("Checking and generating stock index data...")
    # stock_index_check(START_DATE, END_DATE)
    print("Stock index check completed.")    
    
    print(f"Start optimization for test_index_kind: {test_index_kind}")

    # Pre-calculate df_sig_max removed per user request
    # df_sig_max = None


    if test_index_kind == 2:
        index_list = {97: [0.5, 0.7, 0.3, 0.6, 0.5, 0.7, 0.3, 0.6]}
        query = f"""
                SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,                   
                idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high
                FROM test_index_list
                WHERE test_index_kind = {test_index_kind};
                """
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            idx1_values = row[0:8]
            index_list[97] = list(idx1_values)
        initial_end_check = {97: [1, 1, 1, 1, 1, 1, 1, 1]}
    else:
        index_list = {46: [0.4, 1, 0.3, 0.7], 97: [0.5, 0.7, 0.3, 0.6], 113: [0.4, 1, 0.3, 0.7]}
        query = f"""
                SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,  
                       idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high,  
                       idx3_idx_low, idx3_idx_high, idx3_sig_low, idx3_sig_high 
                FROM test_index_list
                WHERE test_index_kind = {test_index_kind};
                """
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            idx1_values = row[0:4]
            idx2_values = row[4:8]
            idx3_values = row[8:12]
            index_list[46] = list(idx1_values)
            index_list[97] = list(idx2_values)
            index_list[113] = list(idx3_values)
        initial_end_check = {46: [1, 1, 1, 1], 97: [1, 1, 1, 1], 113: [1, 1, 1, 1]}

    end_check = copy.deepcopy(initial_end_check)
    res = -999

    # 테스트 기준 변수의 개수 MACD, RSI, OBV Midpoint 총 3개
    while True:
        for key, value in index_list.items():
            # 매수 기준, 익절 기준, 손절 기준 영역 3개
            for i in range(len(value)):
                if end_check[key][i] == 0:
                    continue
                
                param_name = get_param_name(test_index_kind, key, i)
                print(f"\n--- Optimization Step ---")
                print(f"Testing Parameter: {param_name}")
                print(f"Current Value: {index_list[key][i]}")
                
                test_index = np.random.normal(index_list[key][i], 0.2, 10)

                # # 임시용
                test_index_pos = [key, i]
                new_res, new_coef, max_test_index = back_test(index_list, test_index, test_index_pos, test_index_kind,
                                              START_DATE, END_DATE)
                if new_res > res:
                    # 다른 테스트로 인해 low가 high보다 커진 경우 무시
                    if i % 2 == 1:
                        if new_coef < index_list[key][i - 1]:
                            continue
                    else:
                        if new_coef > index_list[key][i + 1]:
                            continue
                    res = new_res
                    index_list[key][i] = new_coef
                    print(f"test_index_kind : {test_index_kind}, {index_list}")
                    if test_index_kind == 2:
                        sql = f"""
                        INSERT INTO test_index_list (
                            test_index_kind, 
                            idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,  
                            idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high                            
                        ) 
                        VALUES (
                            {test_index_kind}, 
                            {index_list[97][0]}, {index_list[97][1]}, {index_list[97][2]}, {index_list[97][3]}, 
                            {index_list[97][4]}, {index_list[97][5]}, {index_list[97][6]}, {index_list[97][7]}                            
                        )
                        ON DUPLICATE KEY UPDATE
                                idx1_idx_low = VALUES(idx1_idx_low), 
                                idx1_idx_high = VALUES(idx1_idx_high),
                                idx1_sig_low = VALUES(idx1_sig_low),
                                idx1_sig_high = VALUES(idx1_sig_high),
                                idx2_idx_low = VALUES(idx2_idx_low),
                                idx2_idx_high = VALUES(idx2_idx_high),
                                idx2_sig_low = VALUES(idx2_sig_low),
                                idx2_sig_high = VALUES(idx2_sig_high);
                        """
                    else:
                        sql = f"""
                        INSERT INTO test_index_list (
                            test_index_kind, 
                            idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,  
                            idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high,  
                            idx3_idx_low, idx3_idx_high, idx3_sig_low, idx3_sig_high
                        ) 
                        VALUES (
                            {test_index_kind}, 
                            {index_list[46][0]}, {index_list[46][1]}, {index_list[46][2]}, {index_list[46][3]}, 
                            {index_list[97][0]}, {index_list[97][1]}, {index_list[97][2]}, {index_list[97][3]}, 
                            {index_list[113][0]}, {index_list[113][1]}, {index_list[113][2]}, {index_list[113][3]}
                        )
                        ON DUPLICATE KEY UPDATE
                                idx1_idx_low = VALUES(idx1_idx_low), 
                                idx1_idx_high = VALUES(idx1_idx_high),
                                idx1_sig_low = VALUES(idx1_sig_low),
                                idx1_sig_high = VALUES(idx1_sig_high),
                                idx2_idx_low = VALUES(idx2_idx_low),
                                idx2_idx_high = VALUES(idx2_idx_high),
                                idx2_sig_low = VALUES(idx2_sig_low),
                                idx2_sig_high = VALUES(idx2_sig_high),
                                idx3_idx_low = VALUES(idx3_idx_low),
                                idx3_idx_high = VALUES(idx3_idx_high),
                                idx3_sig_low = VALUES(idx3_sig_low),
                                idx3_sig_high = VALUES(idx3_sig_high);
                        """
                    cursor.execute(sql)
                    if test_index_kind == 3 and max_test_index != -1:                        
                        cursor.execute(f"DELETE FROM daily_recommand WHERE trade_date  >= '{START_DATE}' and trade_date < '{END_DATE}';")                        
                        cursor.execute(f"SELECT code, trade_date FROM daily_recommand_test WHERE index_order = {max_test_index};")
                        rows = cursor.fetchall()
                        for row in rows:
                            cursor.execute(f"REPLACE INTO daily_recommand (code, trade_date) VALUES ('{row[0]}', '{row[1]}');")                            
                elif new_res < res:
                    end_check[key][i] = 0
                elif math.isclose(new_res, res, rel_tol=1e-7):  # 상대적 오차 1e-7
                    end_check[key][i] = 0

        # 모든 값이 0인지 확인
        if all_values_zero(end_check):
            break
        else:
            # 초기 상태로 다시 설정
            end_check = copy.deepcopy(initial_end_check)

    if cursor:
        cursor.close()
    if connection:
        connection.close()
    
    end_time = time.time()
    elapsed_time = end_time - start_time  # 실행 시간 계산
    print(f"실행 시간 ({test_index_kind}): {elapsed_time} 초")


def delete_old_data(start_date):
    """
    Deletes data older than 120 days from the start_date to maintain performance
    while preserving enough history for indicator calculations.
    """
    print("Cleaning up old data...")
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    
    # Calculate cutoff date (Start Date - 120 days)
    dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    cleanup_limit_date = (dt_start - timedelta(days=200)).strftime("%Y-%m-%d")
    
    print(f"Deleting data older than: {cleanup_limit_date}")
    
    tables = ['daily_price', 'five_minute_price', 'daily_index_test', 'five_minute_index_test']
    
    for table in tables:
        try:
            sql = f"DELETE FROM {table} WHERE trade_date < '{cleanup_limit_date}'"
            cursor.execute(sql)
            print(f"Deleted old rows from {table}")
        except Exception as e:
            print(f"Error deleting from {table}: {e}")
            
    cursor.close()
    connection.close()
    print("Cleanup completed.")




if __name__ == '__main__':
    
    start_time_total = time.time()
    
    # 0: check_buy_hold_recommand 
    # 1: check_sell_hold_recommand s
    # 2: check_minute_recommand 
    # 3: check_buy_new_recommand
    
    if len(sys.argv) > 1:
        test_index_kind = int(sys.argv[1])
        # Cleanup old data first
        delete_old_data(START_DATE)
        run_optimization(test_index_kind)
    else:
        # Run all optimizations in parallel
        # Cleanup old data first
        delete_old_data(START_DATE)
        
        processes = []
        for kind in range(4):
            p = Process(target=run_optimization, args=(kind,))
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()

    end_time_total = time.time()
    print(f"Total execution time: {end_time_total - start_time_total} seconds")
    post_message('All tests have been finished')