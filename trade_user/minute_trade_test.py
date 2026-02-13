import pymysql
import pandas as pd





def minute_trade(test_date, trade_signal_list, test_index, test_index_kind, holdings, df_five_minute_price_day=None):
    # trade_signal_list이 변했음 [code, trade_action, minute_index] 에서 [code, minute_index]로 변했음, minute_index는 [1, 2, 6 ... 77] 의 형태에서 [[1, 'B'], [2, 'B'], [6, 'B'] ... [77, 'B']] 의 형태로 변했음
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    daily_index_results = [[] for _ in range(len(test_index))]
    
    # Pre-filter or index df_five_minute_price_day if provided
    # Assuming df_five_minute_price_day contains [trade_date, code, close] for the specific test_date
    # The original query fetched 77 rows LIMIT.
    
    # test_index에 대한 반복
    for j, test_value in enumerate(test_index):
        daily_code_results = []
        # trade_signal_list에 대한 반복 (code에 대한 반복)
        for trade_signal in trade_signal_list[j]:
            trade_result = []
            daily_code = trade_signal[0]            
            
            if df_five_minute_price_day is not None:
                 # Use filtered data
                 # We expect df_five_minute_price_day to be sorted by time or we sort it?
                 # In back_test, we read it sorted by minute.
                 df = df_five_minute_price_day[df_five_minute_price_day['code'] == daily_code].copy()
                 # Ensure sorted?
                 # Assuming data is loaded correctly sorted.
            else:
                 # Fallback to DB
                sql = f"WITH previous_dates AS (" \
                    f"SELECT DISTINCT trade_date " \
                    f"FROM five_minute_price " \
                    f"WHERE trade_date <= '{test_date} 23:59:59'" \
                    f"AND code = '{daily_code}'" \
                    f"ORDER BY trade_date DESC " \
                    f"LIMIT 77" \
                    f")" \
                    f"SELECT close " \
                    f"FROM five_minute_price " \
                    f"WHERE CODE = '{daily_code}' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"
                df = pd.read_sql(sql, connection)

            buy_tax_rate = 0.0088 / 100
            sell_tax_rate = (0.0088 + 0.18) / 100

            # holdings 초기화 확인
            if j not in holdings:
                holdings[j] = {}

            for minute_signal in trade_signal[1]:
                minute_index, trade_action = minute_signal
                # 각 매수 및 매도 신호를 순차적으로 처리
                if trade_action == 'B':
                    if minute_index < len(df):
                        buy_price = df.iloc[minute_index]['close'] * (1 - buy_tax_rate)
                        
                        if daily_code not in holdings[j] or holdings[j][daily_code]['stk_cnt'] == 0:
                            holdings[j][daily_code] = {'avg_price': buy_price, 'stk_cnt': 1}
                        else:
                            current_avg = holdings[j][daily_code]['avg_price']
                            current_cnt = holdings[j][daily_code]['stk_cnt']
                            new_avg = (current_avg * current_cnt + buy_price) / (current_cnt + 1)
                            holdings[j][daily_code]['avg_price'] = new_avg
                            holdings[j][daily_code]['stk_cnt'] += 1

                elif trade_action == 'S':
                    if daily_code in holdings[j] and holdings[j][daily_code]['stk_cnt'] > 0:
                        if minute_index < len(df):
                            sell_price = df.iloc[minute_index]['close'] * (1 - sell_tax_rate)
                            profit_rate = (sell_price / holdings[j][daily_code]['avg_price']) - 1
                            holdings[j][daily_code]['stk_cnt'] -= 1
                            
                            # if profit_rate < 5:
                            #     profit_rate = 0
                            trade_result.append([profit_rate, 1])
            # [code, 수익, 거래량]
            if len(trade_result) == 0:
                daily_result = [daily_code, 0, 0]
            else:
                total_profit_by_index = sum(result[0] for result in trade_result)
                total_num_by_index = sum(result[1] for result in trade_result)
                daily_result = [daily_code, total_profit_by_index, total_num_by_index]
            daily_code_results.append(daily_result)
        daily_index_results[j] = daily_code_results
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    return daily_index_results


def trade_test_left_cal(end_date, j, test_index_kind, holdings):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    sell_tax_rate = (0.0088 + 0.18) / 100
    
    result = []
    if j in holdings:
        for code, info in holdings[j].items():
            if info['stk_cnt'] > 0:
                sql = f"SELECT close FROM daily_price WHERE code = '{code}' AND trade_date = '{end_date}'"
                cursor.execute(sql)
                close_price_row = cursor.fetchone()
                if close_price_row:
                    close_price = close_price_row[0]
                    profit = (close_price * (1 - sell_tax_rate) - info['avg_price']) / info['avg_price']
                    result.append([code, profit, info['stk_cnt']])

    if cursor:
        cursor.close()
    if connection:
        connection.close()
    return result
