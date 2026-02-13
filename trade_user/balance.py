import win32com.client

def get_balance_total(queue):
    g_objCpTrade = win32com.client.Dispatch('CpTrade.CpTdUtil')
    initCheck = g_objCpTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        return
    acc = g_objCpTrade.AccountNumber[0]  # 계좌번호
    accFlag = g_objCpTrade.GoodsList(acc, 1)  # 주식상품 구분
    objRq = win32com.client.Dispatch("CpTrade.CpTd6033")
    objRq.SetInputValue(0, acc)  # 계좌번호
    objRq.SetInputValue(1, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
    objRq.SetInputValue(2, 50)  # 요청 건수(최대 50)
    objRq.BlockRequest()
    # 통신 및 통신 에러 처리
    rqStatus = objRq.GetDibStatus()
    rqRet = objRq.GetDibMsg1()
    print("통신상태", rqStatus, rqRet)
    if rqStatus != 0:
        return
    queue.put(objRq.GetHeaderValue(3))
    queue.put(round(objRq.GetHeaderValue(8),2))
    queue.put(objRq.GetHeaderValue(9))
    queue.put(objRq.GetHeaderValue(11))  # 현재 balance
    cnt = objRq.GetHeaderValue(7)
    stocks = []
    for i in range(cnt):
        code = objRq.GetDataValue(12, i)
        name = objRq.GetDataValue(0, i)
        stocks.append({code,name})
    queue.put(stocks)

def get_balance_stock(queue=None):
    g_objCpTrade = win32com.client.Dispatch('CpTrade.CpTdUtil')
    initCheck = g_objCpTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        return
    acc = g_objCpTrade.AccountNumber[0]  # 계좌번호
    accFlag = g_objCpTrade.GoodsList(acc, 1)  # 주식상품 구분
    objRq = win32com.client.Dispatch("CpTrade.CpTd6032")
    objRq.SetInputValue(0, acc)  # 계좌번호
    objRq.SetInputValue(1, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
    objRq.BlockRequest()
    # 통신 및 통신 에러 처리
    rqStatus = objRq.GetDibStatus()
    rqRet = objRq.GetDibMsg1()
    print("통신상태", rqStatus, rqRet)
    if rqStatus != 0:
        return
    cnt = objRq.GetHeaderValue(0)
    print('데이터 조회 개수', cnt)

    # 헤더 정보는 한번만 처리

    sumJango = objRq.GetHeaderValue(1)
    sumSellM = objRq.GetHeaderValue(2)
    sumRate = objRq.GetHeaderValue(3)
    print('잔량평가손익', sumJango, '매도실현손익', sumSellM, '수익률', sumRate)


    for i in range(cnt):
        item = {}
        item['종목코드'] = objRq.GetDataValue(12, i)  # 종목코드
        item['종목명'] = objRq.GetDataValue(0, i)  # 종목명
        item['전일잔고'] = objRq.GetDataValue(2, i)
        item['금일매수수량'] = objRq.GetDataValue(3, i)
        item['금일매도수량'] = objRq.GetDataValue(4, i)
        item['금일잔고'] = objRq.GetDataValue(5, i)
        item['평균매입단가'] = objRq.GetDataValue(6, i)
        item['평균매도단가'] = objRq.GetDataValue(7, i)
        item['현재가'] = objRq.GetDataValue(8, i)
        item['잔량평가손익'] = objRq.GetDataValue(9, i)
        item['매도실현손익'] = objRq.GetDataValue(10, i)
        item['수익률'] = objRq.GetDataValue(11, i)
        print(item)
    # queue.put(objRq.GetHeaderValue(3))
    # queue.put(round(objRq.GetHeaderValue(8),2))
    # queue.put(objRq.GetHeaderValue(9))
    # queue.put(objRq.GetHeaderValue(11))  # 현재 balance
    # cnt = objRq.GetHeaderValue(7)
    # stocks = []
    # for i in range(cnt):
    #     code = objRq.GetDataValue(12, i)
    #     name = objRq.GetDataValue(0, i)
    #     stocks.append({code,name})
    # queue.put(stocks)

def get_balance_trade(queue=None):
    g_objCpTrade = win32com.client.Dispatch('CpTrade.CpTdUtil')
    initCheck = g_objCpTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        return
    acc = g_objCpTrade.AccountNumber[0]  # 계좌번호
    accFlag = g_objCpTrade.GoodsList(acc, 1)  # 주식상품 구분
    objRq = win32com.client.Dispatch("CpTrade.CpTd5341")
    objRq.SetInputValue(0, acc)  # 계좌번호
    objRq.SetInputValue(1, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
    objRq.SetInputValue(2, "A000100")  # 상품구분 - 주식 상품 중 첫번째
    objRq.SetInputValue(5, 20)  # 상품구분 - 주식 상품 중 첫번째
    objRq.BlockRequest()
    # 통신 및 통신 에러 처리
    rqStatus = objRq.GetDibStatus()
    rqRet = objRq.GetDibMsg1()
    print("통신상태", rqStatus, rqRet)
    if rqStatus != 0:
        return
    cnt = objRq.GetHeaderValue(6)
    print('데이터 조회 개수', cnt)

    for i in range(cnt):
        item = {}
        item['종목명'] = objRq.GetDataValue(0, i)  # 종목명
        item['종목코드'] = objRq.GetDataValue(3, i)  # 종목코드
        item['주문내용'] = objRq.GetDataValue(5, i) # 주문내용
        item['주문수량'] = objRq.GetDataValue(7, i) # 주문수량
        item['주문단가'] = objRq.GetDataValue(8, i) # 주문단가
        item['체결수량'] = objRq.GetDataValue(10, i) # 체결수량
        item['체결단가'] = objRq.GetDataValue(11, i) # 체결단가

        print(item)
    # queue.put(objRq.GetHeaderValue(3))
    # queue.put(round(objRq.GetHeaderValue(8),2))
    # queue.put(objRq.GetHeaderValue(9))
    # queue.put(objRq.GetHeaderValue(11))  # 현재 balance
    # cnt = objRq.GetHeaderValue(7)
    # stocks = []
    # for i in range(cnt):
    #     code = objRq.GetDataValue(12, i)
    #     name = objRq.GetDataValue(0, i)
    #     stocks.append({code,name})
    # queue.put(stocks)

if __name__ == "__main__":
    get_balance_trade()