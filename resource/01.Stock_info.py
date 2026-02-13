import win32com.client

# 1. CpUtil.CpCodeMgr에 대한 설명 : 유가시장에서 사용되는 종목코드 및 종목코드에 따른 종목명, 현재가를 알 수 있다.
objCpCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")

# 2. GetStockListByMarket(code) 함수의 사용법  : 시장구분에 따른 주식종목배열을 반환한다
# code가 1일 경우 "코스피" 시장을 의미한다.
# code가 2일 경우 "코스닥" 시장을 의미한다.
codeList = objCpCodeMgr.GetStockListByMarket(1)
codeList2 = objCpCodeMgr.GetStockListByMarket(2)

for i, code in enumerate(codeList):
    # 3. CodeToName(code) 함수의 사용법 : code에 해당하는 종목명을 반환한다.
    name = objCpCodeMgr.CodeToName(code)

    # 4. GetStockStdPrice(code) 함수의 사용법 : code에 해당하는 현재가를 반환한다.
    stdPrice = objCpCodeMgr.GetStockStdPrice(code)

    # 결과 : 0 A000020 9540 동화약품
    print(i, code, stdPrice, name)

