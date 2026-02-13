import multiprocessing
from .balance import get_balance_total, get_balance_stock
from .check_recommand import check_recommand
from .trade import five_minute_action
from .daily_price_update import etf_price_update
from datetime import datetime, timedelta

def post_action(request):
    queue = multiprocessing.Queue()
    if request.data[1] == 'balance':
        if request.data[2] == 'total':
            process = multiprocessing.Process(target=get_balance_total, args=(queue,))
            process.start()
            process.join()
            results = []
            while True:
                try:
                    data = queue.get(timeout=1)  # 큐가 비어있을 때까지 데이터를 가져옵니다.
                    results.append(data)
                except:
                    break
            return results
        elif request.data[2] == 'check':
            process = multiprocessing.Process(target=get_balance_stock, args=(queue,))
            process.start()
            process.join()
            results = []
            while True:
                try:
                    data = queue.get(timeout=1)  # 큐가 비어있을 때까지 데이터를 가져옵니다.
                    results.append(data)
                except:
                    break
            return results

    elif request.data[1] == 'trade':
        if request.data[2] == 'on':
            process = multiprocessing.Process(target=five_minute_action, args=(queue,))
            process.start()
            process.join()
            results = []
            while True:
                try:
                    data = queue.get(timeout=1)  # 큐가 비어있을 때까지 데이터를 가져옵니다.
                    results.append(data)
                except:
                    break
            return results

    elif request.data[1] == 'recommend':
        try:
            # request.data가 인덱스를 지원하는 객체라고 가정
            recommand_date = request.data[2]
            if recommand_date is None:
                raise ValueError("recommand_date is None")
        except (IndexError, ValueError) as e:
            # 예외가 발생하면 현재 날짜로 설정
            now = datetime.now()
            if now.weekday() == 5:  # 토요일
                # 하루 전으로 설정 (금요일)
                now -= timedelta(days=1)
            elif now.weekday() == 6:  # 일요일
                # 이틀 전으로 설정 (금요일)
                now -= timedelta(days=2)
            elif now.weekday() == 0 and now.hour <= 15: # 월요일
                # 3일 전으로 설정 (금요일)
                now -= timedelta(days=3)
            else:
                if now.hour > 15:
                    None
                elif now.hour <= 15:
                    now -= timedelta(days=1)
            recommand_date = now.strftime("%Y-%m-%d")
        process = multiprocessing.Process(target=check_recommand, args=(recommand_date, queue, ))
        process.start()
        process.join()
        results = []
        while True:
            try:
                data = queue.get(timeout=1)  # 큐가 비어있을 때까지 데이터를 가져옵니다.
                results.append(data)
            except:
                break
        return results


def get_action(request):
    process = multiprocessing.Process(target=etf_price_update)
    process.start()
    process.join()





