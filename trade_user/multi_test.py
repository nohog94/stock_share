from concurrent.futures import ThreadPoolExecutor
import subprocess
import warnings
from sqlalchemy import exc
import pandas as pd

def run_script(index):
    # SQLAlchemy와 pandas 경고 무시
    warnings.filterwarnings('ignore', category=exc.SAWarning)
    warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
    # Python 실행 시 경고 무시 옵션 추가
    subprocess.run(["python", "-W", "ignore", "-m", "stock_trade.trade_user.back_test", str(index)])

if __name__ == "__main__":
    indices = [3]
    # indices = [0, 2]
    # indices = [0, 2, 3]
    with ThreadPoolExecutor(len(indices)) as executor:
        executor.map(run_script, indices)