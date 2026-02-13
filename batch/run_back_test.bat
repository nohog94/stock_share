@echo off
cd /d c:\stock_trade
call venv\Scripts\activate
python trade_user\back_test.py
pause
