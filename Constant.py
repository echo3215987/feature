import datetime
import os

# Set paramenter
today = datetime.datetime.now()
Path_by_day = today.strftime("%Y%m")
Candidate_Car_age = 15  # 用來挑選候選車子的車齡參數
#login_info = r'DRIVER={Simba Spark ODBC Driver}; SERVER=10.201.2.130; DATABASE=cdp;'  # 連Cassandra DBC
Log_Path = './Log/'
Log_File = Log_Path + 'Log_' + Path_by_day + '.txt'  # refactory by HD
Result_Path = './' + Path_by_day + '/'  # refactory by HD
Package_Path = os.path.dirname(os.path.realpath(__file__))
File_Path = '' + Package_Path
Import_Data_Path = File_Path + '/Import_Data/'
Temp_Path = File_Path + '/Temp_Data/'

# 自動設定 END_DATE，因原本程式 END_DATE使用string 格式，所以在這裡要做轉換
# today = datetime.datetime.now() #mark by HD
yy = str(today.year)
mm = str(today.month)
END_DATE = yy + '-' + mm + '-01'  # END_DATE 設成每月的 1 日

delay = 1  # for mutithread

y_3y = str(today.year - 3)
m_3y = str(today.month)
DATE_3y = y_3y + '-' + m_3y + '-01'  # END_DATE 設成每月的 1 日

FIX_DATE = datetime.datetime.strptime('1988-01-01', "%Y-%m-%d")
END_DATE_TIME = datetime.datetime.strptime(END_DATE, "%Y-%m-%d")
DATE_3y_TIME = datetime.datetime.strptime(DATE_3y, "%Y-%m-%d")

result_queue = []

DATETIME_FORMAT1='%Y%m%d'
DATETIME_FORMAT2='%Y/%m/%d'
DATETIME_FORMAT3='%Y-%m-%d'