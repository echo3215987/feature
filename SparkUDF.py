import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType, StringType, IntegerType, StringType, FloatType
from Constant import *

# 將欄位根據時間條件作轉換
transDate_UDF = udf(lambda date: None if (date is None or
                                          date < FIX_DATE or date > END_DATE_TIME) else date,
                    TimestampType())

# fillna
fillna_INT_UDF = udf(lambda value: 0 if value is None else value, IntegerType())
fillna_String_UDF = udf(lambda value: 'NA' if value is None else value, StringType())
fillna_Float_UDF = udf(lambda value: 0.0 if value is None else value, FloatType())

# 將欄位根據時間條件作轉換 dateformat1
transDatetime_UDF = udf(lambda arr: arr[0] if (isinstance(arr[0], float) or arr[0] is None) else datetime.datetime.strptime(arr[0].replace(' ', ''), arr[1]),
                    TimestampType())

# 將兩個欄位比對, 後者(arr[1])包含前者(arr[0]) 回傳 arr[0] = '', 否則回傳前者原本的值
existValueReplacement_UDF = udf(lambda arr: arr[0] if arr[0] not in arr[1] else '', StringType())

# 欄位長度大於 x 回傳該值, 否則回傳 ''
lengthReplacementOver_UDF = udf(lambda arr: arr[0] if (arr[0] is not None and len(arr[0]) > int(arr[1])) else '')

# 欄位長度大於等於 x 回傳'', 否則回傳該值
lengthReplacementUnder_UDF = udf(lambda arr: '' if (arr[0] is None or len(arr[0]) >= int(arr[1])) else arr[0])

# 該欄位不是float type也不是none 則去空白
strip_UDF = udf(lambda value: value if (isinstance(value, float) or value is None) else value.strip())
