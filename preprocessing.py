from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col

# SparkSession 생성
spark = SparkSession.builder.appName('preprocessing').getOrCreate()

# 파일 경로
file_path = "/2022_manhattan.xlsx"
# Pandas DataFrame으로 읽기
df = pd.read_excel(file_path, engine="openpyxl")
# Spark DataFrame으로 변환
spark_df = spark.createDataFrame(df)

# 불필요한 열 제거
unselected = ["Borough", "Ease-ment", "Address", "Apartment Number", "Zip Code", "Land Square Feet", "Gross Square Feet"]
unselected_upper = [word.upper() for word in unselected]
selected_columns = [c for c in spark_df.columns if c.upper() not in unselected_upper]
df = spark_df.select(*selected_columns)

# "SALE DATE"를 'string' 타입으로 변환
df = df.withColumn("SALE DATE", col("SALE DATE").cast("string"))

# null 값을 0으로 치환
df = df.na.fill(value=0, subset=["RESIDENTIAL\nUNITS", "COMMERCIAL\nUNITS", "TOTAL \nUNITS"])
# null 값을 가진 행 삭제
df = df.na.drop(subset=["YEAR BUILT"])
# 0을 가진 행 삭제
df = df.filter(df["SALE PRICE"] != 0)

# Pandas DataFrame으로 변환
pdf = df.toPandas()

# "SALE DATE"를 'datetime64'로 변환 -> 오류 나서 해당 열 전체 삭제
pdf["SALE DATE"] = pd.to_datetime(pdf["SALE DATE"], format='%Y-%m-%d %H:%M:%S')

# 엑셀 파일로 저장
excel_output_path = "refined_2022_manhattan.xlsx"
pdf.to_excel(excel_writer=excel_output_path, index=False)

# SparkSession 종료
spark.stop()
