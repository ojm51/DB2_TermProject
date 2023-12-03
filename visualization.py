from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# SparkSession 생성
spark = SparkSession.builder.appName('VisualizeSalePrice').getOrCreate()

# 파일 경로
file_path_2022 = "C:/Users/ojm51/PyCharmFiles/pySpark/refined_2022_manhattan.xlsx"
# Pandas DataFrame으로 읽기
df_2022 = pd.read_excel(file_path_2022, engine="openpyxl")

file_path_2021 = "C:/Users/ojm51/PyCharmFiles/pySpark/refined_2022_manhattan.xlsx"
df_2021 = pd.read_excel(file_path_2021, engine="openpyxl")

file_path_2020 = "C:/Users/ojm51/PyCharmFiles/pySpark/refined_2022_manhattan.xlsx"
df_2020 = pd.read_excel(file_path_2020, engine="openpyxl")

df = pd.concat([df_2022, df_2021.iloc[1:], df_2020.iloc[1:]])

# Spark DataFrame으로 변환
spark_df = spark.createDataFrame(df)

# NEIGHBORHOOD 별 SALE PRICE의 평균 계산
avg_sale_price_df = spark_df.groupBy("NEIGHBORHOOD").avg("SALE PRICE")

# Pandas DataFrame으로 변환
avg_sale_price_pd = avg_sale_price_df.toPandas()

plt.figure(figsize=(12, 5))
sns.barplot(x="NEIGHBORHOOD", y="avg(SALE PRICE)", data=avg_sale_price_pd)
plt.xticks(ticks=avg_sale_price_pd["NEIGHBORHOOD"], rotation=45)
plt.ylim(500000, 25000000)
plt.title('Average SALE PRICE by NEIGHBORHOOD')
plt.xlabel('NEIGHBORHOOD')
plt.ylabel('Average SALE PRICE')

plt.grid()
plt.show()
