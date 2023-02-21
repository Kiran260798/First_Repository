from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#read csv file
csv_df=spark.read.csv("/FileStore/tables/hospital_quarterly_financial_utilization_report_sum_of_four_quarters_9.csv",header=True,inferSchema=True)
csv_df.display()

# select columns from csv_df
df=csv_df.select('NET_TOT','OTH_OP_REV','TOT_OP_EXP','NONOP_REV')
df.printSchema()
df.display()

# removes comma from file with the help of regex_replace
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType
df1=df.withColumn("TOT_OP_EXP",regexp_replace(df["TOT_OP_EXP"],",",""))
# df1.display()
df2=df1.withColumn("NET_TOT",regexp_replace(df1["NET_TOT"],",",""))
# df2.display()
df3=df2.withColumn("OTH_OP_REV",regexp_replace(df2["OTH_OP_REV"],",",""))
# df3.display()
df4=df3.withColumn("NONOP_REV",regexp_replace(df3["NONOP_REV"],",",""))
df4.display()

# Pre-tax Net Income (Loss)= Net Patient Revenue Total (Line No. 800) + Other Operating Revenue (Line No. 810) - Total Operating Expenses (Line No. 830)                              + Net Nonoperating Revenue and ExpensesLine No.840)
sum_df=select_df.withColumn("Pre_tax_Net_Income_(Loss)",col("NET_TOT")+col("OTH_OP_REV")+col("NONOP_REV")-col("TOT_OP_EXP"))
# sum_df.display()

#"Total Operating Revenue" equals Net Patient Revenue Total+ Other Operating Revenue
total_revenue=sum_df.withColumn("Total_Operating_Revenue",col("NET_TOT")+col("OTH_OP_REV"))
total_revenue.display()

#Total Margin =(“Pre-tax Net Income” ÷ “Total Operating Revenue”)x 100
total_margin_df=total_revenue.withColumn("Total Margin",col("Pre_tax_Net_Income_(Loss)")/col("Total_Operating_Revenue")*100)
total_margin_df.display()