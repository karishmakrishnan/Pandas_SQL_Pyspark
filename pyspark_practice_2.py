from pyspark.sql.functions import *
from pyspark.sql.window import Window
data = [
    {"txn_id":1, "customer_id":101, "txn_type":"DEBIT", "amount":3000, "timestamp":"2024-01-01 10:00:00"},
    {"txn_id":2, "customer_id":101, "txn_type":"DEBIT", "amount":2500, "timestamp":"2024-01-01 10:00:30"},
    {"txn_id":3, "customer_id":101, "txn_type":"DEBIT", "amount":1000, "timestamp":"2024-01-01 11:00:00"},
    {"txn_id":4, "customer_id":102, "txn_type":"DEBIT", "amount":6000, "timestamp":"2024-01-01 09:00:00"},
]

df = spark.createDataFrame(data)
df_txn = df.groupBy("customer_id").agg(sum("amount").alias("txn_sum"), count("timestamp").alias("total_txn"))\
            .filter(col("txn_sum") > 5000)\
            .filter(col("total_txn") >= 3)
window = Window.partitionBy("customer_id").orderBy("timestamp")

df = df.withColumn("timestamp", to_timestamp("timestamp"))
df_txn_lag = df.withColumn("txn_diff", lag("timestamp").over(window))
df_tx_dif = df_txn_lag.withColumn("diff", col("timestamp").cast("long") - col("txn_diff").cast("long"))\
    .filter(col("diff") <= 60)
df_final = df_txn.join(df_tx_dif, on= "customer_id", how="left")
customers_list = list(df_final.select("customer_id"))
my_list = [row["customer_id"] for row in df_final.select("customer_id").collect()]
print(my_list)
# df_txn_lag.show()
# df_tx_dif.show()
# df_txn.show()
# df.show()

# Cleaner approach
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Convert timestamp and extract date
df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("txn_date", to_date("timestamp"))

# 1️⃣ Aggregate per customer per day (DEBIT only)
daily_agg = (
    df.filter(col("txn_type") == "DEBIT")
      .groupBy("customer_id", "txn_date")
      .agg(
          sum("amount").alias("total_debit"),
          count("*").alias("txn_count")
      )
      .filter((col("total_debit") > 5000) & (col("txn_count") >= 3))
)

# 2️⃣ Check time difference per customer per day
window = Window.partitionBy("customer_id", "txn_date").orderBy("timestamp")

df_lag = df.withColumn("prev_ts", lag("timestamp").over(window))

df_time_flag = (
    df_lag.withColumn(
        "diff",
        col("timestamp").cast("long") - col("prev_ts").cast("long")
    )
    .filter(col("diff") <= 60)
    .select("customer_id", "txn_date")
    .distinct()
)

# 3️⃣ Final intersection
result = (
    daily_agg.join(df_time_flag,
                   ["customer_id", "txn_date"],
                   "inner")
             .select("customer_id")
             .distinct()
             .orderBy("customer_id")
)

result.show()