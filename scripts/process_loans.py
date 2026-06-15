from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round
import sys

def main():
    input_path = sys.argv[1]   
    output_path = sys.argv[2]  

    spark = SparkSession.builder \
        .appName("Credit Risk ETL") \
        .enableHiveSupport() \
        .getOrCreate()

    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .csv(input_path)

    
    df_transformed = df \
        .withColumn("risk_category",
                    when(col("int_rate%") > 15, "high_risk")
                    .when(col("int_rate%") > 10, "medium_risk")
                    .otherwise("low_risk")) \
        .withColumn("recovery_rate", 
                    when(col("loan_amnt_div_instlmnt") > 0, 
                         round(col("recoveries") / col("loan_amnt_div_instlmnt"), 4))
                    .otherwise(0)) \
        .withColumn("is_defaulted", col("loan_status_binary") == 1) \
        .withColumn("high_debt_flag", col("out_prncp") > 10000)

    # Сохраняем в Parquet
    df_transformed.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Результат сохранён в {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()