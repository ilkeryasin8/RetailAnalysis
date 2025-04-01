from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, count, mean, round, to_date, year, month, row_number, lit, substring
)
import matplotlib.pyplot as plt

# 1. Veri okuma
def read_data(spark, input_file):
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    return df

# 2. Date sütununu datetime formatına çevir ve Year, Month sütunlarını doldur
def process_date_columns(df):
    # Time sütunundan Date bilgisini al
    df = df.withColumn("Date", to_date(substring(col("Time"), 1, 10)))
    
    # Year ve Month değerlerini Date'den doldur
    df = df.withColumn("Year", when(col("Year").isNull(), year(col("Date"))).otherwise(col("Year")))
    df = df.withColumn("Month", when(col("Month").isNull(), month(col("Date"))).otherwise(col("Month")))
    
    return df

# 3. Hem Month hem Date sütunları boş olanları sil
def drop_missing_date_month(df):
    return df.dropna(subset=["Month", "Date"])

# 4. Total_Purchases hesaplaması: Boşsa, Total_Amount/Amount yuvarlanarak hesapla
def calculate_total_purchases(df):
    df = df.withColumn("Total_Purchases",
                       when(col("Total_Purchases").isNull(), round(col("Total_Amount")/col("Amount"), 0))
                       .otherwise(col("Total_Purchases")))
    return df

# 5. Eski Transaction_ID'yi yeniden adlandır
def rename_transaction_id(df):
    if "Transaction_ID" in df.columns:
        df = df.withColumnRenamed("Transaction_ID", "Old_Transaction_ID")
    return df

# 6. Yeni Transaction_ID oluştur (Pandas'taki range(1, len(df)+1) karşılığı)
def create_transaction_id(df):
    window_all = Window.orderBy(lit(1))
    df = df.withColumn("Transaction_ID", row_number().over(window_all))
    return df

# 7. Customer_ID ve Age boş olan kayıtları sil
def drop_customer_age_na(df):
    return df.dropna(subset=["Customer_ID", "Age"])

# 8. Belirtilen sütunları at (sil)
def drop_specified_columns(df):
    cols_to_drop = ['Email', 'Name', 'Zipcode', 'Address', 'Phone', 'City', 'State',
                    'Old_Transaction_ID', 'Order_Status', 'Time', 'Date', 'Amount', 'Total_Amount']
    for c in cols_to_drop:
        if c in df.columns:
            df = df.drop(c)
    return df

# 9. Belirtilen sütunlardaki boş değerleri doldur
def fill_na_columns(df):
    return df.fillna({
        'Product_Category': 'Other',
        'Product_Brand': 'Other',
        'Feedback': 'Average',
        'Shipping_Method': 'Standard',
        'Payment_Method': 'Credit Card',
        'Ratings': 3
    })

# 10. Grup bazlı işlemler: Mode hesaplaması ile Country, Gender ve Customer_Segment sütunlarını işle
def mode_default_column(df, target_col, default_val):
    mode_df = df.groupBy("Customer_ID", target_col).agg(count(target_col).alias("cnt"))
    window_spec = Window.partitionBy("Customer_ID").orderBy(col("cnt").desc())
    mode_df = mode_df.withColumn("rn", row_number().over(window_spec)) \
                     .filter(col("rn") == 1) \
                     .drop("rn")
    mode_df = mode_df.withColumnRenamed(target_col, target_col + "_mode")
    df = df.join(mode_df, on="Customer_ID", how="left")
    df = df.withColumn(target_col, when(col("cnt") > 1, col(target_col + "_mode")).otherwise(lit(default_val)))
    df = df.drop("cnt", target_col + "_mode")
    return df

def process_group_columns(df):
    df = mode_default_column(df, "Country", "USA")
    df = mode_default_column(df, "Gender", "Female")
    df = mode_default_column(df, "Customer_Segment", "Regular")
    return df

# 11. Age sütunu: Müşteri bazında mode varsa mode, yoksa ortalamayı al ve yuvarla
def process_age(df):
    age_mode_df = df.groupBy("Customer_ID", "Age").agg(count("Age").alias("cnt"))
    window_age = Window.partitionBy("Customer_ID").orderBy(col("cnt").desc())
    age_mode_df = age_mode_df.withColumn("rn", row_number().over(window_age)) \
                             .filter(col("rn") == 1) \
                             .drop("rn")
    age_mode_df = age_mode_df.withColumnRenamed("Age", "mode_age")
    
    age_avg_df = df.groupBy("Customer_ID").agg(mean("Age").alias("avg_age"))
    
    df = df.join(age_mode_df, on="Customer_ID", how="left")
    df = df.join(age_avg_df, on="Customer_ID", how="left")
    df = df.withColumn("Age", when(col("cnt") > 1, col("mode_age")).otherwise(col("avg_age")))
    df = df.withColumn("Age", round(col("Age"), 0).cast("integer"))
    df = df.drop("cnt", "mode_age", "avg_age")
    return df

# 12. Income sütunu: Boş değerleri 'Medium' yap, Low/Medium/High'ı sayısallaştırıp ortalama al
def process_income(df):
    df = df.withColumn("Income", when(col("Income").isNull(), lit("Medium")).otherwise(col("Income")))
    df = df.withColumn("Income",
                       when(col("Income") == "Low", lit(1))
                      .when(col("Income") == "Medium", lit(2))
                      .when(col("Income") == "High", lit(3))
                      .otherwise(col("Income"))
                      )
    income_avg_df = df.groupBy("Customer_ID").agg(mean("Income").alias("avg_income"))
    df = df.join(income_avg_df, on="Customer_ID", how="left")
    df = df.withColumn("Income", round(col("avg_income"), 1))
    df = df.drop("avg_income")
    return df

# 13. Customer_ID'ye göre sıralama
def order_by_customer(df):
    return df.orderBy("Customer_ID")

# 14. Null değer analizi ve grafik oluşturma
def null_value_analysis(df):
    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    null_counts = df.agg(*null_exprs)
    null_counts_pd = null_counts.toPandas()
    
    plt.figure(figsize=(12, 6))
    null_counts_pd.plot(kind='bar')
    plt.title('Boş Değer Sayıları')
    plt.xlabel('Sütunlar')
    plt.ylabel('Boş Değer Sayısı')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('null_values_analysis.png')
    
    print("\nBoş Değer Sayıları:")
    print(null_counts_pd)

# 15. Veriyi yazdırma (tek dosya halinde)
def write_data(df, output_file):
    # Önce pandas DataFrame'e çevir
    pandas_df = df.toPandas()
    
    # Tek bir CSV dosyası olarak kaydet
    pandas_df.to_csv(output_file, index=False)
    print(f"\nData processing completed. Output saved to {output_file}")

# Tüm işlemleri sırasıyla yürüten ana fonksiyon
def process_data(input_file, output_file):
    # Spark session'ı daha iyi yapılandırma ile oluştur
    spark = SparkSession.builder \
        .appName("RetailDataProcessingWithFunctions") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()
    
    # Log seviyesini ayarla
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        df = read_data(spark, input_file)
        print(f"Initial row count: {df.count()}")
        
        df = process_date_columns(df)
        print(f"Row count after date processing: {df.count()}")
        
        df = drop_missing_date_month(df)
        print(f"Row count after dropping missing dates: {df.count()}")
        
        df = calculate_total_purchases(df)
        print(f"Row count after total purchases calculation: {df.count()}")
        
        df = rename_transaction_id(df)
        df = create_transaction_id(df)
        
        df = drop_customer_age_na(df)
        print(f"Row count after dropping customer/age NA: {df.count()}")
        
        df = drop_specified_columns(df)
        df = fill_na_columns(df)
        df = process_group_columns(df)
        df = process_age(df)
        df = process_income(df)
        df = order_by_customer(df)
        
        print(f"Final row count: {df.count()}")
        
        null_value_analysis(df)
        write_data(df, output_file)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    input_file = "Retail_Analysis.csv"
    output_file = "new_cleaned_data_spark.csv"
    process_data(input_file, output_file)
