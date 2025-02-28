#!/usr/bin/env python
# coding: utf-8

"""
Retail Data Analysis Script - EDA
---------------------------------
Bu script, retail işlem verileri üzerinde hem Pandas hem de PySpark kullanarak 
keşifsel veri analizi (EDA) yapar. Verinin temizlenmesi, dönüştürülmesi, görselleştirilmesi 
ve analiz raporlarının hazırlanması adımlarını içerir. (DB'ye yazım için Parquet kullanılır)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from datetime import datetime
warnings.filterwarnings('ignore')

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, sum, round, month, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType

#####################################
# Pandas Tabanlı Analiz Fonksiyonları
#####################################

def load_data_pandas(filepath="Retail_Analysis.csv"):
    """CSV dosyasını Pandas DataFrame olarak yükler"""
    df = pd.read_csv(filepath)
    print("Pandas DataFrame - İlk 5 satır:")
    print(df.head())
    print("\nGenel bilgi:")
    print(df.info())
    return df

def clean_data_pandas(df):
    """Pandas kullanarak veriyi temizler ve önişlemden geçirir"""
    # Gereksiz sütunları kaldır
    df.drop(columns=['Email', 'Phone', 'Address'], inplace=True)
    
    # Eksik değer içeren satırları kaldır
    df.dropna(inplace=True)
    
    # 'Amount' sütunundaki negatif değerleri kontrol et
    num_negative = (df['Amount'] < 0).sum()
    print(f"Negatif değer sayısı: {num_negative}")
    
    # Tarih sütununu datetime formatına çevir
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # CSV'de mevcut "Month" sütununu yeniden oluşturarak doğru integer değeri atıyoruz.
    df['Month'] = df['Date'].dt.month
    
    # Yinelenen satırları kaldır
    df.drop_duplicates(inplace=True)
    
    print("\nTemizlenmiş veri bilgisi:")
    print(df.info())
    
    # Sütun bazında benzersiz değer sayıları ve özet istatistik
    distinct_counts = {col: df[col].nunique() for col in df.columns}
    for col, count_val in distinct_counts.items():
        print(f"{col}: {count_val} distinct")
    print("\nSayısal değişken özet istatistikleri:")
    print(df.describe())
    print("\nProduct_Category dağılımı:")
    print(df['Product_Category'].value_counts())
    
    return df

def visualize_data_pandas(df):
    """Pandas ile veri görselleştirmelerini yapar, görselleri kaydeder"""
    sns.set(style='whitegrid')
    
    # 1. Grafik: Satış Tutarı Dağılımı (Log Scale)
    plt.figure(figsize=(12, 8))
    sns.histplot(df['Amount'], bins=50, kde=True, color="teal", log_scale=True)
    plt.title("Satış Tutarı Dağılımı (Logaritmik Ölçek)", fontsize=16, fontweight='bold')
    plt.xlabel("Satış Tutarı", fontsize=14)
    plt.ylabel("Frekans", fontsize=14)
    mean_amount = df['Amount'].mean()
    median_amount = df['Amount'].median()
    plt.axvline(mean_amount, color='red', linestyle='--', linewidth=2, label=f'Ortalama: {mean_amount:.2f}')
    plt.axvline(median_amount, color='green', linestyle='-.', linewidth=2, label=f'Medyan: {median_amount:.2f}')
    plt.text(0.7, 0.9, 
             f"Toplam Satış: {len(df):,}\nMin: {df['Amount'].min():.2f}\nMax: {df['Amount'].max():.2f}\nStd: {df['Amount'].std():.2f}", 
             transform=plt.gca().transAxes, bbox=dict(facecolor='white', alpha=0.8))
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend()
    plt.tight_layout()
    plt.savefig("sales_distribution.png")
    plt.show()
    
    # 2. Grafik: En Çok Satılan Ürün Kategorileri (Top 10)
    plt.figure(figsize=(12, 8))
    top_categories = df['Product_Category'].value_counts().head(10)
    total_sales = len(df)
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(top_categories)))
    top_categories.plot(kind='barh', color=colors, edgecolor='black', linewidth=1)
    plt.title("En Çok Satılan Ürün Kategorileri (Top 10)", fontsize=16, fontweight='bold')
    plt.xlabel("Satış Sayısı", fontsize=14)
    plt.ylabel("Kategori", fontsize=14)
    for i, (value, category) in enumerate(zip(top_categories, top_categories.index)):
        percentage = (value / total_sales) * 100
        plt.text(value + 5, i, f"{value:,} ({percentage:.1f}%)", va='center', fontweight='bold')
    plt.grid(True, axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.2)
    plt.savefig("top_categories.png")
    plt.show()
    
    # 3. Grafik: Aylık Satış Trendleri
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 
                   7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    # "Month" sütunu yeniden oluşturuluyor
    df['Month'] = df['Date'].dt.month
    monthly_sales = df.groupby('Month')['Amount'].sum()
    monthly_transactions = df.groupby('Month').size()
    monthly_avg_sale = monthly_sales / monthly_transactions

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [2, 1]})
    fig.suptitle("Aylık Satış Trendleri ve Analizi", fontsize=18, fontweight='bold', y=0.98)
    
    # Üst grafik: Toplam Satışlar
    ax1.plot(monthly_sales.index, monthly_sales, marker='o', markersize=8, linestyle='-', 
             linewidth=2, color='#FF5733', label="Toplam Satışlar")
    ax1.plot(monthly_sales.index, monthly_sales.rolling(2).mean(), linestyle="--", 
             linewidth=2, color="#3498DB", label="Hareketli Ortalama (2 Ay)")
    max_month = monthly_sales.idxmax()
    min_month = monthly_sales.idxmin()
    ax1.plot(max_month, monthly_sales[max_month], 'go', markersize=12, label=f"En Yüksek: {month_names[max_month]}")
    ax1.plot(min_month, monthly_sales[min_month], 'ro', markersize=12, label=f"En Düşük: {month_names[min_month]}")
    ax1.set_title("Aylık Toplam Satış Tutarı", fontsize=14, pad=10)
    ax1.set_xlabel("Ay", fontsize=12)
    ax1.set_ylabel("Toplam Satış Tutarı (₺)", fontsize=12)
    ax1.set_xticks(monthly_sales.index)
    ax1.set_xticklabels([month_names[m] for m in monthly_sales.index], rotation=45)
    ax1.grid(True, alpha=0.3, linestyle='--')
    ax1.legend(loc='upper left')
    for i, val in enumerate(monthly_sales):
        ax1.annotate(f"{val:,.0f}", (monthly_sales.index[i], val), textcoords="offset points", xytext=(0,10), ha='center')
    
    # Alt grafik: İşlem Sayısı ve Ortalama Satış
    color = '#2ECC71'
    ax2.bar(monthly_transactions.index, monthly_transactions, alpha=0.7, color=color, label="İşlem Sayısı")
    ax2.set_xlabel("Ay", fontsize=12)
    ax2.set_ylabel("İşlem Sayısı", fontsize=12, color=color)
    ax2.set_xticks(monthly_transactions.index)
    ax2.set_xticklabels([month_names[m] for m in monthly_transactions.index], rotation=45)
    ax2.tick_params(axis='y', labelcolor=color)
    ax3 = ax2.twinx()
    ax3.plot(monthly_avg_sale.index, monthly_avg_sale, marker='s', markersize=6, 
             linestyle='-', linewidth=2, color='#9B59B6', label="Ortalama Satış Tutarı")
    ax3.set_ylabel("Ortalama Satış Tutarı (₺)", fontsize=12, color='#9B59B6')
    ax3.tick_params(axis='y', labelcolor='#9B59B6')
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax3.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    growth_rate = ((monthly_sales.iloc[-1] - monthly_sales.iloc[0]) / monthly_sales.iloc[0]) * 100
    peak_season = ', '.join([month_names[i] for i in monthly_sales.nlargest(3).index])
    slow_season = ', '.join([month_names[i] for i in monthly_sales.nsmallest(3).index])
    
    analysis_text = (
        f"Analiz Özeti:\n"
        f"• Büyüme Oranı: {growth_rate:.1f}% (ilk aydan son aya)\n"
        f"• Yüksek Sezon: {peak_season}\n"
        f"• Düşük Sezon: {slow_season}\n"
        f"• En Yüksek Satış: {monthly_sales.max():,.0f}₺ ({month_names[max_month]})\n"
        f"• En Düşük Satış: {monthly_sales.min():,.0f}₺ ({month_names[min_month]})"
    )
    fig.text(0.12, 0.01, analysis_text, fontsize=11, bbox=dict(facecolor='white', alpha=0.8))
    plt.tight_layout()
    plt.subplots_adjust(top=0.92, bottom=0.15)
    plt.savefig("monthly_sales_trend.png")
    plt.show()
    
    # 4. Grafik: Gelir Seviyesi vs Satın Alma Tutarı (Boxplot + İstatistik Tablosu)
    plt.figure(figsize=(14, 10))
    ax = sns.boxplot(x=df['Income'], y=df['Amount'], palette="viridis", width=0.6, 
                     linewidth=1.5, fliersize=4, showmeans=True,
                     meanprops={"marker": "o", "markerfacecolor": "white", "markeredgecolor": "black", "markersize": 8})
    plt.title("Gelir Seviyesi ve Satın Alma Tutarı İlişkisi", fontsize=16, fontweight='bold', pad=15)
    plt.xlabel("Müşteri Gelir Seviyesi", fontsize=14, labelpad=10)
    plt.ylabel("Satın Alma Tutarı (₺)", fontsize=14, labelpad=10)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    income_stats = df.groupby('Income')['Amount'].agg(['mean', 'median', 'std', 'count']).reset_index()
    table_data = []
    for _, row in income_stats.iterrows():
        table_data.append([row['Income'], f"{row['mean']:.2f}", f"{row['median']:.2f}", f"{row['std']:.2f}", f"{row['count']}"])
    table = plt.table(cellText=table_data,
                      colLabels=['Gelir Seviyesi', 'Ortalama', 'Medyan', 'Std. Sapma', 'İşlem Sayısı'],
                      cellLoc='center',
                      loc='bottom',
                      bbox=[0, -0.45, 1, 0.25])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.5)
    plt.figtext(0.02, 0.02, 
                "Analiz Notları:\n• Gelir seviyesi arttıkça satın alma tutarı genellikle artış göstermektedir.\n"
                "• Yüksek gelirli müşterilerde satın alma tutarı dağılımı daha geniştir, bu da daha çeşitli alışveriş davranışlarını gösterir.\n"
                "• Orta gelirli müşterilerde aykırı değerler daha fazla olabilir.",
                fontsize=11, wrap=True, bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.5'))
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.45)
    plt.savefig("income_vs_amount.png")
    plt.show()

#####################################
# PySpark Tabanlı Analiz Fonksiyonları
#####################################

def initialize_spark():
    """Spark oturumunu başlatır"""
    spark = SparkSession.builder \
        .appName("RetailDataAnalysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
    return spark

def save_processed_data(spark_df):
    """İşlenmiş Spark DataFrame'ini Parquet formatında kaydeder"""
    spark_df.write.mode("overwrite").parquet("processed_retail_data.parquet")
    print("Processed data saved to 'processed_retail_data.parquet'")

def run_spark_analysis(spark, pandas_df):
    """
    PySpark kullanarak, Pandas'da yapılan temizlikten sonra oluşturulan DataFrame'den
    Spark DataFrame oluşturulur, ardından gerekli dönüşümler yapılır.
    """
    # Pandas DataFrame'den Spark DataFrame'e dönüştürme
    spark_df = spark.createDataFrame(pandas_df)
    
    # Gereksiz sütunlar zaten Pandas'da kaldırıldı; 
    # tip dönüşümleri ve explicit şema uygulaması yapılabilir.
    print("Spark DataFrame (Pandas dönüşümünden):")
    spark_df.printSchema()
    
    print("Spark DataFrame İstatistiksel Özet:")
    spark_df.describe().show()
    
    print("Product_Category dağılımı (Spark):")
    spark_df.groupBy("Product_Category").count().orderBy("count", ascending=False).show()
    
    # Null değer sayıları
    schema_dict = dict(spark_df.dtypes)
    null_counts = spark_df.select([
        sum(
            when(
                col(c).isNull() | (isnan(col(c)) if schema_dict[c] in ["double", "float"] else col(c).isNull()),
                1
            ).otherwise(0)
        ).alias(c) for c in spark_df.columns
    ])
    print("Spark DataFrame için null değer sayıları:")
    null_counts.show()
    
    print("Kategori bazında ortalama 'Amount':")
    spark_df.groupBy("Product_Category").agg({"Amount": "mean"}).orderBy("avg(Amount)", ascending=False).show()
    
    # Müşteri bazında toplam harcama
    customer_spending = spark_df.groupBy("Customer_ID").agg(sum("Total_Amount").alias("Total_Spending"))
    customer_spending = customer_spending.withColumn("Total_Spending", round(col("Total_Spending"), 2))
    print("En çok harcayan müşteriler (ilk 10):")
    customer_spending.orderBy(col("Total_Spending"), ascending=False).show(10)
    
    # Aylık satış trendi: 'Date' sütunu üzerinden dönüşüm yapıyoruz.
    monthly_sales = spark_df.groupBy(month("Date").alias("Month")).agg(sum("Total_Amount").alias("Total_Sales"))
    monthly_sales = monthly_sales.withColumn("Total_Sales", round(col("Total_Sales"), 2))
    print("Aylık satış trendi (Spark):")
    monthly_sales.orderBy("Month").show()
    
    # Müşteri segmentleri
    customer_segments = customer_spending.withColumn("Segment",
        when(col("Total_Spending") > 5000, "VIP")
        .when(col("Total_Spending") > 1000, "Regular")
        .otherwise("Low Spender")
    )
    print("Müşteri segmentleri:")
    customer_segments.groupBy("Segment").count().show()
    
    # Ürün kategorilerine göre satışlar
    category_sales = spark_df.groupBy("Product_Category").agg(sum("Total_Amount").alias("Total_Sales"))
    print("Ürün kategorilerine göre satışlar:")
    category_sales.orderBy(col("Total_Sales"), ascending=False).show()
    
    # Ödeme yöntemleri
    payment_methods = spark_df.groupBy("Payment_Method").agg(count("Transaction_ID").alias("Total_Transactions"))
    print("Ödeme yöntemleri:")
    payment_methods.orderBy(col("Total_Transactions"), ascending=False).show()
    
    # Explicit şema uygulaması (isteğe bağlı)
    schema = StructType([
        StructField("Transaction_ID", LongType(), True),
        StructField("Customer_ID", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Zipcode", LongType(), True),
        StructField("Country", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Income", StringType(), True),
        StructField("Customer_Segment", StringType(), True),
        StructField("Date", TimestampType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Time", StringType(), True),
        StructField("Total_Purchases", IntegerType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Total_Amount", DoubleType(), True),
        StructField("Product_Category", StringType(), True),
        StructField("Product_Brand", StringType(), True),
        StructField("Product_Type", StringType(), True),
        StructField("Feedback", StringType(), True),
        StructField("Shipping_Method", StringType(), True),
        StructField("Payment_Method", StringType(), True),
        StructField("Order_Status", StringType(), True),
        StructField("Ratings", DoubleType(), True),
        StructField("products", StringType(), True),
    ])
    spark_df = spark_df.select(
        col("Transaction_ID").cast(LongType()),
        col("Customer_ID").cast(LongType()),
        col("Name").cast(StringType()),
        col("City").cast(StringType()),
        col("State").cast(StringType()),
        col("Zipcode").cast(LongType()),
        col("Country").cast(StringType()),
        col("Age").cast(IntegerType()),
        col("Gender").cast(StringType()),
        col("Income").cast(StringType()),
        col("Customer_Segment").cast(StringType()),
        col("Date").cast(TimestampType()),
        col("Year").cast(IntegerType()),
        col("Month").cast(IntegerType()),
        col("Time").cast(StringType()),
        col("Total_Purchases").cast(IntegerType()),
        col("Amount").cast(DoubleType()),
        col("Total_Amount").cast(DoubleType()),
        col("Product_Category").cast(StringType()),
        col("Product_Brand").cast(StringType()),
        col("Product_Type").cast(StringType()),
        col("Feedback").cast(StringType()),
        col("Shipping_Method").cast(StringType()),
        col("Payment_Method").cast(StringType()),
        col("Order_Status").cast(StringType()),
        col("Ratings").cast(DoubleType()),
        col("products").cast(StringType())
    )
    print("Explicit şema uygulandıktan sonra Spark DataFrame şeması:")
    spark_df.printSchema()
    
    # İşlenmiş veriyi Parquet formatında kaydet
    save_processed_data(spark_df)
    
    # İsteğe bağlı: Spark DataFrame'i Pandas'a çevirip JSON formatında almak (DB yazımı için hazır)
    df_pandas = spark_df.toPandas()
    json_data = df_pandas.to_dict(orient="records")
    print("Spark DataFrame, Pandas sözlüğü formatına dönüştürüldü (DB yazımı için hazır)")
    return json_data

#####################################
# Ana Fonksiyon
#####################################

def main():
    # Pandas tabanlı analiz
    print(">>> Pandas tabanlı analiz başlatılıyor...")
    df = load_data_pandas()
    df = clean_data_pandas(df)
    visualize_data_pandas(df)
    
    # PySpark tabanlı analiz: Pandas'da yapılan işlemler sonrası elde edilen df üzerinden Spark DataFrame oluşturuyoruz.
    print(">>> PySpark tabanlı analiz başlatılıyor...")
    spark = initialize_spark()
    json_data = run_spark_analysis(spark, df)
    
    # json_data, veritabanına yazım için kullanılabilir.
    
    spark.stop()
    print("Analiz tamamlandı!")

if __name__ == "__main__":
    main()
