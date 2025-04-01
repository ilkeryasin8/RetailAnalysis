import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def process_data(input_file, output_file):
    # Veriyi oku
    df = pd.read_csv(input_file)
    
    # Date sütununu datetime'a çevir
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Year değeri boş olanları Date'den doldur
    df['Year'] = df['Year'].fillna(df['Date'].dt.year)
    df['Month'] = df['Month'].fillna(df['Date'].dt.month)

    # hem month hem de date boş olanları sil
    df = df.dropna(subset=['Month', 'Date'])
    
    # Total_Purchases boş değerlerini hesapla
    df['Total_Purchases'] = df['Total_Purchases'].fillna(
        (df['Total_Amount'] / df['Amount']).round(0)  # Yukarı yuvarlama
    )
    
    # Eski Transaction_ID'yi yeniden adlandır
    if 'Transaction_ID' in df.columns:
        df = df.rename(columns={'Transaction_ID': 'Old_Transaction_ID'})
    
    # Yeni Transaction_ID oluştur (1'den başlayarak)
    df['Transaction_ID'] = range(1, len(df) + 1)
    
    # Customer_ID ve Age boş olan kayıtları sil
    df = df.dropna(subset=['Customer_ID', 'Age'])
    
    # 1. Belirtilen sütunları at
    columns_to_drop = ['Email', 'Name', 'Zipcode', 'Address', 'Phone', 'City', 'State', 
                      'Old_Transaction_ID', 'Order_Status', 'Time','Date','Amount','Total_Amount']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    # Boş değerleri doldur
    df['Product_Category'] = df['Product_Category'].fillna('Other')
    df['Product_Brand'] = df['Product_Brand'].fillna('Other')
    df['Feedback'] = df['Feedback'].fillna('Average')
    df['Shipping_Method'] = df['Shipping_Method'].fillna('Standard')
    df['Payment_Method'] = df['Payment_Method'].fillna('Credit Card')
    df['Ratings'] = df['Ratings'].fillna(3)
    
    # 2. Customer_ID'ye göre ülke, cinsiyet ve müşteri segmentini işleme al
    df['Country'] = df.groupby('Customer_ID')['Country'].transform(
    lambda x: x.mode().iloc[0] if x.value_counts().max() > 1 else 'USA'
    )
    df['Gender'] = df.groupby('Customer_ID')['Gender'].transform(
        lambda x: x.mode().iloc[0] if x.value_counts().max() > 1 else 'Female'
    )
    df['Customer_Segment'] = df.groupby('Customer_ID')['Customer_Segment'].transform(
        lambda x: x.mode().iloc[0] if x.value_counts().max() > 1 else 'Regular'
    )
    
    # 3. Yaş bilgisi için işleme:
    # Eğer tekrarlı değer varsa en çok tekrar edeni, yoksa ortalamayı al.
    def mode_or_avg(x):
        counts = x.value_counts()
        if counts.max() > 1:  # tekrarlı değer mevcutsa
            return counts.idxmax()
        else:
            return x.mean()
    
    grouped_ages = df.groupby('Customer_ID')['Age']
    mode_ages = grouped_ages.agg(mode_or_avg)
    df['Age'] = df['Customer_ID'].map(mode_ages).round(0)  # Yukarı yuvarlama
    
    # 4. Income işleme:
    # Kategorik değerleri sayısal değerlere çevir
    income_map = {
        'Low': 1,
        'Medium': 2,
        'High': 3
    }
    df['Income'] = df['Income'].fillna('Medium')  # Boş değerleri Medium yap
    df['Income'] = df['Income'].map(income_map)
    
    # Her müşteri için Income ortalamasını al
    grouped_income = df.groupby('Customer_ID')['Income']
    avg_income = grouped_income.agg('mean')
    df['Income'] = df['Customer_ID'].map(avg_income).round(1)  # Virgülden sonra 1 basamak
    
    # 5. Customer_ID'ye göre sırala
    df = df.sort_values('Customer_ID')
    
    # Boş değer analizi ve grafik
    null_counts = df.isnull().sum()
    
    # Grafik oluştur
    plt.figure(figsize=(12, 6))
    null_counts.plot(kind='bar')
    plt.title('Boş Değer Sayıları')
    plt.xlabel('Sütunlar')
    plt.ylabel('Boş Değer Sayısı')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('null_values_analysis.png')
    
    # Boş değer sayılarını yazdır
    print("\nBoş Değer Sayıları:")
    print(null_counts)
    
    # İşlenmiş veriyi kaydet
    df.to_csv(output_file, index=False)
    print(f"\nData processing completed. Output saved to {output_file}")

if __name__ == "__main__":
    input_file = "Retail_Analysis.csv"
    output_file = "new_cleaned_data.csv"
    process_data(input_file, output_file)
