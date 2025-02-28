# Retail Data Analysis

## Overview
This project performs exploratory data analysis (EDA) on retail transaction data. It includes data loading, cleaning, transformation, visualization, and MongoDB integration. The analysis provides insights into customer behavior, sales trends, and product performance.

## Files
- `eda.py`: Python script for data cleaning, transformation, and exploratory analysis
- `mongo_upload.py`: Python script for uploading processed data to MongoDB using Spark
- `mongo_read.py`: Python script for reading data from MongoDB using Spark and performing analysis
- `run_all.py`: Python script to run all three scripts in sequence
- `run_all.sh`: Shell script to run all scripts in sequence (for Unix/Linux/Mac)
- `retail_data.csv`: Input data file (not included in the repository)

## Requirements
- Python 3.6+
- PySpark
- Pandas
- NumPy
- Matplotlib
- Seaborn
- PyMongo
- MongoDB Spark Connector

## Installation
```bash
pip install pyspark pandas numpy matplotlib seaborn pymongo
```

## Usage

### Option 1: Run All Scripts at Once

#### Using Python (Cross-platform)
```bash
python run_all.py
```

#### Using Shell Script (Unix/Linux/Mac)
```bash
chmod +x run_all.sh  # Make the script executable (first time only)
./run_all.sh
```

All of these options will:
1. Run the EDA script
2. Run the MongoDB upload script
3. Run the MongoDB read script

The scripts will stop if any of the steps fail and will display the total execution time at the end.

### Option 2: Run Scripts Individually

#### Step 1: Data Cleaning and Analysis
First, run the EDA script to clean and analyze the data:
```bash
python eda.py
```
This will:
- Load the retail data from CSV
- Clean and transform the data
- Apply proper schema
- Perform exploratory analysis
- Create visualizations
- Save the processed data to a parquet file for later use

#### Step 2: Upload Data to MongoDB
After running the EDA script, you can upload the processed data to MongoDB:
```bash
python mongo_upload.py
```
This will:
- Load the processed data from the parquet file
- Connect to MongoDB using the Spark MongoDB connector
- Upload the data to the specified MongoDB collection

#### Step 3: Read Data from MongoDB
Finally, you can read the data from MongoDB and perform additional analysis:
```bash
python mongo_read.py
```
This will:
- Connect to MongoDB using the Spark MongoDB connector
- Read the data from the specified MongoDB collection
- Perform basic analysis on the data
- Save a sample of the data to CSV for verification

## Analysis Performed

### Data Cleaning (eda.py)
- Removing unnecessary columns (Email, Phone, Address)
- Checking for negative values in the Amount column
- Formatting date values
- Removing duplicate rows

### Exploratory Analysis (eda.py)
- Basic statistics of the dataset
- Checking for null values
- Distribution of categorical variables (Product Category)
- Average purchase amount by category
- Top spending customers
- Monthly sales trends
- Customer segmentation (VIP, Regular, Low Spender)
- Sales by product category
- Payment method analysis

### Visualizations (eda.py)
- Distribution of transaction amounts
- Monthly sales trends
- Product category distribution
- Age distribution by product category

### MongoDB Analysis (mongo_read.py)
- Product category distribution
- Average amount by payment method
- Total sales by customer segment
- Sample data verification

## Data Schema
The dataset contains the following fields:
- Transaction_ID: Unique identifier for each transaction
- Customer_ID: Unique identifier for each customer
- Name: Customer name
- City: Customer city
- State: Customer state
- Zipcode: Customer zipcode
- Country: Customer country
- Age: Customer age
- Gender: Customer gender
- Income: Customer income level
- Customer_Segment: Customer segment (New, Regular)
- Date: Transaction date
- Year: Transaction year
- Month: Transaction month
- Time: Transaction time
- Total_Purchases: Number of items purchased
- Amount: Transaction amount
- Total_Amount: Total amount spent
- Product_Category: Category of product purchased
- Product_Brand: Brand of product purchased
- Product_Type: Type of product purchased
- Feedback: Customer feedback
- Shipping_Method: Shipping method used
- Payment_Method: Payment method used
- Order_Status: Status of the order
- Ratings: Customer rating
- products: Product name

## Results
The analysis reveals insights about:
- Sales patterns across different product categories
- Customer spending behavior
- Seasonal trends in sales
- Payment method preferences
- Age distribution across product categories

## Future Work
- Implement predictive models for sales forecasting
- Develop customer segmentation using clustering algorithms
- Create a recommendation system based on purchase history
- Perform sentiment analysis on customer feedback