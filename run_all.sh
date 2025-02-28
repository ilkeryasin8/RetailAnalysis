#!/bin/bash

# Run All Scripts
# --------------
# This shell script runs all three Python scripts in sequence:
# 1. eda.py - Data cleaning and analysis
# 2. mongo_upload.py - Upload data to MongoDB
# 3. mongo_read.py - Read data from MongoDB and analyze

# Function to run a script and check for errors
run_script() {
    script_name=$1
    description=$2
    
    echo -e "\n================================================================================"
    echo "RUNNING: $script_name - $description"
    echo -e "================================================================================\n"
    
    python "$script_name"
    
    # Check if the script executed successfully
    if [ $? -eq 0 ]; then
        echo -e "\n✅ $script_name completed successfully!\n"
        return 0
    else
        echo -e "\n❌ Error running $script_name. Stopping the pipeline.\n"
        return 1
    fi
}

# Start time
start_time=$(date +%s)

echo -e "\n🚀 Starting the retail data analysis pipeline...\n"

# Step 1: Run EDA script
run_script "eda.py" "Data cleaning and exploratory analysis"
if [ $? -ne 0 ]; then
    exit 1
fi

# Step 2: Run MongoDB upload script
run_script "mongo_upload.py" "Upload data to MongoDB"
if [ $? -ne 0 ]; then
    exit 1
fi

# Step 3: Run MongoDB read script
run_script "mongo_read.py" "Read and analyze data from MongoDB"
if [ $? -ne 0 ]; then
    exit 1
fi

# Calculate total execution time
end_time=$(date +%s)
execution_time=$((end_time - start_time))
minutes=$((execution_time / 60))
seconds=$((execution_time % 60))

echo -e "\n================================================================================"
echo "🎉 All scripts completed successfully!"
echo "⏱️ Total execution time: $minutes minutes and $seconds seconds"
echo -e "================================================================================\n" 