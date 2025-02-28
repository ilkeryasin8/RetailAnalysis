#!/usr/bin/env python
# coding: utf-8

"""
Run All Scripts
--------------
This script runs all three Python scripts in sequence:
1. eda.py - Data cleaning and analysis
2. mongo_upload.py - Upload data to MongoDB
3. mongo_read.py - Read data from MongoDB and analyze
"""

import os
import subprocess
import time
import sys

def run_script(script_name, description):
    """Run a Python script and handle errors"""
    print("\n" + "="*80)
    print(f"RUNNING: {script_name} - {description}")
    print("="*80 + "\n")
    
    try:
        # Run the script and capture output
        process = subprocess.run([sys.executable, script_name], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE,
                                 text=True,
                                 check=True)
        
        # Print the output
        print(process.stdout)
        
        print(f"\n✅ {script_name} completed successfully!\n")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error running {script_name}:")
        print(f"Exit code: {e.returncode}")
        print(f"Error message: {e.stderr}")
        return False

def main():
    """Main function to run all scripts in sequence"""
    start_time = time.time()
    
    print("\n🚀 Starting the retail data analysis pipeline...\n")
    
    # Step 1: Run EDA script
    if not run_script("eda.py", "Data cleaning and exploratory analysis"):
        print("❌ EDA script failed. Stopping the pipeline.")
        return
    
    # Step 2: Run MongoDB upload script
    if not run_script("mongo_upload.py", "Upload data to MongoDB"):
        print("❌ MongoDB upload script failed. Stopping the pipeline.")
        return
    
    # Step 3: Run MongoDB read script
    if not run_script("mongo_read.py", "Read and analyze data from MongoDB"):
        print("❌ MongoDB read script failed.")
        return
    
    # Calculate total execution time
    execution_time = time.time() - start_time
    minutes, seconds = divmod(execution_time, 60)
    
    print("\n" + "="*80)
    print(f"🎉 All scripts completed successfully!")
    print(f"⏱️ Total execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
    print("="*80 + "\n")

if __name__ == "__main__":
    main() 