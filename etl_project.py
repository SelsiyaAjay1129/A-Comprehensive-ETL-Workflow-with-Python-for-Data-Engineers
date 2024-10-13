import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import os

# Paths for log file and transformed data output
log_file = r'C:\Users\AJAY\Desktop\Guvi\Output\log_file.txt'
transformed_data_path = r'C:\Users\AJAY\Desktop\Guvi\Output\transformed_data.csv'

# Set the source path for your files
source_path = r'C:\Users\AJAY\Desktop\Guvi\Source'

# Function to log messages with timestamps
def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a') as f:
        f.write(f'{timestamp} - {message}\n')

# Function to extract data from CSV files
def extract_csv(file_path):
    try:
        log(f"Extracting data from {file_path}")
        return pd.read_csv(file_path)
    except Exception as e:
        log(f"Failed to extract CSV: {e}")
        return pd.DataFrame()

# Function to extract data from JSON files
def extract_json(file_path):
    try:
        log(f"Extracting data from {file_path}")
        return pd.read_json(file_path)
    except Exception as e:
        log(f"Failed to extract JSON: {e}")
        return pd.DataFrame()

# Function to extract data from XML files
def extract_xml(file_path):
    try:
        log(f"Extracting data from {file_path}")
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = []
        for record in root.findall('record'):
            row = {child.tag: child.text for child in record}
            data.append(row)
        return pd.DataFrame(data)
    except Exception as e:
        log(f"Failed to extract XML: {e}")
        return pd.DataFrame()

# Master function to extract data from all supported file types
def extract_data(files):
    all_data = pd.DataFrame()
    for file in files:
        if file.endswith('.csv'):
            df = extract_csv(file)
            log(f"Extracted {len(df)} records from {file}")  # Log the number of records
            all_data = pd.concat([all_data, df], ignore_index=True)
        elif file.endswith('.json'):
            df = extract_json(file)
            log(f"Extracted {len(df)} records from {file}")  # Log the number of records
            all_data = pd.concat([all_data, df], ignore_index=True)
        elif file.endswith('.xml'):
            df = extract_xml(file)
            log(f"Extracted {len(df)} records from {file}")  # Log the number of records
            all_data = pd.concat([all_data, df], ignore_index=True)
    return all_data

# Function to transform data (convert heights to meters and weights to kilograms)
def transform_data(df):
    try:
        log("Transforming data (converting heights and weights)")
        df['Height'] = pd.to_numeric(df['Height'], errors='coerce')
        df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce')
        df.dropna(subset=['Height', 'Weight'], inplace=True)
        df['Height'] = df['Height'].apply(lambda x: round(float(x) * 0.0254, 2))
        df['Weight'] = df['Weight'].apply(lambda x: round(float(x) * 0.453592, 2))
        return df
    except Exception as e:
        log(f"Transformation failed: {e}")
        return df

# Function to load transformed data into a CSV file
def load_data(df, output_path):
    try:
        log(f"Loading data into {output_path}")
        df.to_csv(output_path, index=False)
    except Exception as e:
        log(f"Failed to load data: {e}")

# Main ETL pipeline function
def etl_pipeline():
    log("ETL process started")
    
    # Extraction Phase
    log("Extraction phase started")
    files = glob.glob(os.path.join(source_path, '*'))  # Using the specified source path
    data = extract_data(files)
    log("Extraction phase completed")
    
    # Transformation Phase
    log("Transformation phase started")
    transformed_data = transform_data(data)
    log("Transformation phase completed")
    
    # Loading Phase
    log("Loading phase started")
    load_data(transformed_data, transformed_data_path)
    log("Loading phase completed")
    
    log("ETL process completed")

# Run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()
