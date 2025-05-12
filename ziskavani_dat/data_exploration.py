import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

# Set style for better looking plots
plt.style.use('default')
sns.set_palette("husl")


SARZE = "sarze_2"


def load_and_prepare_data(file_path):
    df = pd.read_csv(file_path)
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
    return df

def create_combined_plots(dataframes, filenames):
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 18))
    
    # Time series plot for temperatures
    for df, filename in zip(dataframes, filenames):
        if 'teplota' in filename:  # Only plot temperature data
            label = os.path.basename(filename).replace('.csv', '').replace('_', ' ').title()
            ax1.plot(df['time'], df['value'], label=label, alpha=0.7)
    
    ax1.set_title('Combined Temperature Time Series')
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Temperature (°C)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Time series plot for power consumption
    power_files = [
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav0_prikon.csv',
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav1_prikon.csv',
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav2_prikon.csv',
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav3_prikon.csv'
    ]
    
        
    # Dictionary to store aggregated data for each file
    aggregated_data = {}
    
    # Process each power file for hour-of-day aggregation
    # Dictionary to store individual power consumption data
    all_power_data = {}
    
    for power_file in power_files:
        # Read the CSV file
        df = pd.read_csv(power_file)
        df['time'] = pd.to_datetime(df['time'])
        
        # Resample to hourly and calculate sum
        hourly_power = df.resample('h', on='time')['value'].sum()
        
        # Convert to kWh (divide by 60 since data is in kW)
        hourly_kwh = hourly_power / 60
        
        # Extract hour of day and date
        hourly_kwh_df = hourly_kwh.reset_index()
        hourly_kwh_df['hour'] = hourly_kwh_df['time'].dt.hour
        hourly_kwh_df['date'] = hourly_kwh_df['time'].dt.date
        
        # Convert date to string to avoid datetime operations in groupby
        hourly_kwh_df['date_str'] = hourly_kwh_df['date'].astype(str)
        
        # Group by date_str and hour and sum only the value column
        grouped = hourly_kwh_df.groupby(['date_str', 'hour'])['value'].sum().reset_index()
        
        # Create a timestamp for better plotting
        grouped['timestamp'] = grouped.apply(
            lambda x: pd.Timestamp(f"{x['date_str']} {x['hour']}:00:00"), axis=1
        )
        
        # Store in dictionary
        label = os.path.basename(power_file).replace('.csv', '').replace('_', ' ').title()
        aggregated_data[label] = grouped
        all_power_data[label] = grouped
        
        # Plot individual sources
        #plt.plot(grouped['timestamp'], grouped['value'], label=label, alpha=0.7)
    
    # Calculate total power consumption across all sources
    if all_power_data:
        # Create a common timestamp index
        all_timestamps = sorted(set(ts for data in all_power_data.values() for ts in data['timestamp']))
        total_power = pd.DataFrame({'timestamp': all_timestamps})
        
        # Merge all data on timestamp
        for label, data in all_power_data.items():
            total_power = pd.merge(total_power, 
                                   data[['timestamp', 'value']], 
                                   on='timestamp', 
                                   how='left', 
                                   suffixes=('', f'_{label}'))
        
        # Fill NaN values with 0
        for col in total_power.columns:
            if col != 'timestamp' and 'value' in col:
                total_power[col] = total_power[col].fillna(0)
        
        # Sum all value columns
        value_cols = [col for col in total_power.columns if 'value' in col]
        total_power['total_value'] = total_power[value_cols].sum(axis=1)
        
        # Plot the total
        plt.plot(total_power['timestamp'], total_power['total_value'], 
                 label='Total Power Consumption', 
                 linewidth=2, color='black')
    
     
    ax2.set_title('Hourly Power Consumption Aggregated by Hour and Day (kWh)')
    ax2.set_xlabel('Time')
    ax2.set_ylabel('Power Consumption (kWh)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)

    


    plt.tight_layout()
    plt.savefig(f'plots/{SARZE}/combined_temperature_power_analysis.png')
    plt.close()

def analyze_csv(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Convert time to datetime if it exists
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        
        # Calculate time differences and intervals
        time_diff = df['time'].diff()
        mean_interval = time_diff.mean()
        median_interval = time_diff.median()
        min_interval = time_diff.min()
        max_interval = time_diff.max()
        
        print(f"\nTime intervals for {os.path.basename(file_path)}:")
        print(f"Mean interval: {mean_interval}")
        print(f"Median interval: {median_interval}")
        print(f"Minimum interval: {min_interval}")
        print(f"Maximum interval: {max_interval}")
    
    # Create basic statistics
    print(f"\nBasic statistics for {os.path.basename(file_path)}:")
    print(df.describe())
    
    return df

def plot_hourly_power_consumption():
    # List of power consumption files
    power_files = [
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav0_prikon.csv',
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav1_prikon.csv',
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav2_prikon.csv',
        f'../data/{SARZE}/tepelna_cerpadla/xcc_tcstav3_prikon.csv'
    ]
    
    # Create figures
    plt.figure(figsize=(15, 8))
    
    # Process each power file for the original hourly time series
    for power_file in power_files:
        # Read the CSV file
        df = pd.read_csv(power_file)
        df['time'] = pd.to_datetime(df['time'])
        
        # Resample to hourly and calculate sum
        hourly_power = df.resample('h', on='time')['value'].sum()
        
        # Convert to kWh (divide by 60 since data is in kW)
        hourly_kwh = hourly_power / 60
        
        # Plot
        label = os.path.basename(power_file).replace('.csv', '').replace('_', ' ').title()
        plt.plot(hourly_kwh.index, hourly_kwh.values, label=label, alpha=0.7)
    
    plt.title('Hourly Power Consumption (kWh)')
    plt.xlabel('Time')
    plt.ylabel('Power Consumption (kWh)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(f'plots/{SARZE}/hourly_power_consumption.png')
    plt.close()
    
    # Create figure for hour-of-day aggregation
    plt.figure(figsize=(15, 8))
    
    # Create figure for daily aggregation
    plt.figure(figsize=(15, 8))
    
    # Process each power file for daily aggregation
    for power_file in power_files:
        # Read the CSV file
        df = pd.read_csv(power_file)
        df['time'] = pd.to_datetime(df['time'])
        
        # Resample to hourly first, then sum to daily
        hourly_power = df.resample('h', on='time')['value'].sum()
        hourly_kwh = hourly_power / 60
        
        # Resample to daily
        daily_kwh = hourly_kwh.resample('D').sum()
        
        # Plot
        label = os.path.basename(power_file).replace('.csv', '').replace('_', ' ').title()
        plt.plot(daily_kwh.index, daily_kwh.values, label=label, alpha=0.7)
    
    plt.title('Daily Power Consumption (kWh)')
    plt.xlabel('Date')
    plt.ylabel('Power Consumption (kWh)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(f'plots/{SARZE}/daily_power_consumption.png')
    plt.close()

def main():
    # Create plots directory if it doesn't exist
    if not os.path.exists('plots'):
        os.makedirs('plots')
    
    # List of CSV files to analyze
    csv_files = [
        f'../data/{SARZE}/teplota/xcc_venkovni_teplota.csv',
        f'../data/{SARZE}/teplota/gw1100a_outdoor_temperature.csv',
        f'../data/{SARZE}/teplota/xcc_venkovni_teplota_nefiltrovana.csv',
        f'../data/{SARZE}/teplota/gw1100a_indoor_temperature.csv'
    ]
    
    # Analyze each CSV file and collect dataframes
    dataframes = []
    for csv_file in csv_files:
        print(f"\nAnalyzing {csv_file}...")
        df = analyze_csv(csv_file)
        dataframes.append(df)
    
    # Create combined plots
    create_combined_plots(dataframes, csv_files)
    
    # Create hourly power consumption plot
    plot_hourly_power_consumption()

if __name__ == "__main__":
    main()