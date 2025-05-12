import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path
from datetime import datetime
import numpy as np

def plot_csv_data(sarze_number, file_paths, start_time=None, end_time=None):
    """
    Plot data from multiple CSV files in different subdirectories on the same graph.
    
    Args:
        sarze_number (int): Number of the sarze directory (1-4)
        file_paths (list): List of tuples containing (subdirectory, filename, custom_name, color, alpha)
            where color is a matplotlib color string (e.g., 'blue', '#FF0000') and alpha is a float between 0 and 1
        start_time (str): Start time in format 'YYYY-MM-DD HH:MM:SS' (optional)
        end_time (str): End time in format 'YYYY-MM-DD HH:MM:SS' (optional)
    """
    # Create a figure with subplots
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Initialize variables to track min and max times
    min_time = None
    max_time = None
    
    for (subdirectory, filename, custom_name, color, alpha) in file_paths:
        # Construct the path to the CSV file
        base_path = Path(__file__).parent.parent / 'data'
        file_path = base_path / f'sarze_{sarze_number}' / subdirectory / filename
        
        if not file_path.exists():
            print(f"Error: File {file_path} does not exist!")
            continue
        
        # Read the CSV file
        try:
            df = pd.read_csv(file_path)
            
            # Convert time column to datetime if it exists
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time']).dt.tz_localize(None)  # Remove timezone info
                df.set_index('time', inplace=True)
                
                # Filter by time range if specified
                if start_time:
                    start_dt = pd.to_datetime(start_time)
                    df = df[df.index >= start_dt]
                if end_time:
                    end_dt = pd.to_datetime(end_time)
                    df = df[df.index <= end_dt]
                
                # Update min and max times
                if min_time is None or df.index.min() < min_time:
                    min_time = df.index.min()
                if max_time is None or df.index.max() > max_time:
                    max_time = df.index.max()
            
            print(f"\nColumns in {subdirectory}/{filename}: {df.columns.tolist()}")
            print(f"Data range: {df.index.min()} to {df.index.max()}")
            
            # Plot each numeric column
            for column in df.select_dtypes(include=['float64', 'int64']).columns:
                label = f"{custom_name}"
                ax.plot(df.index, df[column], label=label, color=color, alpha=alpha)  #, marker='o', markersize=3, linestyle='-')
            
        except Exception as e:
            print(f"Error reading or plotting the file {subdirectory}/{filename}: {e}")
            print(f"Error details: {str(e)}")
    
    # Customize the plot
    ax.set_title(f'')
    ax.set_xlabel('Time')
    ax.set_ylabel('Value')
    ax.legend(bbox_to_anchor=(0.95, 1), loc='upper left')  # Place legend outside the plot
    ax.grid(True)
    
    # Set x-axis limits to match the data range
    if min_time is not None and max_time is not None:
        ax.set_xlim(min_time, max_time)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    
    # Save the plot to a file instead of showing it
    output_dir = Path(__file__).parent.parent / 'plots'
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'plot_{timestamp}.png'
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    print(f"\nPlot saved to: {output_file}")
    plt.close()  # Close the figure to free memory



if __name__ == "__main__":
    
    
    plot_csv_data(
        sarze_number=5,
        file_paths=[
            ('teplota', 'xcc_venkovni_teplota_nefiltrovana.csv', '°C, Venkovní teplota naměřená tepelnými čerpadly', 'brown', 0.8),
            ('teplota', 'gw1100a_outdoor_temperature.csv', '°C, Venkovní teplota naměřená meteostanicí', 'red', 0.8),
            #('teplota', 'xcc_venkovni_teplota.csv', '°C, Filtrace venkovní teploty', 'green', 0.8),
            #('tepelna_cerpadla/prikon', 'xcc_tcstav0_prikon.csv', 'kw, Příkon tepelného čerpadla č. 1', 'royalblue', 0.6),
            #('tepelna_cerpadla/prikon', 'xcc_tcstav1_prikon.csv', 'kw, Příkon tepelného čerpadla č. 2', 'dodgerblue', 0.6),
            #('tepelna_cerpadla/prikon', 'xcc_tcstav2_prikon.csv', 'kw, Příkon tepelného čerpadla č. 3', 'deepskyblue', 0.6),
            #('tepelna_cerpadla/prikon', 'xcc_tcstav3_prikon.csv', 'kw, Příkon tepelného čerpadla č. 4', 'cornflowerblue', 0.6)
        ],
        start_time='2025-04-26 0:00:00',  
        end_time='2025-04-30 00:00:00'     

    )
