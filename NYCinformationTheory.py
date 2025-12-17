#%%%
import pandas as pd
import requests
import os
import time
from io import BytesIO

# --- CONFIGURATION ---
START_YEAR = 2015 
END_YEAR = 2024
BOROUGHS = ['manhattan', 'brooklyn', 'queens', 'bronx', 'statenisland'] 
OUTPUT_DIR = 'nyc_raw_data'
MASTER_FILE = 'nyc_real_estate_history(all).csv'

# Column Mapping
COLUMN_MAP = {
    'SALE DATE': 'Date', 'SALE_DATE': 'Date',
    'SALE PRICE': 'Price', 'SALE_PRICE': 'Price',
    'GROSS SQUARE FEET': 'BuildingArea', 'GROSS_SQUARE_FEET': 'BuildingArea',
    'LAND SQUARE FEET': 'LandArea', 'LAND_SQUARE_FEET': 'LandArea',
    'YEAR BUILT': 'YearBuilt', 'YEAR_BUILT': 'YearBuilt',
    'BUILDING CLASS CATEGORY': 'BldgClass', 'BUILDING_CLASS_CATEGORY': 'BldgClass',
    'NEIGHBORHOOD': 'Neighborhood'
}

def download_file(year, borough):
    """
    Attempts to download the NYC Rolling Sales file using a 'shotgun' approach.
    """
    base_url = "https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales"
    boro_lower = borough.lower()
    boro_cap = borough.capitalize()
    
    patterns = [
        f"{base_url}/annualized-sales/{year}/{year}_{boro_lower}.xlsx",
        f"{base_url}/annualized-sales/{year}/{year}_{boro_lower}.xls",
        f"{base_url}/annualized-sales/{year}/{year}_{boro_cap}.xlsx",
        f"{base_url}/annualized-sales/{year}/{year}_{boro_cap}.xls",
        f"{base_url}/annualized-sales/{year}_{boro_lower}.xls",
        f"{base_url}/annualized-sales/{year}_{boro_cap}.xls",
        f"{base_url}/annualized-sales/sales_{year}_{boro_lower}.xls",
        f"{base_url}/09pdf/rolling_sales/sales_{year}_{boro_lower}.xls"
    ]
    
    for url in patterns:
        try:
            print(f"   Checking: {url} ...")
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                if b'<!DOCTYPE html>' not in response.content[:100]:
                    print(f"   -> FOUND! Downloading {year} {borough}...")
                    ext = url.split('.')[-1]
                    return BytesIO(response.content), ext
        except Exception:
            continue
            
    print(f"   -> SKIPPING {year} {borough} (Not found in known locations)")
    return None, None

def read_excel_auto_header(file_obj, ext):
    """
    Reads an Excel file and automatically finds the header row by looking
    for 'SALE PRICE' or 'SALE_PRICE'.
    """
    try:
        # Load the whole sheet (or first 20 rows to find header)
        if ext == 'xlsx':
            df_raw = pd.read_excel(file_obj, engine='openpyxl', header=None, nrows=20)
        else:
            df_raw = pd.read_excel(file_obj, header=None, nrows=20)
            
        # Find the row that contains "SALE PRICE" or "SALE_PRICE"
        header_idx = -1
        for i, row in df_raw.iterrows():
            row_str = row.astype(str).str.upper().values
            if any('SALE PRICE' in x or 'SALE_PRICE' in x for x in row_str):
                header_idx = i
                break
        
        # Reset file pointer to beginning
        file_obj.seek(0)
        
        # Read file again with correct header
        if header_idx != -1:
            if ext == 'xlsx':
                return pd.read_excel(file_obj, engine='openpyxl', header=header_idx)
            else:
                return pd.read_excel(file_obj, header=header_idx)
        else:
            # Fallback: Assume header is row 0 if keyword not found
            if ext == 'xlsx':
                return pd.read_excel(file_obj, engine='openpyxl')
            else:
                return pd.read_excel(file_obj)
                
    except Exception as e:
        print(f"      [Read Error]: {e}")
        return None

def process_data():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_frames = []
    print(f"--- Starting NYC Data Crawler ({START_YEAR}-{END_YEAR}) ---")
    
    for year in range(START_YEAR, END_YEAR + 1):
        for boro in BOROUGHS:
            print(f"Target: {year} {boro}")
            file_obj, ext = download_file(year, boro)
            
            if file_obj:
                # Use the new dynamic header reader
                df = read_excel_auto_header(file_obj, ext)
                
                if df is not None:
                    try:
                        # 1. Standardize Headers
                        df.columns = [str(c).strip().replace('\n', '') for c in df.columns]
                        df.columns = [c.upper() for c in df.columns]
                        
                        # Rename columns
                        cols_to_keep = [c for c in df.columns if c in COLUMN_MAP]
                        df = df[cols_to_keep].rename(columns=COLUMN_MAP)
                        
                        # 2. Cleaning
                        for col in ['Price', 'BuildingArea', 'LandArea']:
                            if col in df.columns:
                                df[col] = df[col].astype(str).str.replace(r'[$,]', '', regex=True)
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                                
                        if 'Date' in df.columns:
                            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

                        df['Boro_Source'] = boro
                        df['Year_Source'] = year
                        
                        # Filter
                        if 'Price' in df.columns and 'BuildingArea' in df.columns:
                            valid_rows = len(df)
                            df = df[
                                (df['Price'] > 1000) & 
                                (df['BuildingArea'] > 100)
                            ]
                            print(f"   -> Extracted {len(df)} valid sales (from {valid_rows} raw rows).")
                            all_frames.append(df)
                        else:
                            print("   -> WARNING: Could not find Price/Area columns after header fix.")
                        
                    except Exception as e:
                        print(f"   -> Processing Error: {e}")
            
            time.sleep(0.5)

    print("\n--- Merging All Years ---")
    if all_frames:
        master_df = pd.concat(all_frames, ignore_index=True)
        print(f"Total Observations: {len(master_df)}")
        master_df.to_csv(MASTER_FILE, index=False)
        print(f"\nSUCCESS! Master dataset saved to: {MASTER_FILE}")
    else:
        print("No data was downloaded.")

if __name__ == "__main__":
    process_data()

#%%
# Inflation Adjustment

import pandas as pd
import numpy as np
import pandas_datareader.data as web
import datetime

df = pd.read_csv('nyc_real_estate_history(all).csv', low_memory=False)
df['Date'] = pd.to_datetime(df['Date'])

# Series ID: 'CPIAUCSL' 
# (Consumer Price Index for All Urban Consumers: All Items, Seasonally Adjusted)
# This is the standard monthly inflation metric.

print("Fetching CPI data from FRED...")

# Determine date range needed (add buffer to ensure coverage)
start_date = df['Date'].min().replace(day=1)
end_date = datetime.datetime.now()

# Fetch data using pandas_datareader
try:
    cpi_df = web.DataReader('CPIAUCSL', 'fred', start_date, end_date)
except ImportError:
    raise ImportError("Please install the library: pip install pandas-datareader")

# Rename column for clarity
cpi_df.columns = ['CPI']

# Align Data for Monthly Granularity
# Create a 'YearMonth' key in both dataframes to merge on. 
# This aligns a specific transaction date (e.g., 2022-05-14) with that month's CPI (2022-05-01).

# Convert index to Year-Month period for easy merging
cpi_df.index = cpi_df.index.to_period('M')
df['YearMonth'] = df['Date'].dt.to_period('M')

# Merge CPI data into the main dataframe
df = df.merge(cpi_df, left_on='YearMonth', right_index=True, how='left')

# Handling lag: CPI data usually lags by 1 month. 
# If recent transactions have NaN CPI, fill with the most recent available CPI value.
df['CPI'] = df['CPI'].ffill()

# Calculate Multiplier (Base: Latest Available Month)
# normalize everything to "Today's Dollars" (the most recent CPI print downloaded)

target_cpi = cpi_df['CPI'].iloc[-1]
latest_date_str = cpi_df.index[-1].strftime('%B %Y')

print(f"Adjusting prices to {latest_date_str} dollars (CPI: {target_cpi})")

# Formula: Real_Price = Nominal_Price * (Target_CPI / Month_Sold_CPI)
df['Inflation_Multiplier'] = target_cpi / df['CPI']

# Calculate Real Metrics

df['Real_Price'] = df['Price'] * df['Inflation_Multiplier']

# Update PPSF to be "Real PPSF"
df['Real_PPSF'] = df['Real_Price'] / df['BuildingArea']
df['Log_Real_PPSF'] = np.log(df['Real_PPSF'].replace(0, np.nan)) # Handle potential log(0) errors

# Clean up helper column if desired
df.drop(columns=['YearMonth', 'CPI'], inplace=True)

# Save the "Real" dataset
output_filename = 'nyc_real_estate_history(all)_REAL_DOLLARS.csv'
df.to_csv(output_filename, index=False)

print(f"Conversion complete. Data saved to {output_filename}") 

#%%
# EDA / descriptive stats pipeline 

df['BldgClass'].unique() 

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# --- Configuration ---
INPUT_FILE = 'nyc_real_estate_history(all)_REAL_DOLLARS.csv'

def perform_eda():
    # 1. Load Data
    print(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    
    # 2. Data Transformation
    # Log Building Area (handling zeros/negatives)
    df['Log_BuildingArea'] = np.log(df['BuildingArea'].replace(0, np.nan))
    
    # Log Land Area (New column to replace raw Land Area plot)
    df['Log_LandArea'] = np.log(df['LandArea'].replace(0, np.nan))
    
    # Clean Building Class
    df['BldgClass'] = df['BldgClass'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

    # 3. Descriptive Statistics (Log Vars Only)
    log_cols = ['Log_Real_PPSF', 'Log_BuildingArea', 'Log_LandArea']
    print("\n--- Descriptive Statistics (Log Transformed) ---")
    print(df[log_cols].describe().apply(lambda s: s.apply('{0:.2f}'.format)))
    
    # --- Visualizations ---
    sns.set(style="whitegrid")
    
    # Plot 1: Distribution of Building Classes
    plt.figure(figsize=(12, 6))
    top_classes = df['BldgClass'].value_counts().head(15).index
    sns.countplot(y='BldgClass', data=df[df['BldgClass'].isin(top_classes)], 
                  order=top_classes, palette='viridis')
    plt.title('Top 15 Building Classes')
    plt.xlabel('Count')
    plt.tight_layout()
    plt.show() # Display plot
    
    # Plot 2: Log Area Distributions
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Log Land Area
    # Filter infinite/NaN values for clean plotting
    land_data = df['Log_LandArea'].replace([np.inf, -np.inf], np.nan).dropna()
    sns.histplot(land_data, bins=50, ax=axes[0], color='skyblue')
    axes[0].set_title('Log Land Area Distribution')
    
    # Log Building Area
    bldg_data = df['Log_BuildingArea'].replace([np.inf, -np.inf], np.nan).dropna()
    sns.histplot(bldg_data, bins=50, ax=axes[1], color='orange')
    axes[1].set_title('Log Building Area Distribution')
    
    plt.tight_layout()
    plt.show() # Display plot

    # Plot 3: Log Price Distribution
    plt.figure(figsize=(10, 6))
    price_data = df['Log_Real_PPSF'].replace([np.inf, -np.inf], np.nan).dropna()
    sns.histplot(price_data, bins=50, color='purple')
    plt.title('Log Real Price Per Sq Ft Distribution')
    plt.tight_layout()
    plt.show() # Display plot

    # Plot 4: Price vs Building Class
    plt.figure(figsize=(12, 8))
    # Use top 10 classes
    top_10 = df['BldgClass'].value_counts().head(10).index
    subset = df[df['BldgClass'].isin(top_10)].copy()
    subset = subset[np.isfinite(subset['Log_Real_PPSF'])] # Ensure clean data
    
    sns.boxplot(x='Log_Real_PPSF', y='BldgClass', data=subset, order=top_10, palette='Set2')
    plt.title('Log Real Price Per Sq Ft by Building Class')
    plt.tight_layout()
    plt.show() # Display plot

    # Plot 5: Correlation Matrix
    plt.figure(figsize=(8, 6))
    clean_corr_df = df[log_cols].replace([np.inf, -np.inf], np.nan).dropna()
    corr = clean_corr_df.corr()
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f", vmin=-1, vmax=1)
    plt.title('Correlation Matrix (Log Variables)')
    plt.tight_layout()
    plt.show() # Display plot

if __name__ == "__main__":
    perform_eda()

#%%  
# Data Cleaning
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

INPUT_FILE = 'nyc_real_estate_history(all)_REAL_DOLLARS.csv'

# 1. Load Data
print(f"Loading {INPUT_FILE}...")
df = pd.read_csv(INPUT_FILE, low_memory=False)

# 2. Clean Building Class Column (Standardize format)
df['BldgClass'] = df['BldgClass'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

# 3. Define Residential Classes to Keep
residential_classes = [
        '01 ONE FAMILY DWELLINGS',
        '02 TWO FAMILY DWELLINGS',
        '03 THREE FAMILY DWELLINGS',
        '04 TAX CLASS 1 CONDOS',
        '07 RENTALS - WALKUP APARTMENTS',
        '08 RENTALS - ELEVATOR APARTMENTS',
        '09 COOPS - WALKUP APARTMENTS',
        '10 COOPS - ELEVATOR APARTMENTS',
        '11A CONDO-RENTALS',
        '12 CONDOS - WALKUP APARTMENTS',
        '13 CONDOS - ELEVATOR APARTMENTS',
        '14 RENTALS - 4-10 UNIT',
        '15 CONDOS - 2-10 UNIT RESIDENTIAL',
        '16 CONDOS - 2-10 UNIT WITH COMMERCIAL UNIT',
        '17 CONDO COOPS'
    ]

# 4. Filter Data
initial_count = len(df)
df = df[df['BldgClass'].isin(residential_classes)].copy()
final_count = len(df)
print(f"Filtered out {initial_count - final_count} non-residential records.")
print(f"Remaining records: {final_count}")

# 5. Save the Cleaned Data
df['Date'] = pd.to_datetime(df['Date'])
df.to_csv('nyc_residential_cleaned.csv', index=False)
print(f"Saved cleaned dataset to {INPUT_FILE}")

# 6. Recalculate Log Metrics (Ensure columns exist and are up to date)
# Use a small epsilon or replace 0 with NaN to avoid log(0) errors
df['Log_BuildingArea'] = np.log(df['BuildingArea'].replace(0, np.nan))
df['Log_Real_PPSF'] = np.log(df['Real_PPSF'].replace(0, np.nan))

# Drop any rows that resulted in NaNs during log transformation
plot_data = df.dropna(subset=['Log_BuildingArea', 'Log_Real_PPSF'])

# 7. Scatterplot: Log Real PPSF vs Log Building Area
plt.figure(figsize=(10, 6))
sns.scatterplot(x='Log_BuildingArea', y='Log_Real_PPSF', data=plot_data, alpha=0.3, s=15)

plt.title('Log Real Price Per Sq Ft vs. Log Building Area (Residential Only)')
plt.xlabel('Log Building Area')
plt.ylabel('Log Real Price Per Sq Ft')
plt.grid(True, linestyle='--', alpha=0.6)

plt.show() 

import pandas as pd

df = pd.read_csv('nyc_residential_cleaned.csv')

# Filter for the conditions
low_price_df = df[df['Price'] < 1000]
small_area_df = df[df['BuildingArea'] < 100]

# Print Results
print(f"Total records: {len(df)}")
print(f"Records with Price < $1,000: {len(low_price_df)}")
print(f"Records with Building Area < 100 sqft: {len(small_area_df)}")

if not low_price_df.empty:
    print("\nLow Price Examples:")
    print(low_price_df.head())
    
if not small_area_df.empty:
    print("\nSmall Area Examples:")
    print(small_area_df.head())

df = pd.read_csv('nyc_residential_cleaned.csv', parse_dates=['Date'])

df

#%%%
# Core KDE Class Definitions

# Original Class

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy.stats import gaussian_kde
from datetime import datetime
import gc


class ContinuousTimeKDE:
    def __init__(self, df, x_col, y_col, time_col, time_bandwidth_days=30):
        """
        Constructs a Time-Varying Density Object with Analytic Derivatives.
        """
        self.data = df[[x_col, y_col, time_col]].dropna().copy()
        
        self.start_date = self.data[time_col].min()
        self.data['time_numeric'] = (self.data[time_col] - self.start_date).dt.days
        
        self.x = self.data[x_col].values
        self.y = self.data[y_col].values
        self.t = self.data['time_numeric'].values
        
        # Bandwidth for time (sigma_t)
        self.sigma_t = time_bandwidth_days
        
    def _get_weighted_kde(self, t_query):
        """Helper to build the spatial KDE at a specific time."""
        # Temporal Weights: Gaussian Kernel on Time
        time_weights = np.exp(-0.5 * ((self.t - t_query) / self.sigma_t) ** 2)
        
        # Filter for speed
        mask = time_weights > 1e-4
        if mask.sum() < 10:
            return None, None, None, None
            
        x_subset = self.x[mask]
        y_subset = self.y[mask]
        w_subset = time_weights[mask]
        
        # Normalize weights for the spatial KDE
        w_subset_norm = w_subset / w_subset.sum()
        
        return x_subset, y_subset, w_subset, w_subset_norm

    def get_density_at_time(self, query_date, grid_x, grid_y):
        """
        Returns the 2D probability surface (Z) for a specific moment in time.
        Uses scipy.stats.gaussian_kde for speed (faster than analytic derivatives).
        """
        if isinstance(query_date, str):
            query_date = pd.to_datetime(query_date)
        t_query = (query_date - self.start_date).days
        
        # Use the helper to get weights
        x_sub, y_sub, w_raw, w_norm = self._get_weighted_kde(t_query)
        
        if x_sub is None:
            return np.zeros_like(grid_x)
        
        try:
            # Note: gaussian_kde automatically normalizes weights
            kernel = gaussian_kde([x_sub, y_sub], weights=w_raw)
        except np.linalg.LinAlgError:
            return np.zeros_like(grid_x)
        
        # Evaluate on grid
        positions = np.vstack([grid_x.ravel(), grid_y.ravel()])
        z = np.reshape(kernel(positions).T, grid_x.shape)
        
        return z

    def get_analytic_derivatives(self, query_date, grid_x, grid_y):
        """
        Calculates Density (p), Spatial Score (grad log p), and Time Derivative (dp/dt)
        ANALYTICALLY.
        
        Returns:
            Z (Density), 
            U (Grad X), V (Grad Y), 
            P_dot (Time Derivative dp/dt)
        """
        if isinstance(query_date, str):
            query_date = pd.to_datetime(query_date)
        t_query = (query_date - self.start_date).days
        
        x_sub, y_sub, w_raw, w_norm = self._get_weighted_kde(t_query)
        if x_sub is None:
            return np.zeros_like(grid_x), np.zeros_like(grid_x), np.zeros_like(grid_x), np.zeros_like(grid_x)

        # 1. Fit KDE to get Spatial Covariance
        try:
            kde = gaussian_kde([x_sub, y_sub], weights=w_norm)
        except np.linalg.LinAlgError:
            return np.zeros_like(grid_x), np.zeros_like(grid_x), np.zeros_like(grid_x), np.zeros_like(grid_x)

        inv_cov = np.linalg.inv(kde.covariance)
        
        # 2. Setup Grids for Broadcasting
        # Grid: (2, N_grid)
        grid_coords = np.vstack([grid_x.ravel(), grid_y.ravel()])
        
        # Data: (2, N_data)
        data_coords = np.vstack([x_sub, y_sub])
        
        # Difference: (2, N_grid, N_data)
        diff = grid_coords[:, :, None] - data_coords[:, None, :]
        
        # 3. Compute Spatial Kernel Components
        # Project diff onto precision matrix: Sigma^-1 (x - x_i)
        projected_diff = np.tensordot(inv_cov, diff, axes=(1, 0)) # (2, N_grid, N_data)
        
        # Exponent: -0.5 * (x-xi)^T Sigma^-1 (x-xi)
        exponent = -0.5 * np.sum(diff * projected_diff, axis=0)
        kernel_vals = np.exp(exponent) # (N_grid, N_data)
        
        # 4. Compute Density (p)
        weighted_kernels = kernel_vals * w_norm[None, :]
        density = np.sum(weighted_kernels, axis=1) # (N_grid,)
        
        # 5. Compute Spatial Gradient (dp/dx, dp/dy)
        grad_spatial = np.sum(-projected_diff * weighted_kernels[None, :, :], axis=2)
        
        # 6. Compute Time Derivative (dp/dt)
        # Re-calculate mask to get correct time subset logic
        raw_time_weights = np.exp(-0.5 * ((self.t - t_query) / self.sigma_t) ** 2)
        mask = raw_time_weights > 1e-4
        t_sub = self.t[mask]
        
        # Gradient of weights w.r.t time
        grad_W_factor = -(t_query - t_sub) / (self.sigma_t**2) 
        
        # Term 1: Unnormalized change
        term1 = np.sum(weighted_kernels * grad_W_factor[None, :], axis=1)
        
        # Term 2: Normalization drift (Ensures integral(dp/dt) = 0)
        w_subset = raw_time_weights[mask]
        sum_W = w_subset.sum()
        sum_dW = np.sum(w_subset * grad_W_factor)
        normalization_drift = sum_dW / sum_W
        
        term2 = density * normalization_drift
        
        p_dot = term1 - term2

        # 7. Reshape and Normalize Scores
        epsilon = 1e-12
        Z = density.reshape(grid_x.shape)
        P_dot = p_dot.reshape(grid_x.shape)
        
        U = (grad_spatial[0, :] / (density + epsilon)).reshape(grid_x.shape)
        V = (grad_spatial[1, :] / (density + epsilon)).reshape(grid_x.shape)
        
        # Mask noise
        mask_clean = Z < 1e-5
        U[mask_clean] = 0
        V[mask_clean] = 0
        P_dot[mask_clean] = 0
        
        return Z, U, V, P_dot


    def get_analytic_derivatives_blocked(kde_obj, query_date, grid_x, grid_y, chunk_size=500):
        """
        Memory-efficient wrapper that computes analytic derivatives in chunks.
        
        This avoids allocating the full (2, N_grid, N_data) intermediate array
        by processing grid points in batches.
        
        Parameters:
        -----------
        kde_obj : ContinuousTimeKDE
            The KDE object with necessary internal data
        query_date : datetime
            Date to query
        grid_x, grid_y : np.ndarray
            2D meshgrids
        chunk_size : int
            Number of grid points per chunk
            
        Returns:
        --------
        Z, grad_x, grad_y, grad_t : np.ndarray
            Density and gradients, same shape as grid_x
        """
        from scipy.stats import gaussian_kde
        
        if isinstance(query_date, str):
            query_date = pd.to_datetime(query_date)
        
        t_query = (query_date - kde_obj.start_date).days
        
        # Get weighted data subset
        time_weights = np.exp(-0.5 * ((kde_obj.t - t_query) / kde_obj.sigma_t) ** 2)
        mask = time_weights > 1e-4
        
        if mask.sum() < 10:
            return (np.zeros_like(grid_x), np.zeros_like(grid_x), 
                    np.zeros_like(grid_x), np.zeros_like(grid_x))
        
        x_sub = kde_obj.x[mask]
        y_sub = kde_obj.y[mask]
        t_sub = kde_obj.t[mask]
        w_raw = time_weights[mask]
        w_norm = w_raw / w_raw.sum()
        
        # Fit KDE to get covariance
        try:
            kde = gaussian_kde([x_sub, y_sub], weights=w_norm)
        except np.linalg.LinAlgError:
            return (np.zeros_like(grid_x), np.zeros_like(grid_x),
                    np.zeros_like(grid_x), np.zeros_like(grid_x))
        
        inv_cov = np.linalg.inv(kde.covariance)
        
        # Flatten grids for processing
        grid_flat_x = grid_x.ravel()
        grid_flat_y = grid_y.ravel()
        n_grid = len(grid_flat_x)
        n_data = len(x_sub)
        
        # Data coordinates
        data_x = x_sub
        data_y = y_sub
        
        # Initialize output arrays (flat)
        density = np.zeros(n_grid)
        grad_x_flat = np.zeros(n_grid)
        grad_y_flat = np.zeros(n_grid)
        grad_t_flat = np.zeros(n_grid)
        
        # Time derivative components
        grad_W_factor = -(t_query - t_sub) / (kde_obj.sigma_t**2)
        sum_W = w_raw.sum()
        sum_dW = np.sum(w_raw * grad_W_factor)
        normalization_drift = sum_dW / sum_W
        
        # Process in chunks to save memory
        for start_idx in range(0, n_grid, chunk_size):
            end_idx = min(start_idx + chunk_size, n_grid)
            chunk_size_actual = end_idx - start_idx
            
            # Grid chunk
            gx_chunk = grid_flat_x[start_idx:end_idx]
            gy_chunk = grid_flat_y[start_idx:end_idx]
            
            # Compute differences: (chunk_size, n_data)
            diff_x = gx_chunk[:, None] - data_x[None, :]
            diff_y = gy_chunk[:, None] - data_y[None, :]
            
            # Project onto precision matrix: Sigma^-1 @ [dx, dy]
            proj_x = inv_cov[0, 0] * diff_x + inv_cov[0, 1] * diff_y
            proj_y = inv_cov[1, 0] * diff_x + inv_cov[1, 1] * diff_y
            
            # Exponent: -0.5 * (x-xi)^T Sigma^-1 (x-xi)
            exponent = -0.5 * (diff_x * proj_x + diff_y * proj_y)
            kernel_vals = np.exp(exponent)  # (chunk_size, n_data)
            
            # Weighted kernels
            weighted_kernels = kernel_vals * w_norm[None, :]
            
            # Density
            density[start_idx:end_idx] = np.sum(weighted_kernels, axis=1)
            
            # Spatial gradients: Sum[ w_i * K_i * (-Sigma^-1(x-xi)) ]
            grad_x_flat[start_idx:end_idx] = np.sum(-proj_x * weighted_kernels, axis=1)
            grad_y_flat[start_idx:end_idx] = np.sum(-proj_y * weighted_kernels, axis=1)
            
            # Time derivative
            term1 = np.sum(weighted_kernels * grad_W_factor[None, :], axis=1)
            term2 = density[start_idx:end_idx] * normalization_drift
            grad_t_flat[start_idx:end_idx] = term1 - term2
            
            # Force garbage collection between chunks
            del diff_x, diff_y, proj_x, proj_y, exponent, kernel_vals, weighted_kernels
            gc.collect()
        
        # Reshape back to grid shape
        Z = density.reshape(grid_x.shape)
        grad_x_out = grad_x_flat.reshape(grid_x.shape)
        grad_y_out = grad_y_flat.reshape(grid_x.shape)
        grad_t_out = grad_t_flat.reshape(grid_x.shape)
        
        return Z, grad_x_out, grad_y_out, grad_t_out

#%% 
"""
GPU Accelerated KDE Class *If your machine doesn't have an NVIDIA GPU, you cannot run this. Running the bandwith sweep without this
would take a very long amount of time. How long? I have no clue, it took so long I had to make another version of the core class just to 
accomplish the sweep in a timely manner. The results of the sweep can be found in my paper, and the optimal bandwith from this process is 
used throughout this research.*
"""

import torch
import numpy as np
import pandas as pd
import gc

class ContinuousTimeKDE:
    def __init__(self, df, x_col, y_col, time_col, time_bandwidth_days=30, device=None):
        """
        GPU-Accelerated Time-Varying Density Object. I Also call this a Madelung object, after 
        the Madelung qeuations describing probability fluid
        """
        # 1. Setup Device
        if device:
            self.device = torch.device(device)
        else:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        print(f"Initialized GPU-KDE on: {self.device}")

        # 2. Prepare Data
        # Copy to avoid SettingWithCopy warnings
        self.data = df[[x_col, y_col, time_col]].dropna().copy()
        
        self.start_date = self.data[time_col].min()
        self.data['time_numeric'] = (self.data[time_col] - self.start_date).dt.days
        
        # 3. Move Data to GPU Tensors
        # Use float32 for speed; use float64 if extreme precision is needed
        self.x = torch.tensor(self.data[x_col].values, dtype=torch.float32, device=self.device)
        self.y = torch.tensor(self.data[y_col].values, dtype=torch.float32, device=self.device)
        self.t = torch.tensor(self.data['time_numeric'].values, dtype=torch.float32, device=self.device)
        
        self.sigma_t = time_bandwidth_days
    
    def _get_weighted_kde(self, t_query):
        """Helper to build the spatial KDE at a specific time."""
        # Temporal Weights: Gaussian Kernel on Time (use torch, not numpy)
        time_weights = torch.exp(-0.5 * ((self.t - t_query) / self.sigma_t) ** 2)
        
        # Filter for speed
        mask = time_weights > 1e-4
        if mask.sum() < 10:
            return None, None, None, None
            
        x_subset = self.x[mask]
        y_subset = self.y[mask]
        w_subset = time_weights[mask]
        
        # Normalize weights for the spatial KDE
        w_subset_norm = w_subset / w_subset.sum()
        
        return x_subset, y_subset, w_subset, w_subset_norm
  
    def get_density_at_time(self, query_date, grid_x, grid_y):
        if isinstance(query_date, str):
            query_date = pd.to_datetime(query_date)
        t_query = (query_date - self.start_date).days
        
        x_sub, y_sub, w_raw, w_norm = self._get_weighted_kde(t_query)
        
        if x_sub is None:
            return np.zeros_like(grid_x)
        
        try:
            # Move to CPU for scipy
            kernel = gaussian_kde(
                [x_sub.cpu().numpy(), y_sub.cpu().numpy()], 
                weights=w_raw.cpu().numpy()
            )
        except np.linalg.LinAlgError:
            return np.zeros_like(grid_x)
        
        positions = np.vstack([grid_x.ravel(), grid_y.ravel()])
        z = np.reshape(kernel(positions).T, grid_x.shape)
        
        return z
        
    def get_analytic_derivatives(self, query_date, grid_x, grid_y, batch_size=10000):
        """
        Calculates Density (Z), Velocity (U, V), and Time Derivative (P_dot) on the GPU.
        
        Parameters:
        -----------
        query_date : datetime or str
        grid_x, grid_y : np.ndarray (CPU meshgrids)
        batch_size : int
            Number of grid points to process at once. 10k is usually safe for modern GPUs.
            
        Returns:
        --------
        Z, U, V, P_dot : np.ndarray (CPU arrays)
        """
        # --- 1. Setup Query Time ---
        if isinstance(query_date, str):
            query_date = pd.to_datetime(query_date)
        t_query = (query_date - self.start_date).days
        
        # --- 2. Calculate Time Weights (Gaussian) ---
        # w = exp(-0.5 * ((t - tq) / sigma)^2)
        # Done entirely on GPU
        weights = torch.exp(-0.5 * ((self.t - t_query) / self.sigma_t) ** 2)
        
        # Filter: Ignore weights < 1e-4 for efficiency
        mask = weights > 1e-4
        if mask.sum() < 10:
            return (np.zeros_like(grid_x), np.zeros_like(grid_x), 
                    np.zeros_like(grid_x), np.zeros_like(grid_x))
            
        # Extract Active Subset
        x_sub = self.x[mask]
        y_sub = self.y[mask]
        t_sub = self.t[mask]
        w_sub = weights[mask]
        
        # Normalize weights
        w_norm = w_sub / w_sub.sum()
        
        # --- 3. Estimate Weighted Covariance Matrix (GPU) ---
        # Neff = 1 / sum(w^2)
        neff = 1.0 / (w_norm ** 2).sum()
        
        # Weighted Means
        mean_x = (x_sub * w_norm).sum()
        mean_y = (y_sub * w_norm).sum()
        
        # Centered Data
        xc = x_sub - mean_x
        yc = y_sub - mean_y
        
        # Weighted Covariance (Reliability Weights formula)
        # Cov = (w * xc * xc^T) / (1 - sum(w^2))
        denom = 1.0 - (w_norm ** 2).sum()
        cov_xx = (w_norm * xc * xc).sum() / denom
        cov_yy = (w_norm * yc * yc).sum() / denom
        cov_xy = (w_norm * xc * yc).sum() / denom
        
        # Scott's Rule Factor
        # D=2 => factor = neff^(-1/6)
        factor = neff ** (-1.0 / 6.0)
        
        # Kernel Covariance = Data Cov * Factor^2
        k_cov_xx = cov_xx * (factor ** 2)
        k_cov_yy = cov_yy * (factor ** 2)
        k_cov_xy = cov_xy * (factor ** 2)
        
        # Precision Matrix (Inverse of Covariance)
        det = k_cov_xx * k_cov_yy - k_cov_xy**2
        if det <= 1e-12:
            return (np.zeros_like(grid_x), np.zeros_like(grid_x), 
                    np.zeros_like(grid_x), np.zeros_like(grid_x))
            
        inv_det = 1.0 / det
        prec_xx = k_cov_yy * inv_det
        prec_yy = k_cov_xx * inv_det
        prec_xy = -k_cov_xy * inv_det
        
        # --- 4. Batch Processing Setup ---
        # Flatten Grid & Move to GPU
        gx_flat = torch.tensor(grid_x.ravel(), dtype=torch.float32, device=self.device)
        gy_flat = torch.tensor(grid_y.ravel(), dtype=torch.float32, device=self.device)
        n_grid = len(gx_flat)
        
        # Initialize Output Tensors
        density_out = torch.zeros(n_grid, device=self.device)
        grad_x_out = torch.zeros(n_grid, device=self.device)
        grad_y_out = torch.zeros(n_grid, device=self.device)
        p_dot_out = torch.zeros(n_grid, device=self.device)
        
        # Time Derivative Constants
        # grad_w_factor = -(t - t_sub)/sigma^2
        grad_w_factor_sub = -(t_query - t_sub) / (self.sigma_t**2)
        
        # Drift Term: sum(w * grad_w_factor) / sum(w)
        sum_dw = (w_sub * grad_w_factor_sub).sum()
        sum_w = w_sub.sum()
        norm_drift = sum_dw / sum_w

        # --- 5. Batched Execution ---
        for start in range(0, n_grid, batch_size):
            end = min(start + batch_size, n_grid)
            
            # Grid Chunk: (Batch, 1)
            gx_chunk = gx_flat[start:end].unsqueeze(1)
            gy_chunk = gy_flat[start:end].unsqueeze(1)
            
            # Difference Vectors: (Batch, N_data)
            # Automatic broadcasting: (Batch, 1) - (1, N) -> (Batch, N)
            dx = gx_chunk - x_sub.unsqueeze(0)
            dy = gy_chunk - y_sub.unsqueeze(0)
            
            # Mahalanobis Distance Squared
            # dist = dx*(dx*Pxx + dy*Pxy) + dy*(dx*Pxy + dy*Pyy)
            # Pre-calculate projected components for efficiency
            proj_x = dx * prec_xx + dy * prec_xy
            proj_y = dx * prec_xy + dy * prec_yy
            
            dist_sq = dx * proj_x + dy * proj_y
            
            # Kernel Values: exp(-0.5 * dist_sq)
            kernel_vals = torch.exp(-0.5 * dist_sq)
            
            # Apply Weights
            weighted_k = kernel_vals * w_norm.unsqueeze(0)
            
            # --- Accumulate Results ---
            
            # Density (Z)
            z_chunk = weighted_k.sum(dim=1)
            density_out[start:end] = z_chunk
            
            # Spatial Gradients
            # dK/dx = K * (-Sigma^-1 * x) = K * (-proj_x)
            grad_x_out[start:end] = (-proj_x * weighted_k).sum(dim=1)
            grad_y_out[start:end] = (-proj_y * weighted_k).sum(dim=1)
            
            # Time Derivative (dp/dt)
            # term1 = Sum(K * w_norm * grad_w_factor)
            # term2 = Density * drift
            term1 = (weighted_k * grad_w_factor_sub.unsqueeze(0)).sum(dim=1)
            p_dot_out[start:end] = term1 - (z_chunk * norm_drift)
            
            # Clean up VRAM
            del dx, dy, proj_x, proj_y, dist_sq, kernel_vals, weighted_k, z_chunk, term1
            
        # --- 6. Finalize & Return to CPU ---
        Z = density_out.reshape(grid_x.shape)
        epsilon = 1e-12
        
        # Velocity Fields (U, V) = Grad P / P
        U = (grad_x_out / (density_out + epsilon)).reshape(grid_x.shape)
        V = (grad_y_out / (density_out + epsilon)).reshape(grid_x.shape)
        P_dot = p_dot_out.reshape(grid_x.shape)
        
        # Clean noise
        mask_clean = Z < 1e-5
        U[mask_clean] = 0
        V[mask_clean] = 0
        P_dot[mask_clean] = 0
        
        return (Z.cpu().numpy(), U.cpu().numpy(), 
                V.cpu().numpy(), P_dot.cpu().numpy()) 
    
#%% Instantiation Block

filename = 'nyc_residential_cleaned.csv'

df_clean = pd.read_csv(filename, parse_dates=['Date'])

# 3. Instantiate the 4D Density Object
kde_4d = ContinuousTimeKDE(df_clean, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=52)

# Grid Setup
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50) # 50x50 grid for speed
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='14D')

#%% Model Saving block
import joblib

print("Saving 4D KDE object to disk...")
joblib.dump(kde_4d, 'nyc_housing_kde_model.joblib')
print("Save complete.")

#%% Bandwith Optimization 

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from sklearn.metrics import r2_score
from joblib import Parallel, delayed
from tqdm import tqdm  # Import tqdm

# --- Helper Function for Parallel Workers ---
def analyze_single_bandwidth(bw, df, dates, grid_x, grid_y):
    """
    Worker function to test a single time bandwidth.
    Instantiates its own KDE model to avoid pickling issues.
    """
    try:
        # 1. Instantiate KDE for this specific bandwidth
        kde = ContinuousTimeKDE(df, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=bw)
        
        all_c, all_m = [], []
        
        # 2. Collect Phase Space Data (High Res Loop)
        for d in dates:
            Z, U, V, _ = kde.get_analytic_derivatives(d, grid_x, grid_y)
            
            c = Z.flatten()
            m = np.sqrt(U**2 + V**2).flatten()
            
            mask = c > 1e-5
            all_c.append(c[mask])
            all_m.append(m[mask])
            
        if not all_c: return None
        
        univ_c = np.concatenate(all_c)
        univ_m = np.concatenate(all_m)
        
        # 3. Extract Frontier 
        bins = np.linspace(univ_c.min(), univ_c.max(), 50)
        digitized = np.digitize(univ_c, bins)
        
        fx, fy = [], []
        
        for i in range(1, len(bins)):
            mask = digitized == i
            if np.any(mask):
                max_val = np.percentile(univ_m[mask], 99)
                mid_x = (bins[i-1] + bins[i]) / 2
                fx.append(mid_x)
                fy.append(max_val)
        
        if len(fx) < 5: return None 
        
        # 4. Measure Signal Strength
        signal_strength = np.mean(sorted(fy)[-5:]) 
        
        # 5. Measure Model Fit (Exponential Decay)
        def exp_decay(x, a, b, floor): 
            return a * np.exp(-b * x) + floor
        
        p0 = [np.max(fy), 1000, 1]
        
        try:
            popt, _ = curve_fit(exp_decay, fx, fy, p0=p0, maxfev=2000, bounds=(0, np.inf))
            y_pred = exp_decay(np.array(fx), *popt)
            r2 = r2_score(fy, y_pred)
        except:
            r2 = 0 
            
        return {
            'bw': bw,
            'r2': r2,
            'signal': signal_strength,
            'fragility': popt[1] if r2 > 0 else 0
        }
        
    except Exception as e:
        return None

# --- Main Execution Function ---

def find_characteristic_time_scale_gpu(df, start_date, end_date):
    """
    Optimized bandwidth search using GPU acceleration.
    Runs sequentially to avoid VRAM contention.
    """
    # 1. Setup High-Res Grid [Discretization] 
    x_g = np.linspace(df['Log_BuildingArea'].min(), df['Log_BuildingArea'].max(), 50)
    y_g = np.linspace(df['Log_Real_PPSF'].min(), df['Log_Real_PPSF'].max(), 50)
    X, Y = np.meshgrid(x_g, y_g)
    
    # 2. Setup Dates
    dates = pd.date_range(start=start_date, end=end_date, freq='14D')
    
    # 3. Initialize GPU KDE *ONCE*
    # Load data to VRAM one time. Update sigma_t inside the loop.
    print("Initializing GPU KDE...")
    kde = ContinuousTimeKDE(df, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=30)
    
    bandwidths = range(15, 91)
    results = []

    print(f"Starting GPU Optimization...")
    print(f"Grid: 50x50 | Time Steps: {len(dates)} | Bandwidths: {len(bandwidths)}")
    
    # 4. Sequential Loop (The GPU makes this fast enough to not need Parallel)
    for bw in tqdm(bandwidths, desc="Optimizing Bandwidth"):
        
        # Update bandwidth on the existing object
        kde.sigma_t = bw 
        
        all_c, all_m = [], []
        
        # --- Collect Phase Space Data ---
        valid_bw = True
        for d in dates:
            # GPU Call
            Z, U, V, _ = kde.get_analytic_derivatives(d, X, Y)
            
            c = Z.flatten()
            m = np.sqrt(U**2 + V**2).flatten()
            
            # Filter noise
            mask = c > 1e-5
            if np.any(mask):
                all_c.append(c[mask])
                all_m.append(m[mask])
        
        if not all_c:
            continue
            
        univ_c = np.concatenate(all_c)
        univ_m = np.concatenate(all_m)
        
        # --- Extract Frontier ---
        bins = np.linspace(univ_c.min(), univ_c.max(), 50)
        digitized = np.digitize(univ_c, bins)
        
        fx, fy = [], []
        for i in range(1, len(bins)):
            mask = digitized == i
            if np.any(mask):
                max_val = np.percentile(univ_m[mask], 99)
                mid_x = (bins[i-1] + bins[i]) / 2
                fx.append(mid_x)
                fy.append(max_val)
                
        if len(fx) < 5:
            continue

        # --- Measure Metrics ---
        # 1. Signal Strength (Velocity at high density)
        signal_strength = np.mean(sorted(fy)[-5:])
        
        # 2. Model Fit (Exponential Decay)
        def exp_decay(x, a, b, floor): 
            return a * np.exp(-b * x) + floor
        
        try:
            p0 = [np.max(fy), 1000, 1]
            popt, _ = curve_fit(exp_decay, fx, fy, p0=p0, maxfev=2000, bounds=(0, np.inf))
            y_pred = exp_decay(np.array(fx), *popt)
            r2 = r2_score(fy, y_pred)
        except:
            r2 = 0
            
        results.append({
            'bw': bw,
            'r2': r2,
            'signal': signal_strength
        })

    # --- Process Results ---
    if not results:
        print("No valid results found.")
        return None
        
    df_res = pd.DataFrame(results).sort_values('bw')
    
    # Normalize
    r2_range = df_res['r2'].max() - df_res['r2'].min()
    sig_range = df_res['signal'].max() - df_res['signal'].min()
    
    df_res['r2_norm'] = (df_res['r2'] - df_res['r2'].min()) / r2_range if r2_range > 0 else 0
    df_res['signal_norm'] = (df_res['signal'] - df_res['signal'].min()) / sig_range if sig_range > 0 else 0
    df_res['score'] = df_res['r2_norm'] + df_res['signal_norm']
    
    best_bw = df_res.loc[df_res['score'].idxmax(), 'bw']
    
    # --- Visualization ---
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    color = 'tab:blue'
    ax1.set_xlabel('Bandwidth (Days)', fontsize=12)
    ax1.set_ylabel('Model Fit ($R^2$)', color=color, fontsize=12)
    ax1.plot(df_res['bw'], df_res['r2'], color=color, linewidth=3, label='Fit Quality')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, alpha=0.3)
    
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Signal Strength', color=color, fontsize=12)
    ax2.plot(df_res['bw'], df_res['signal'], color=color, linewidth=3, linestyle='--', label='Signal')
    ax2.tick_params(axis='y', labelcolor=color)
    
    plt.axvline(best_bw, color='green', linestyle=':', linewidth=3, label=f'Optimal: {best_bw} Days')
    plt.title(f"Market Memory Optimization (GPU)\nOptimal Scale: {best_bw} Days")
    plt.show()
    
    print(f"Optimal Bandwidth: {best_bw} Days")
    return int(best_bw)

# --- Execution ---

optimal_bw = find_characteristic_time_scale_gpu(
    df, 
    start_date=df_clean['Date'].min(), 
    end_date=df_clean['Date'].max()
)

#%% Bandwith Diagnostic check

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm  # Import colormap

def bandwidth_diagnostic(df, bandwidths=[43, 52, 65], test_date='2018-06-01', 
                          date_range=('2015-01-01', '2022-01-01')):
    """
    Diagnostic plots to distinguish genuine market timescale from estimator artifact.
    """
    
    # --- Setup Grid ---
    x_g = np.linspace(df['Log_BuildingArea'].min(), df['Log_BuildingArea'].max(), 50)
    y_g = np.linspace(df['Log_Real_PPSF'].min(), df['Log_Real_PPSF'].max(), 50)
    X, Y = np.meshgrid(x_g, y_g)
    
    dates = pd.date_range(start=date_range[0], end=date_range[1], freq='14D')
    test_date = pd.to_datetime(test_date)
    
    # Storage for results
    frontiers = {}
    snapshots = {}
    
    # --- Collect Data for Each Bandwidth ---
    for bw in bandwidths:
        print(f"Processing bandwidth: {bw} days...")
        
        kde = ContinuousTimeKDE(df, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', 
                                time_bandwidth_days=bw)
        
        # --- Frontier: Aggregate across all dates ---
        all_c, all_m = [], []
        
        for d in dates:
            Z, U, V, _ = kde.get_analytic_derivatives(d, X, Y)
            
            c = Z.flatten()
            m = np.sqrt(U**2 + V**2).flatten()
            
            mask = c > 1e-5
            if np.any(mask):
                all_c.append(c[mask])
                all_m.append(m[mask])
        
        if not all_c:
            frontiers[bw] = ([], [])
            continue

        univ_c = np.concatenate(all_c)
        univ_m = np.concatenate(all_m)
        
        # Extract frontier 
        bins = np.linspace(univ_c.min(), univ_c.max(), 50)
        digitized = np.digitize(univ_c, bins)
        
        fx, fy = [], []
        for i in range(1, len(bins)):
            mask = digitized == i
            if np.sum(mask) > 10:  # Require minimum samples
                fx.append((bins[i-1] + bins[i]) / 2)
                fy.append(np.percentile(univ_m[mask], 99))
        
        frontiers[bw] = (np.array(fx), np.array(fy))
        
        # --- Snapshot: Single date for spatial comparison ---
        Z, U, V, _ = kde.get_analytic_derivatives(test_date, X, Y)
        snapshots[bw] = {
            'Z': Z,
            'U': U,
            'V': V,
            'mag': np.sqrt(U**2 + V**2)
        }
    
    # PLOT 1: Overlaid Frontier Curves
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # --- Dynamic Colors ---
    # Generate distinct colors for however many bandwidths are passed
    color_map = cm.get_cmap('viridis', len(bandwidths))
    
    for i, bw in enumerate(bandwidths):
        fx, fy = frontiers[bw]
        # Get color by index
        color = color_map(i)
        
        ax.plot(fx, fy, color=color, linewidth=2.5, label=f'{bw} days', alpha=0.8)
        ax.scatter(fx, fy, color=color, s=20, alpha=0.5)
    
    ax.set_xlabel('Density', fontsize=12)
    ax.set_ylabel('Velocity', fontsize=12)
    ax.set_title('Frontier Curves: Do They Preserve Shape?', fontsize=14)
    ax.legend(title='Bandwidth')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # PLOT 2: Velocity Magnitude Heatmaps
    fig, axes = plt.subplots(1, len(bandwidths), figsize=(5 * len(bandwidths), 5))
    if len(bandwidths) == 1: axes = [axes] # Handle single bandwidth case
    
    # Find common color scale
    vmax = max(snapshots[bw]['mag'].max() for bw in bandwidths)
    
    for ax, bw in zip(axes, bandwidths):
        mag = snapshots[bw]['mag']
        Z = snapshots[bw]['Z']
        
        # Mask low-density regions
        mag_masked = np.where(Z > 1e-5, mag, np.nan)
        
        im = ax.pcolormesh(X, Y, mag_masked, shading='auto', cmap='inferno', 
                           vmin=0, vmax=vmax)
        ax.set_xlabel('Log Building Area')
        ax.set_ylabel('Log Real PPSF')
        ax.set_title(f'{bw} Days')
        
    fig.colorbar(im, ax=axes, label='Velocity Magnitude', shrink=0.8)
    fig.suptitle(f'Velocity Magnitude at {test_date.strftime("%Y-%m-%d")}: Spatial Consistency?', 
                 fontsize=14)
    
    plt.tight_layout()
    plt.show()
    
    
    # PLOT 3: Quiver Plots (Flow Direction)
    fig, axes = plt.subplots(1, len(bandwidths), figsize=(5 * len(bandwidths), 5))
    if len(bandwidths) == 1: axes = [axes]

    # Subsample for readability
    skip = 3
    X_sub = X[::skip, ::skip]
    Y_sub = Y[::skip, ::skip]
    
    for ax, bw in zip(axes, bandwidths):
        U = snapshots[bw]['U'][::skip, ::skip]
        V = snapshots[bw]['V'][::skip, ::skip]
        Z = snapshots[bw]['Z'][::skip, ::skip]
        mag = np.sqrt(U**2 + V**2)
        
        # Mask and normalize arrows
        mask = Z > 1e-5
        U_norm = np.where(mask, U / (mag + 1e-12), 0)
        V_norm = np.where(mask, V / (mag + 1e-12), 0)
        
        # Color by magnitude
        ax.quiver(X_sub, Y_sub, U_norm, V_norm, mag,
                  cmap='viridis', scale=25, width=0.004, alpha=0.8)
        
        # Density contours for context
        ax.contour(X, Y, snapshots[bw]['Z'], levels=5, colors='gray', 
                   linewidths=0.5, alpha=0.5)
        
        ax.set_xlabel('Log Building Area')
        ax.set_ylabel('Log Real PPSF')
        ax.set_title(f'{bw} Days')
    
    fig.suptitle(f'Flow Direction at {test_date.strftime("%Y-%m-%d")}: Directional Agreement?', 
                 fontsize=14)
    
    plt.show()
    
    # QUANTITATIVE CHECK: Direction Cosine Similarity
    print("\n" + "="*50)
    print("DIRECTIONAL AGREEMENT (Cosine Similarity)")
    print("="*50)
    
    for i, bw1 in enumerate(bandwidths):
        for bw2 in bandwidths[i+1:]:
            U1, V1 = snapshots[bw1]['U'], snapshots[bw1]['V']
            U2, V2 = snapshots[bw2]['U'], snapshots[bw2]['V']
            Z1, Z2 = snapshots[bw1]['Z'], snapshots[bw2]['Z']
            
            # Only compare where both have signal
            mask = (Z1 > 1e-5) & (Z2 > 1e-5)
            
            if mask.sum() == 0:
                print(f"{bw1} vs {bw2} days: No Overlap")
                continue

            # Cosine similarity per point
            dot = U1[mask] * U2[mask] + V1[mask] * V2[mask]
            mag1 = np.sqrt(U1[mask]**2 + V1[mask]**2) + 1e-12
            mag2 = np.sqrt(U2[mask]**2 + V2[mask]**2) + 1e-12
            
            cos_sim = dot / (mag1 * mag2)
            
            print(f"{bw1} vs {bw2} days:")
            print(f"  Mean cosine similarity: {cos_sim.mean():.3f}")
            print(f"  Fraction agreeing (cos > 0.5): {(cos_sim > 0.5).mean():.1%}")
            print()
    
    return frontiers, snapshots 

frontiers, snapshots = bandwidth_diagnostic(
    df, 
    bandwidths=[43, 52, 60, 70],
    test_date='2018-06-01'
)

#%% Heisenberg Regressions

# This method can plot the fronteir of any availible date

import matplotlib.pyplot as plt
import seaborn as sns

def plot_heisenberg_uncertainty(kde, query_date, grid_x, grid_y):
    # 1. Get the Fields
    Z, U, V, P_dot = kde.get_analytic_derivatives(query_date, grid_x, grid_y)
    
    # 2. Calculate Observables
    # Fisher Information (Precision of Price) ~ Density * |Grad Log P|^2
    # In information geometry, Fisher Info metric approximates the Hessian at peaks.
    # Use the Density (Z) itself as a proxy for "Market Consensus/Certainty"
    certainty = Z.flatten()
    
    # Momentum (Speed of Price Discovery)
    momentum = np.sqrt(U**2 + V**2).flatten()
    
    # 3. Visualization
    plt.figure(figsize=(10, 6))
    
    # Filter out empty space noise
    mask = certainty > 1e-5
    
    plt.scatter(certainty[mask], momentum[mask], 
                c=momentum[mask], cmap='inferno', alpha=0.5, s=10)
    
    plt.title(f"The Real Estate 'Heisenberg' Limit ({query_date.date()})")
    plt.xlabel("Price Certainty (Density/Consensus) $\\rightarrow$")
    plt.ylabel("Liquidity Velocity (Score Magnitude) $\\uparrow$")
    plt.grid(True, alpha=0.3)
    
    # Annotation
    plt.annotate('THE ATTRACTOR\n(Stable, but stuck)', 
                 xy=(certainty.max(), 0), 
                 xytext=(certainty.max()*0.6, momentum.max()*0.2),
                 arrowprops=dict(facecolor='black', shrink=0.05))
                 
    plt.annotate('THE RAPID\n(Fast, but uncertain)', 
                 xy=(0, momentum.max()), 
                 xytext=(certainty.max()*0.1, momentum.max()*0.8),
                 arrowprops=dict(facecolor='black', shrink=0.05))

    plt.show()

plot_heisenberg_uncertainty(kde_4d, pd.to_datetime('2020-01-01'), X, Y)

#%% Plots the frontier over all data through time (super positioned)

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

def plot_heisenberg_exponential_frontier(kde, start_date, end_date, grid_x, grid_y, months_step=3):
    """
    Fits a smooth exponential limit to the Universal Phase Space and reports goodness-of-fit.
    """
    # 1. Aggregation
    dates = pd.date_range(start=start_date, end=end_date, freq=f'{months_step}MS')
    print(f"Aggregating Phase Space data for {len(dates)} timestamps...")
    
    all_certainty = []
    all_momentum = []
    
    for d in dates:
        Z, U, V, _ = kde.get_analytic_derivatives(d, grid_x, grid_y)
        c = Z.flatten()
        m = np.sqrt(U**2 + V**2).flatten()
        
        mask = c > 1e-5
        all_certainty.append(c[mask])
        all_momentum.append(m[mask])
        
    univ_certainty = np.concatenate(all_certainty)
    univ_momentum = np.concatenate(all_momentum)
    
    # 2. Extracting the Empirical Hard Edge
    bins = np.linspace(univ_certainty.min(), univ_certainty.max(), 50)
    digitized = np.digitize(univ_certainty, bins)
    
    frontier_x = []
    frontier_y = []
    
    for i in range(1, len(bins)):
        mask = digitized == i
        if np.any(mask):
            max_val = np.max(univ_momentum[mask])
            mid_x = (bins[i-1] + bins[i]) / 2
            frontier_x.append(mid_x)
            frontier_y.append(max_val)
            
    # Convert to numpy arrays for vectorized math
    x_data = np.array(frontier_x)
    y_data = np.array(frontier_y)

    # 3. Curve Fitting & Metrics
    def exponential_decay(x, a, b, c):
        return a * np.exp(-b * x) + c

    # Initial guess
    p0 = [np.max(y_data), 50, 0]
    
    try:
        popt, pcov = curve_fit(exponential_decay, x_data, y_data, p0=p0, maxfev=5000)
        
        # --- CALCULATE METRICS ---
        y_pred = exponential_decay(x_data, *popt)
        residuals = y_data - y_pred
        
        # Sum of Squares Residuals (SS_res)
        ss_res = np.sum(residuals**2)
        
        # Total Sum of Squares (SS_tot)
        ss_tot = np.sum((y_data - np.mean(y_data))**2)
        
        # R-squared
        r_squared = 1 - (ss_res / ss_tot)
        
        # RMSE (Root Mean Square Error)
        rmse = np.sqrt(np.mean(residuals**2))
        
        print("-" * 30)
        print("FRONTIER FIT METRICS")
        print("-" * 30)
        print(f"Equation:  v = {popt[0]:.2f} * e^(-{popt[1]:.2f} * c) + {popt[2]:.2f}")
        print(f"R-squared: {r_squared:.4f}")
        print(f"RMSE:      {rmse:.4f}")
        print("-" * 30)

        # Generate smooth line for plotting
        x_smooth = np.linspace(min(x_data), max(x_data), 200)
        y_smooth = exponential_decay(x_smooth, *popt)
        
        # Updated Label with R^2
        label_str = f'Fit ($R^2={r_squared:.2f}$): $v = {popt[0]:.1f}e^{{-{popt[1]:.1f}c}} + {popt[2]:.2f}$'
        
    except RuntimeError:
        print("Curve fit failed to converge.")
        x_smooth = x_data
        y_smooth = y_data
        label_str = "Empirical Max (Fit Failed)"

    # 4. Visualization
    fig, ax = plt.subplots(figsize=(14, 10))
    
    hb = ax.hexbin(
        univ_certainty, 
        univ_momentum, 
        gridsize=60, 
        cmap='inferno', 
        bins='log',
        mincnt=1,
        linewidths=0.1
    )
    
    ax.plot(x_smooth, y_smooth, color='cyan', linestyle='-', linewidth=3, label=label_str)

    ax.set_title(f"Phase Space of Certainty for NYC Real Estate\n(Exponential Efficiency Frontier)", fontsize=16)
    ax.set_xlabel("Price Certainty (Density) $\\rightarrow$", fontsize=12)
    ax.set_ylabel("Liquidity Velocity (Score Magnitude) $\\uparrow$", fontsize=12)
    
    cb = fig.colorbar(hb, ax=ax)
    cb.set_label('Frequency of Occurrence (Log Scale)', rotation=270, labelpad=15)
    
    ax.legend(loc='upper right', framealpha=0.9, facecolor='white', edgecolor='black', fontsize=12)
    plt.grid(True, alpha=0.15)
    plt.show()

# --- Execution ---
plot_heisenberg_exponential_frontier(
    kde_4d, 
    start_date=df_clean['Date'].min(), 
    end_date=df_clean['Date'].max(), 
    grid_x=x_grid, 
    grid_y=y_grid 
) 

#%% Plots the fronteirs for each burough

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from sklearn.metrics import r2_score

# --- Configuration ---
BOROUGH_COLORS = {
    'manhattan': '#1f77b4', # Blue
    'brooklyn':  '#ff7f0e', # Orange
    'queens':    '#2ca02c', # Green
    'bronx':     '#d62728', # Red
    'statenisland': '#9467bd' # Purple
}

# The Standard Observer (Locked Bandwidth)
GLOBAL_BANDWIDTH = 52 

def analyze_borough_frontiers(df_all, start_date, end_date):
    """
    Fits the Heisenberg Efficiency Frontier for EACH borough individually
    using the fixed 'Standard Observer' bandwidth.
    """
    results = {}
    
    # Setup Plot
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Iterate through each Borough
    for borough, color in BOROUGH_COLORS.items():
        print(f"\nAnalyzing {borough}...")
        
        # 1. Filter Data
        df_boro = df_all[df_all['Boro_Source'] == borough].copy()
        
        if len(df_boro) < 1000:
            print(f"Skipping {borough} (Not enough data)")
            continue
            
        # 2. Instantiate KDE (Standard Observer)
        # Use the SAME bandwidth for everyone to ensure fair comparison
        kde_boro = ContinuousTimeKDE(df_boro, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=GLOBAL_BANDWIDTH)
        
        # 3. Grid Setup (Dynamic to fit the borough's specific size/price range)
        # We need a tight grid to see the frontier clearly
        x_g = np.linspace(df_boro['Log_BuildingArea'].min(), df_boro['Log_BuildingArea'].max(), 50)
        y_g = np.linspace(df_boro['Log_Real_PPSF'].min(), df_boro['Log_Real_PPSF'].max(), 50)
        X, Y = np.meshgrid(x_g, y_g)
        
        # 4. Collect Phase Space Data
        # Sampling every 14 days (High Res)
        dates = pd.date_range(start=start_date, end=end_date, freq='14D')
        
        all_c, all_m = [], []
        
        for d in dates:
            Z, U, V, _ = kde_boro.get_analytic_derivatives(d, X, Y)
            c = Z.flatten()
            m = np.sqrt(U**2 + V**2).flatten()
            
            mask = c > 1e-5
            all_c.append(c[mask])
            all_m.append(m[mask])
            
        if not all_c: continue
        
        univ_c = np.concatenate(all_c)
        univ_m = np.concatenate(all_m)
        
        # 5. Extract Empirical Frontier (Robust 99th Percentile)
        bins = np.linspace(univ_c.min(), univ_c.max(), 50)
        digitized = np.digitize(univ_c, bins)
        
        frontier_x, frontier_y = [], []
        for i in range(1, len(bins)):
            mask = digitized == i
            if np.any(mask):
                max_val = np.percentile(univ_m[mask], 99)
                frontier_x.append((bins[i-1] + bins[i]) / 2)
                frontier_y.append(max_val)
                
        x_data = np.array(frontier_x)
        y_data = np.array(frontier_y)
        
        # 6. Curve Fitting
        def exp_decay(x, a, b, floor):
            return a * np.exp(-b * x) + floor

        try:
            # Bounds: a>0, b>0, floor>=0
            p0 = [np.max(y_data), 1000, 1.0]
            popt, _ = curve_fit(exp_decay, x_data, y_data, p0=p0, maxfev=5000, bounds=(0, np.inf))
            
            y_pred = exp_decay(x_data, *popt)
            r2 = r2_score(y_data, y_pred)
            
            results[borough] = {
                'Max_Speed': popt[0] + popt[2], 
                'Fragility': popt[1],
                'Friction': popt[2],
                'R2': r2
            }
            
            # Plot Curve
            x_plot = np.linspace(0, x_data.max(), 100)
            y_plot = exp_decay(x_plot, *popt)
            ax.plot(x_plot, y_plot, color=color, linewidth=3, 
                    label=f"{borough} ($v_{{max}}={popt[0]+popt[2]:.1f}, R^2={r2:.2f}$)")
            
            # Plot faint dots
            ax.scatter(x_data, y_data, color=color, alpha=0.15, s=15)
            
        except Exception as e:
            print(f"Fit failed for {borough}: {e}")

    # 7. Final Plot Formatting
    ax.set_title("The 'Fingerprints' of Capital: Comparative Market Physics\n(Standard Bandwidth: 45 Days)", fontsize=16)
    ax.set_xlabel("Price Certainty (Density) $\\rightarrow$", fontsize=12)
    ax.set_ylabel("Max Liquidity Velocity (Speed Limit) $\\uparrow$", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    plt.show()
    
    # 8. Comparison Table
    print("\n" + "="*90)
    print(f"{'BOROUGH':<15} | {'MAX SPEED':<12} | {'FRAGILITY (Decay)':<18} | {'FRICTION (Floor)':<15} | {'R²':<6}")
    print("-" * 90)
    for b, res in results.items():
        print(f"{b:<15} | {res['Max_Speed']:<12.2f} | {res['Fragility']:<18.1f} | {res['Friction']:<15.2f} | {res['R2']:.3f}")
    print("="*90)

# --- Execution ---
analyze_borough_frontiers(
    df_clean, 
    start_date=df_clean['Date'].min(), 
    end_date=df_clean['Date'].max()
)

#%% Following experiment verifies the curves wobble, but maintain their shape through time, parametric drift

plot_heisenberg_exponential_frontier(
    kde_4d, 
    start_date='2017-01-01',
    end_date='2018-12-31',
    grid_x=x_grid, 
    grid_y=y_grid 
)

plot_heisenberg_exponential_frontier(
    kde_4d, 
    start_date='2020-01-01', 
    end_date='2021-06-30', 
    grid_x=x_grid, 
    grid_y=y_grid 
)

#%% Configuration Entropy

from scipy.stats import gaussian_kde

def calculate_borough_entropy(df):
    from scipy.stats import entropy
    
    results = []
    
    # Analyze Manhattan vs Brooklyn
    for boro in ['manhattan', 'brooklyn', 'bronx', 'queens', 'statenisland']:
        subset = df[df['Boro_Source'] == boro]
        
        # Fit KDE
        vals = np.vstack([subset['Log_BuildingArea'], subset['Log_Real_PPSF']])
        kde = gaussian_kde(vals)
        
        # Evaluate on the grid (reuse X, Y from global scope)
        positions = np.vstack([X.ravel(), Y.ravel()])
        z = kde(positions)
        
        # Normalize to ensure it sums to 1 (discrete probability distribution for entropy)
        p = z / z.sum()
        
        # Calculate Entropy
        # S = - sum(p * log(p))
        ent = entropy(p)
        
        results.append({
            'Borough': boro,
            'Entropy (S)': ent,
            'Peak Density': z.max()
        })
        
    return pd.DataFrame(results)

entropy_report = calculate_borough_entropy(df)
print(entropy_report) 

#%% Visualization of M(x) [HTML]

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import plotly.graph_objects as go
from datetime import datetime

# --- Execution Pipeline ---

# Load Data
filename = 'nyc_residential_cleaned.csv'

df_clean = pd.read_csv(filename, parse_dates=['Date'])
# Instantiate the 4D Madelung Object M(x)
kde_4d = ContinuousTimeKDE(df_clean, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=52)

# Interactive Visualization with Plotly
print("Generating animation frames...")

# Grid Setup
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50) # 50x50 grid for speed
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

# Time Steps for Animation (e.g., every 14 days)
date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='14D')

# Pre-calculate Frames
frames = []
max_z = 0 # Track max density for fixed Z-axis scaling

for date in date_range:
    date_str = date.strftime('%Y-%m-%d')
    Z = kde_4d.get_density_at_time(date, X, Y)
    
    current_max = Z.max()
    if current_max > max_z:
        max_z = current_max
        
    frames.append(go.Frame(
        data=[go.Surface(z=Z, x=X, y=Y, colorscale='Viridis')],
        name=date_str
    ))

# Create Initial Plot Data
initial_date = date_range[0]
Z_init = kde_4d.get_density_at_time(initial_date, X, Y)

fig = go.Figure(
    data=[go.Surface(z=Z_init, x=X, y=Y, colorscale='Viridis', colorbar=dict(title='Density'))],
    frames=frames
)

# Layout Configuration (Animation Controls)
fig.update_layout(
    title='4D Market Evolution: Log Area vs Log PPSF (Time-Varying KDE)',
    scene=dict(
        xaxis_title='Log Building Area',
        yaxis_title='Log PPSF',
        zaxis_title='Density',
        zaxis=dict(range=[0, max_z * 1.1]), # Fix Z-axis so it doesn't jump
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.2))
    ),
    updatemenus=[{
        'type': 'buttons',
        'showactive': False,
        'buttons': [
            {
                'label': 'Play',
                'method': 'animate',
                'args': [None, {
                    'frame': {'duration': 100, 'redraw': True},
                    'fromcurrent': True,
                    'transition': {'duration': 0}
                }]
            },
            {
                'label': 'Pause',
                'method': 'animate',
                'args': [[None], {
                    'frame': {'duration': 0, 'redraw': False},
                    'mode': 'immediate',
                    'transition': {'duration': 0}
                }]
            }
        ]
    }],
    sliders=[{
        'steps': [
            {
                'method': 'animate',
                'args': [[f.name], {'mode': 'immediate', 'frame': {'duration': 0, 'redraw': True}, 'transition': {'duration': 0}}],
                'label': f.name
            } for f in frames
        ],
        'currentvalue': {'prefix': 'Date: ', 'font': {'size': 20}},
        'pad': {'t': 50}
    }]
)

# save it as an interactive HTML file
output_file = 'market_evolution_4d(10YRS, REAL USD).html'
fig.write_html(output_file)
print(f"Interactive 3D animation saved to {output_file}")

fig.show()

#%% GIF of M(x)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
import matplotlib.animation as animation
from scipy.stats import gaussian_kde
from datetime import datetime

# --- Execution Pipeline ---

# 1. Load Data
filename = 'nyc_residential_cleaned.csv' 

df_clean = pd.read_csv(filename, parse_dates=['Date'])


# 3. Instantiate the 4D Density Object
# Bandwidth: 45 days gives a smoother, less jittery animation
kde_4d = ContinuousTimeKDE(df_clean, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=52)

# 4. GIF Generation
print("Generating 3D Animation (GIF)... this may take a minute.")

# Setup Grid
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50)
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

# Setup Figure
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection='3d')

# Initial Plot (placeholder)
surf = ax.plot_surface(X, Y, np.zeros_like(X), cmap=cm.viridis)

# Find global max density to fix the Z-axis (prevents jumping)
# Check a few sample points
sample_dates = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='30D')
max_z = 0
for d in sample_dates:
    z_sample = kde_4d.get_density_at_time(d, X, Y)
    max_z = max(max_z, z_sample.max())

ax.set_zlim(0, max_z * 1.2)
ax.set_xlabel('Log Building Area (Scale)')
ax.set_ylabel('Log Price/SqFt (Value)')
ax.set_zlabel('Density')
ax.view_init(elev=30, azim=225) # Nice camera angle

# The Update Function (called for each frame)
dates_to_animate = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='14D')

def update(frame_date):
    ax.clear() # Clear previous frame
    
    # Re-apply axis labels and limits (they get cleared too)
    ax.set_zlim(0, max_z * 1.2)
    ax.set_xlabel('Log Building Area')
    ax.set_ylabel('Log PPSF')
    ax.set_zlabel('Density')
    ax.set_title(f"Market Density: {frame_date.strftime('%Y-%m-%d')}")
    ax.view_init(elev=30, azim=225) 
    
    # Calculate new surface
    Z = kde_4d.get_density_at_time(frame_date, X, Y)
    
    # Plot new surface
    surf = ax.plot_surface(X, Y, Z, cmap=cm.viridis, edgecolor='none', alpha=0.9)
    return surf,

# Create Animation Object
ani = animation.FuncAnimation(
    fig, 
    update, 
    frames=dates_to_animate, 
    interval=200 # 200ms per frame = 5 frames per second
)

# Save as GIF
output_gif = 'housing_market_evolution.gif'
# Use 'pillow' writer which doesn't require installing ffmpeg
ani.save(output_gif, writer='pillow', fps=5)

print(f"Animation saved to {output_gif}")

#%% Visualization showing what areas of M(x) are dominated by what buroughs

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy.stats import gaussian_kde

# --- Configuration ---
# Hardcoded colors to ensure consistency (Standard NYC Planning colors where possible)
BOROUGH_COLOR_MAP = {
    'manhattan': '#1f77b4', # Blue
    'brooklyn':  '#ff7f0e', # Orange
    'queens':    '#2ca02c', # Green
    'bronx':     '#d62728', # Red
    'statenisland': '#9467bd' # Purple
}

# --- 1. The Multi-Class KDE Engine ---
class MultiClassKDE:
    def __init__(self, df, x_col, y_col, t_col, class_col, time_bandwidth_days=45):
        self.df = df
        self.x_col = x_col
        self.y_col = y_col
        self.class_col = class_col
        
        self.start_date = df[t_col].min()
        self.df['t_int'] = (df[t_col] - self.start_date).dt.days
        self.sigma_t = time_bandwidth_days
        
        # Sort classes alphabetically to ensure consistent indexing map
        self.classes = sorted(df[class_col].unique())
        self.class_map = {name: i for i, name in enumerate(self.classes)}
        
    def get_density_and_class(self, query_date, grid_x, grid_y):
        """
        Calculates Global Density (Z) and Dominant Borough Index (C)
        """
        if isinstance(query_date, str):
            query_date = pd.to_datetime(query_date)
        t_query = (query_date - self.start_date).days
        
        # 1. Temporal Weighting
        time_diff = self.df['t_int'].values - t_query
        weights = np.exp(-0.5 * (time_diff / self.sigma_t) ** 2)
        
        mask = weights > 1e-4
        if not np.any(mask):
            return np.zeros_like(grid_x), np.zeros_like(grid_x)
            
        df_sub = self.df.iloc[mask]
        w_sub = weights[mask]
        
        # 2. Fit Global KDE (Height of the mountain)
        positions = np.vstack([grid_x.ravel(), grid_y.ravel()])
        try:
            kde_global = gaussian_kde(
                [df_sub[self.x_col], df_sub[self.y_col]], 
                weights=w_sub
            )
            Z_global = kde_global(positions).reshape(grid_x.shape)
        except:
            return np.zeros_like(grid_x), np.zeros_like(grid_x)

        # 3. Fit Per-Borough KDEs (Color of the territory)
        class_probs = np.zeros((len(self.classes), grid_x.size))
        
        for i, cls in enumerate(self.classes):
            cls_mask = (df_sub[self.class_col] == cls).values
            
            # We need a few points to fit a KDE
            if np.sum(cls_mask) > 5: 
                try:
                    kde_cls = gaussian_kde(
                        [df_sub.loc[cls_mask, self.x_col], df_sub.loc[cls_mask, self.y_col]],
                        weights=w_sub[cls_mask]
                    )
                    prob_vec = kde_cls(positions)
                    # Scale by total weight (Prior Probability)
                    # This ensures a borough with 10x more sales wins the territory
                    prob_vec *= np.sum(w_sub[cls_mask]) 
                    class_probs[i, :] = prob_vec
                except:
                    pass
        
        # 4. Determine Winner
        C_flat = np.argmax(class_probs, axis=0)
        C_map = C_flat.reshape(grid_x.shape)
        
        return Z_global, C_map

# --- Execution Pipeline ---

# 1. Load Data
filename = 'nyc_residential_cleaned.csv'
print(f"Loading {filename}...")
df = pd.read_csv(filename, low_memory=False)

# 2. Preprocessing
print("Cleaning data...")
df_clean = df[
    (df['Price'] > 1000) & 
    (df['BuildingArea'] > 100) &
    (df['Date'].notna())
].copy()

# Basic Transformations
df_clean['ppsf'] = df_clean['Price'] / df_clean['BuildingArea']
df_clean['Log_Real_PPSF'] = np.log(df_clean['ppsf'])
df_clean['Log_BuildingArea'] = np.log(df_clean['BuildingArea'])
df_clean['Date'] = pd.to_datetime(df_clean['Date'])

# Ensure 'Borough' column matches our map (Case sensitive check)
target_col = 'Borough' 
if target_col not in df_clean.columns:
    # Attempt to find it
    candidates = [c for c in df_clean.columns if 'boro' in c.lower()]
    if candidates:
        target_col = candidates[0]
        print(f"Using column '{target_col}' for Boroughs.")
    else:
        raise ValueError("Could not find a 'Borough' column in the CSV.")

# Filter for the 5 target boroughs to remove noise
valid_boroughs = list(BOROUGH_COLOR_MAP.keys())
df_clean = df_clean[df_clean[target_col].isin(valid_boroughs)]
print(f"Data ready. Observations: {len(df_clean)}")

# 3. Instantiate KDE
mc_kde = MultiClassKDE(df_clean, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', target_col, time_bandwidth_days=52)

# 4. Color Logic (Crucial for correct mapping)
sorted_boroughs = mc_kde.classes # These are sorted alphabetically
ordered_colors = [BOROUGH_COLOR_MAP[b] for b in sorted_boroughs]

print(f"Mapping Indices: { {i:b for i, b in enumerate(sorted_boroughs)} }")

# 5. Grid Setup
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50)
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

# 6. Generate Animation Frames
frames = []
date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='30D')
max_z = 0

print("Generating 3D frames (this may take a moment)...")
for date in date_range:
    date_str = date.strftime('%Y-%m-%d')
    Z, C = mc_kde.get_density_and_class(date, X, Y)
    
    if Z.max() > max_z: max_z = Z.max()
    
    frames.append(go.Frame(
        data=[go.Surface(
            z=Z, x=X, y=Y,
            surfacecolor=C,
            # Discrete colorscale mapping integers 0..N-1 to colors
            colorscale=[[i/(len(ordered_colors)-1), c] for i, c in enumerate(ordered_colors)],
            cmin=0, cmax=len(ordered_colors)-1,
            showscale=False,
            name='Territories',
            opacity=0.9
        )],
        name=date_str
    ))

# 7. Build Initial Figure
Z_init, C_init = mc_kde.get_density_and_class(date_range[0], X, Y)

fig = go.Figure(
    data=[go.Surface(
        z=Z_init, x=X, y=Y,
        surfacecolor=C_init,
        colorscale=[[i/(len(ordered_colors)-1), c] for i, c in enumerate(ordered_colors)],
        cmin=0, cmax=len(ordered_colors)-1,
        showscale=False,
        opacity=0.9
    )],
    frames=frames
)

# 8. The Legend Hack (Invisible Scatters)
for i, name in enumerate(sorted_boroughs):
    fig.add_trace(go.Scatter3d(
        x=[None], y=[None], z=[None],
        mode='markers',
        marker=dict(size=10, color=ordered_colors[i]),
        name=name
    ))

# 9. Layout
fig.update_layout(
    title='The 5 Boroughs: 3D Market Territories',
    scene=dict(
        xaxis_title='Log Building Area',
        yaxis_title='Log Price/SqFt',
        zaxis_title='Market Density',
        zaxis=dict(range=[0, max_z*1.1]),
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.3))
    ),
    updatemenus=[{
        'type': 'buttons',
        'buttons': [
            {'label': 'Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 100, 'redraw': True}, 'fromcurrent': True}]},
            {'label': 'Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': False}, 'mode': 'immediate'}]}
        ]
    }],
    sliders=[{
        'steps': [{'method': 'animate', 'args': [[f.name], {'mode': 'immediate', 'frame': {'duration': 0, 'redraw': True}}], 'label': f.name} for f in frames],
        'currentvalue': {'prefix': 'Date: ', 'font': {'size': 20}},
    }]
)

output_file = 'nyc_borough_territories.html'
fig.write_html(output_file)
print(f"Visualization saved to {output_file}") 

fig,show()

#%% Metric Instability Map

print("Generating Metric Instability Animation...")

# Grid Setup (Reuse from previous steps)
# Assuming kde_4d, X, Y, date_range are already defined
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50)
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

frames = []
max_instability = 0

print("Generating frames with date stamps...")
date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='14D')

for date in date_range:
    date_str = date.strftime('%Y-%m-%d')
    
    # 1. Get Analytic Physics Fields
    Z, U, V, P_dot = kde_4d.get_analytic_derivatives(date, X, Y)
    
    # 2. Calculate Metric Instability (Trace g_ij)
    Metric_Instability = U**2 + V**2
    
    # Normalize/Clip
    # Using 98th percentile to avoid washing out the ring with one single outlier
    limit = np.percentile(Metric_Instability, 98) 
    Metric_Instability = np.clip(Metric_Instability, 0, limit)
    
    # Track global max for colorbar consistency
    if limit > max_instability:
        max_instability = limit

    # Downsample for vectors
    skip = 4
    
    frames.append(go.Frame(
        data=[
            # Trace 0: The Manifold (Colored by Instability)
            go.Surface(
                z=Z, x=X, y=Y,
                surfacecolor=Metric_Instability,
                colorscale='Inferno', 
                cmin=0, cmax=max_instability,
                opacity=0.9,
                name='Metric Instability'
            ),
            
            # Trace 1: The Force Vectors
            go.Cone(
                x=X[::skip, ::skip].flatten(),
                y=Y[::skip, ::skip].flatten(),
                z=Z[::skip, ::skip].flatten(),
                u=U[::skip, ::skip].flatten(),
                v=V[::skip, ::skip].flatten(),
                w=np.zeros_like(U[::skip, ::skip].flatten()),
                sizemode="absolute",
                sizeref=2, 
                anchor="tail",
                colorscale='Greys',
                showscale=False,
                opacity=0.5,
                name='Market Force'
            )
        ],
        name=date_str,
        # --- NEW: Update Title for Every Frame ---
        layout=go.Layout(
            title_text=f"Market Instability Field: {date_str}",
            title_x=0.5,
            title_font=dict(size=24)
        )
    ))

# --- Updated Initial Plot ---
# (Ensure the initial figure also has a title)
initial_date = date_range[0]
date_str_init = initial_date.strftime('%Y-%m-%d')
Z_init, U_init, V_init, _ = kde_4d.get_analytic_derivatives(initial_date, X, Y)
Metric_init = np.clip(U_init**2 + V_init**2, 0, np.percentile(U_init**2 + V_init**2, 98))

fig = go.Figure(
    data=[
        go.Surface(
            z=Z_init, x=X, y=Y, 
            surfacecolor=Metric_init,
            colorscale='Inferno', 
            cmin=0, cmax=max_instability,
            colorbar=dict(title='Local Instability (Trace g_ij)'),
            name='Metric Instability'
        ),
        go.Cone(
            x=X[::skip, ::skip].flatten(),
            y=Y[::skip, ::skip].flatten(),
            z=Z_init[::skip, ::skip].flatten(),
            u=U_init[::skip, ::skip].flatten(),
            v=V_init[::skip, ::skip].flatten(),
            w=np.zeros_like(U_init[::skip, ::skip].flatten()),
            sizemode="absolute",
            sizeref=2,
            anchor="tail",
            colorscale='Greys',
            showscale=False,
            opacity=0.5,
            name='Market Force'
        )
    ],
    frames=frames
)

# Layout
fig.update_layout(
    title='Thermodynamic Phase Space: Density (Height) vs. Instability (Color)',
    scene=dict(
        xaxis_title='Log Building Area (Physical Scale)',
        yaxis_title='Log PPSF (Value Intensity)',
        zaxis_title='Market Density',
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.2))
    ),
    updatemenus=[{
        'type': 'buttons',
        'showactive': False,
        'buttons': [
            {'label': 'Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 100, 'redraw': True}, 'fromcurrent': True}]},
            {'label': 'Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': False}, 'mode': 'immediate'}]}
        ]
    }]
)

output_file = 'metric_instability_field.html'
fig.write_html(output_file)
print(f"Metric Tensor visualization saved to {output_file}")
fig.show()

#%% Kinetic Energy Map


print("Generating Kinetic Energy Density Animation...")

# Grid Setup
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50)
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

frames = []
max_ke = 0

print("Generating frames...")
date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='14D')

for date in date_range:
    date_str = date.strftime('%Y-%m-%d')
    
    # 1. Get Analytic Physics Fields
    Z, U, V, P_dot = kde_4d.get_analytic_derivatives(date, X, Y)
    
    # 2. Kinetic Energy Density: ρ|v|² (density-weighted instability)
    KE_density = Z * (U**2 + V**2)
    
    # 3. Clip outliers using percentile
    limit = np.percentile(KE_density, 98)
    KE_density_clipped = np.clip(KE_density, 0, limit)
    
    if limit > max_ke:
        max_ke = limit

    skip = 4
    
    frames.append(go.Frame(
        data=[
            # The Manifold (Height = Density, Color = KE Density)
            go.Surface(
                z=Z, x=X, y=Y,
                surfacecolor=KE_density_clipped,
                colorscale='Hot',
                cmin=0, cmax=max_ke,
                opacity=0.9,
                name='KE Density'
            ),
            
            # Flow Vectors (scaled by local KE for visual weight)
            go.Cone(
                x=X[::skip, ::skip].flatten(),
                y=Y[::skip, ::skip].flatten(),
                z=Z[::skip, ::skip].flatten(),
                u=U[::skip, ::skip].flatten(),
                v=V[::skip, ::skip].flatten(),
                w=np.zeros_like(U[::skip, ::skip].flatten()),
                sizemode="absolute",
                sizeref=2,
                anchor="tail",
                colorscale='Greys',
                showscale=False,
                opacity=0.5,
                name='Probability Current'
            )
        ],
        name=date_str,
        layout=go.Layout(
            title_text=f"Kinetic Energy Density ρ|v|²: {date_str}",
            title_x=0.5,
            title_font=dict(size=24)
        )
    ))

# --- Initial Frame ---
initial_date = date_range[0]
date_str_init = initial_date.strftime('%Y-%m-%d')
Z_init, U_init, V_init, _ = kde_4d.get_analytic_derivatives(initial_date, X, Y)
KE_init = np.clip(Z_init * (U_init**2 + V_init**2), 0, max_ke)

fig = go.Figure(
    data=[
        go.Surface(
            z=Z_init, x=X, y=Y,
            surfacecolor=KE_init,
            colorscale='Hot',
            cmin=0, cmax=max_ke,
            colorbar=dict(title='ρ|v|² (Energy Density)'),
            name='KE Density'
        ),
        go.Cone(
            x=X[::skip, ::skip].flatten(),
            y=Y[::skip, ::skip].flatten(),
            z=Z_init[::skip, ::skip].flatten(),
            u=U_init[::skip, ::skip].flatten(),
            v=V_init[::skip, ::skip].flatten(),
            w=np.zeros_like(U_init[::skip, ::skip].flatten()),
            sizemode="absolute",
            sizeref=2,
            anchor="tail",
            colorscale='Greys',
            showscale=False,
            opacity=0.5,
            name='Probability Current'
        )
    ],
    frames=frames
)

fig.update_layout(
    title=f'Market Kinetic Energy Density: {date_str_init}',
    scene=dict(
        xaxis_title='Log Building Area',
        yaxis_title='Log Real PPSF',
        zaxis_title='Probability Density ρ',
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.2))
    ),
    updatemenus=[{
        'type': 'buttons',
        'showactive': False,
        'buttons': [
            {'label': 'Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 100, 'redraw': True}, 'fromcurrent': True}]},
            {'label': 'Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': False}, 'mode': 'immediate'}]}
        ]
    }],
    sliders=[{
        'steps': [
            {
                'method': 'animate',
                'args': [[f.name], {'mode': 'immediate', 'frame': {'duration': 0, 'redraw': True}, 'transition': {'duration': 0}}],
                'label': f.name
            } for f in frames
        ],
        'currentvalue': {'prefix': 'Date: ', 'font': {'size': 16}},
        'pad': {'t': 50}
    }]
)

output_file = 'kinetic_energy_density.html'
fig.write_html(output_file)
print(f"Kinetic Energy Density visualization saved to {output_file}")

fig.show() 

#%% Spatial Fisher Metric Streamlines

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import cm

# --- Execution Pipeline ---

# Load Data
filename = 'nyc_residential_cleaned.csv' 
print(f"Loading {filename}...")
df = pd.read_csv(filename, low_memory=False)

kde_4d = ContinuousTimeKDE(df_clean, 'Log_BuildingArea', 'Log_Real_PPSF', 'Date', time_bandwidth_days=52)

# Visualization Setup
print("Generating 2D Animation...")

# Create Grid
x_grid = np.linspace(df_clean['Log_BuildingArea'].min(), df_clean['Log_BuildingArea'].max(), 50)
y_grid = np.linspace(df_clean['Log_Real_PPSF'].min(), df_clean['Log_Real_PPSF'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

# Setup Figure
fig, ax = plt.subplots(figsize=(10, 8))
date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='14D')

def update(frame_date):
    ax.clear() # Clear previous frame
    
    date_str = frame_date.strftime('%Y-%m-%d')
    print(f"Rendering frame: {date_str}")
    
    # Get Derivatives from M(x)
    Z, U, V, P_dot = kde_4d.get_analytic_derivatives(frame_date, X, Y)
    
    # 1. Background Density (Contourf)
    # Uses Z (density height) to color the background
    cont = ax.contourf(X, Y, Z, levels=30, cmap='Blues_r', alpha=0.6)
    
    # 2. Vector Field (Streamplot)
    # This automatically handles integration, streamlines, and ARROWHEADS
    # density: controls how close lines are (1 = default, 2 = dense)
    # color: Color lines by magnitude (speed) or keep them solid
    magnitude = np.sqrt(U**2 + V**2)
    
    strm = ax.streamplot(
        X, Y, U, V,
        color='black',      # White lines pop against Viridis background
        linewidth=1, 
        arrowsize=1.5,      # Size of the arrow heads
        density=1.2         # Density of streamlines
    )
    
    # Formatting
    ax.set_title(f"Market Flow: {date_str}", fontsize=16)
    ax.set_xlabel("Log Area", fontsize=12)
    ax.set_ylabel("Log Price per SqFt", fontsize=12)
    ax.set_xlim(x_grid.min(), x_grid.max())
    ax.set_ylim(y_grid.min(), y_grid.max())

# Create Animation
ani = animation.FuncAnimation(
    fig, 
    update, 
    frames=date_range, 
    interval=1000  # Time between frames in ms
)

# Save
output_file = 'market_flow_2d.mp4'
# Note: Requires ffmpeg installed. For gif, change extension to .gif and writer='pillow'
ani.save(output_file, writer='ffmpeg', fps=10, dpi=150)
print(f"Animation saved to {output_file}")
plt.close()

#%% Quantum Potential Streamlines

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.colors as mcolors

# --- Execution Pipeline ---

# 1. Load Data
filename = 'nyc_real_estate_history_REAL_DOLLARS.csv' 
print(f"Loading {filename}...")
try:
    df = pd.read_csv(filename, low_memory=False)
except FileNotFoundError:
    print("File not found. Please ensure filename is correct.")

# 2. Preprocessing
df_clean = df[
    (df['Price'] > 1000) & 
    (df['BuildingArea'] > 100) &
    (df['Date'].notna())
].copy()

df_clean['ppsf'] = df_clean['Price'] / df_clean['BuildingArea']
df_clean['log_ppsf'] = np.log(df_clean['ppsf'])
df_clean['log_area'] = np.log(df_clean['BuildingArea'])
df_clean['Date'] = pd.to_datetime(df_clean['Date'])

# 3. Instantiate KDE
kde_4d = ContinuousTimeKDE(df_clean, 'log_area', 'log_ppsf', 'Date', time_bandwidth_days=45)

# 4. Visualization Setup: Quantum Potential
print("Generating 2D Quantum Potential Animation...")

# Grid
x_grid = np.linspace(df_clean['log_area'].min(), df_clean['log_area'].max(), 50)
y_grid = np.linspace(df_clean['log_ppsf'].min(), df_clean['log_ppsf'].max(), 50)
X, Y = np.meshgrid(x_grid, y_grid)

# Get spacing for gradient calculation
dx = x_grid[1] - x_grid[0]
dy = y_grid[1] - y_grid[0]

fig, ax = plt.subplots(figsize=(12, 10))
date_range = pd.date_range(start=df_clean['Date'].min(), end=df_clean['Date'].max(), freq='30D')

def update(frame_date):
    ax.clear()
    date_str = frame_date.strftime('%Y-%m-%d')
    print(f"Rendering: {date_str}")
    
    # 1. Get Base Fields
    Z, U, V, P_dot = kde_4d.get_analytic_derivatives(frame_date, X, Y)
    
    # 2. Calculate Quantum Potential (Q)
    # Gradient returns [d/dy, d/dx] for (rows, cols)
    dU_dy, dU_dx = np.gradient(U, dy, dx)
    dV_dy, dV_dx = np.gradient(V, dy, dx)
    
    divergence = dU_dx + dV_dy
    fisher_info_density = U**2 + V**2
    
    # Q Formula: -( |s|^2 + 2 * div(s) )
    Q = -(fisher_info_density + 2 * divergence)
    
    # 3. Masking & Robust Scaling
    # Q goes to infinity in empty space (div(s) is unstable). 
    # Mask low density regions to focus on the market.
    mask = Z < 1e-5 
    Q_masked = np.ma.masked_where(mask, Q)
    
    # Calculate robust limits for colorbar (e.g., 2nd to 98th percentile)
    # This prevents one singluarity from washing out the whole plot
    vmin, vmax = np.nanpercentile(Q_masked, [2, 98])
    # Force symmetry around 0 for diverging colormap
    limit = max(abs(vmin), abs(vmax))
    
    # 4. Plot Q Field (The Background Color)
    # RdBu_r: Red = Positive Pressure (Peaks/Stability), Blue = Negative (Slopes/Change)
    cont = ax.contourf(
        X, Y, Q_masked, 
        levels=40, 
        cmap='RdBu_r', 
        vmin=-limit, vmax=limit,
        extend='both',
        alpha=0.8
    )
    
    # 5. Overlay Streamlines (The Flow)
    # Color lines black to contrast with the colorful Q-field
    strm = ax.streamplot(
        X, Y, U, V,
        color='k',
        linewidth=0.8,
        arrowsize=1.0,
        density=1.0
    )
    
    # 6. Overlay Density Iso-lines (Optional context)
    # Thin faint lines showing where the actual "hills" are
    ax.contour(X, Y, Z, levels=5, colors='k', alpha=0.2, linewidths=0.5)

    ax.set_title(f"Market Quantum Potential: {date_str}\n(Red = Stability/Pressure, Blue = Tension/Flow)", fontsize=14)
    ax.set_xlabel("Log Building Area")
    ax.set_ylabel("Log Price per SqFt")

# Create Animation
ani = animation.FuncAnimation(fig, update, frames=date_range, interval=200)

output_file = 'market_quantum_potential.mp4'
ani.save(output_file, writer='ffmpeg', fps=10, dpi=150)
print(f"Saved to {output_file}")
plt.close()

#%% Battaillean Decomposition

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from scipy.fft import fft2, ifft2, fftfreq
import torch


def extract_diffusion_coefficient(kde, query_date):
    """
    Extracts the diffusion tensor from the KDE's kernel covariance.
    
    In the Fokker-Planck framework: D ~ σ²_spatial / τ_temporal
    The KDE already computes the kernel covariance matrix via Scott's rule.
    """
    if isinstance(query_date, str):
        query_date = pd.to_datetime(query_date)
    t_query = (query_date - kde.start_date).days
    
    # Get temporal weights (same as in get_analytic_derivatives)
    weights = torch.exp(-0.5 * ((kde.t - t_query) / kde.sigma_t) ** 2)
    mask = weights > 1e-4
    
    x_sub = kde.x[mask]
    y_sub = kde.y[mask]
    w_sub = weights[mask]
    w_norm = w_sub / w_sub.sum()
    
    # Effective sample size
    neff = 1.0 / (w_norm ** 2).sum()
    
    # Weighted means
    mean_x = (x_sub * w_norm).sum()
    mean_y = (y_sub * w_norm).sum()
    
    # Weighted covariance
    xc = x_sub - mean_x
    yc = y_sub - mean_y
    denom = 1.0 - (w_norm ** 2).sum()
    
    cov_xx = ((w_norm * xc * xc).sum() / denom).cpu().numpy()
    cov_yy = ((w_norm * yc * yc).sum() / denom).cpu().numpy()
    cov_xy = ((w_norm * xc * yc).sum() / denom).cpu().numpy()
    
    # Scott's rule factor
    factor = float(neff.cpu().numpy()) ** (-1.0 / 6.0)
    
    # Kernel covariance (spatial smoothing scale)
    k_cov_xx = cov_xx * (factor ** 2)
    k_cov_yy = cov_yy * (factor ** 2)
    k_cov_xy = cov_xy * (factor ** 2)
    
    # Diffusion tensor: D = kernel_covariance / temporal_scale
    tau = kde.sigma_t  # temporal bandwidth in days
    
    D_xx = k_cov_xx / tau
    D_yy = k_cov_yy / tau
    D_xy = k_cov_xy / tau
    
    # Scalar approximation (trace / 2)
    D_scalar = (D_xx + D_yy) / 2
    
    return {
        'D_xx': D_xx,
        'D_yy': D_yy,
        'D_xy': D_xy,
        'D_scalar': float(D_scalar),
        'kernel_cov': (k_cov_xx, k_cov_yy, k_cov_xy),
        'tau': tau,
        'neff': float(neff.cpu().numpy())
    }


def get_flux_decomposition(kde, query_date, grid_x, grid_y, verbose=True):
    """
    Decomposes market dynamics into Housekeeping and Osmotic (Excess) components.
    Uses observable diffusion extracted from KDE bandwidth.
    
    Housekeeping (v_hk): Maintains non-equilibrium circulation (The Accursed Share)
    Osmotic (v_ex): Maintains structural gradients (The Form)
    """
    X, Y = np.meshgrid(grid_x, grid_y)
    dx = grid_x[1] - grid_x[0]
    dy = grid_y[1] - grid_y[0]
    
    # Get fields from Madelung object
    Z, U, V, P_dot = kde.get_analytic_derivatives(query_date, X, Y)
    
    # === EXTRACT CALIBRATED DIFFUSION ===
    D_info = extract_diffusion_coefficient(kde, query_date)
    D_scalar = D_info['D_scalar']
    D_xx = D_info['D_xx']
    D_yy = D_info['D_yy']
    
    if verbose:
        print(f"Diffusion coefficient D = {D_scalar:.6f} (neff = {D_info['neff']:.1f})")
    
    # === OSMOTIC VELOCITY (Structural Support) ===
    # v_ex = D * ∇ln(ρ) = D * s
    # Using anisotropic diffusion for accuracy
    v_ex_x = D_xx * U
    v_ex_y = D_yy * V
    v_ex_mag = np.sqrt(v_ex_x**2 + v_ex_y**2)
    
    # === PROBABILITY CURRENT FROM CONTINUITY ===
    # ∂ρ/∂t + ∇·J = 0  →  ∇·J = -P_dot
    # 
    # Solve Poisson equation: ∇²φ = P_dot, then J = -∇φ
    
    # FFT-based Poisson solver
    kx = fftfreq(len(grid_x), dx) * 2 * np.pi
    ky = fftfreq(len(grid_y), dy) * 2 * np.pi
    KX, KY = np.meshgrid(kx, ky)
    K_sq = KX**2 + KY**2
    K_sq[0, 0] = 1  # Avoid division by zero
    
    # φ = F^{-1}[ F[P_dot] / (-k²) ]
    P_dot_hat = fft2(P_dot)
    phi_hat = P_dot_hat / (-K_sq)
    phi_hat[0, 0] = 0  # Zero mean
    phi = np.real(ifft2(phi_hat))
    
    # J = -∇φ
    J_y, J_x = np.gradient(-phi, dy, dx)
    
    # === HOUSEKEEPING VELOCITY (The Accursed Share) ===
    # v_hk = J/ρ (velocity maintaining the current)
    epsilon = 1e-10
    mask = Z > 1e-5
    
    v_hk_x = np.zeros_like(J_x)
    v_hk_y = np.zeros_like(J_y)
    v_hk_x[mask] = J_x[mask] / (Z[mask] + epsilon)
    v_hk_y[mask] = J_y[mask] / (Z[mask] + epsilon)
    v_hk_mag = np.sqrt(v_hk_x**2 + v_hk_y**2)
    
    # === TOTAL EFFECTIVE DRIFT ===
    v_total_x = v_hk_x + v_ex_x
    v_total_y = v_hk_y + v_ex_y
    v_total_mag = np.sqrt(v_total_x**2 + v_total_y**2)
    
    # === ENTROPY PRODUCTION (The Accursed Share Rate) ===
    # σ_hk = ∫ (v_hk² / D) * ρ dx dy
    integrand_hk = np.zeros_like(Z)
    integrand_hk[mask] = (v_hk_mag[mask]**2 / (D_scalar + epsilon)) * Z[mask]
    accursed_share = np.trapz(np.trapz(integrand_hk, dx=dx), dx=dy)
    
    # === STRUCTURAL ENTROPY (Osmotic Cost) ===
    # σ_ex = ∫ (v_ex² / D) * ρ dx dy  
    integrand_ex = np.zeros_like(Z)
    integrand_ex[mask] = (v_ex_mag[mask]**2 / (D_scalar + epsilon)) * Z[mask]
    structural_cost = np.trapz(np.trapz(integrand_ex, dx=dx), dx=dy)
    
    # === TOTAL ENTROPY PRODUCTION ===
    total_entropy = accursed_share + structural_cost
    
    return {
        'density': Z,
        'P_dot': P_dot,
        'J': (J_x, J_y),
        'v_hk': (v_hk_x, v_hk_y),
        'v_hk_mag': v_hk_mag,
        'v_ex': (v_ex_x, v_ex_y),
        'v_ex_mag': v_ex_mag,
        'v_total': (v_total_x, v_total_y),
        'v_total_mag': v_total_mag,
        'accursed_share': accursed_share,
        'structural_cost': structural_cost,
        'total_entropy': total_entropy,
        'bataille_ratio': accursed_share / (structural_cost + epsilon),
        'mask': mask,
        'grid': (X, Y),
        'D_info': D_info,
        'query_date': query_date
    }


def plot_flux_decomposition(fields, title_suffix="", save_path=None):
    """
    Visualizes the Bataillean flux decomposition.
    """
    X, Y = fields['grid']
    Z = fields['density']
    mask = fields['mask']
    
    v_hk_x, v_hk_y = fields['v_hk']
    v_ex_x, v_ex_y = fields['v_ex']
    v_hk_mag = fields['v_hk_mag']
    v_ex_mag = fields['v_ex_mag']
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    
    # === Row 1: Velocity Magnitudes ===
    
    # Plot 1: Housekeeping Velocity (The Accursed Share)
    v_hk_plot = np.where(mask, v_hk_mag, np.nan)
    v_hk_clipped = np.clip(v_hk_plot, 0, np.nanpercentile(v_hk_plot, 98))
    
    im1 = axes[0, 0].pcolormesh(X, Y, v_hk_clipped, cmap='Reds', shading='auto')
    axes[0, 0].contour(X, Y, Z, levels=5, colors='black', linewidths=0.5, alpha=0.4)
    axes[0, 0].set_title(f'Housekeeping |v_hk|\n(The Accursed Share) {title_suffix}')
    axes[0, 0].set_xlabel('Log Building Area')
    axes[0, 0].set_ylabel('Log Real PPSF')
    plt.colorbar(im1, ax=axes[0, 0])
    
    # Plot 2: Osmotic Velocity (Structural Support)
    v_ex_plot = np.where(mask, v_ex_mag, np.nan)
    v_ex_clipped = np.clip(v_ex_plot, 0, np.nanpercentile(v_ex_plot, 98))
    
    im2 = axes[0, 1].pcolormesh(X, Y, v_ex_clipped, cmap='Blues', shading='auto')
    axes[0, 1].contour(X, Y, Z, levels=5, colors='black', linewidths=0.5, alpha=0.4)
    axes[0, 1].set_title(f'Osmotic |v_ex|\n(Structural Support) {title_suffix}')
    axes[0, 1].set_xlabel('Log Building Area')
    axes[0, 1].set_ylabel('Log Real PPSF')
    plt.colorbar(im2, ax=axes[0, 1])
    
    # Plot 3: Ratio (Circulation / Structure)
    ratio = np.zeros_like(v_hk_mag)
    ratio[mask] = v_hk_mag[mask] / (v_ex_mag[mask] + 1e-10)
    ratio_plot = np.where(mask, ratio, np.nan)
    ratio_clipped = np.clip(ratio_plot, 0.01, 100)
    
    im3 = axes[0, 2].pcolormesh(X, Y, ratio_clipped, cmap='RdYlBu_r', 
                                  shading='auto', norm=LogNorm(vmin=0.1, vmax=10))
    axes[0, 2].contour(X, Y, Z, levels=5, colors='black', linewidths=0.5, alpha=0.4)
    axes[0, 2].set_title(f'|v_hk| / |v_ex|\n(Red=Circulation, Blue=Structure) {title_suffix}')
    axes[0, 2].set_xlabel('Log Building Area')
    axes[0, 2].set_ylabel('Log Real PPSF')
    plt.colorbar(im3, ax=axes[0, 2])
    
    # === Row 2: Vector Fields ===
    
    skip = 3
    X_sub = X[::skip, ::skip]
    Y_sub = Y[::skip, ::skip]
    
    # Plot 4: Housekeeping Flow
    v_hk_x_sub = v_hk_x[::skip, ::skip]
    v_hk_y_sub = v_hk_y[::skip, ::skip]
    Z_sub = Z[::skip, ::skip]
    mask_sub = Z_sub > 1e-5
    
    axes[1, 0].contourf(X, Y, Z, levels=20, cmap='Greys', alpha=0.3)
    axes[1, 0].quiver(X_sub[mask_sub], Y_sub[mask_sub], 
                       v_hk_x_sub[mask_sub], v_hk_y_sub[mask_sub],
                       color='red', alpha=0.7, scale=None)
    axes[1, 0].set_title(f'Housekeeping Flow v_hk\n(Circulation Pattern) {title_suffix}')
    axes[1, 0].set_xlabel('Log Building Area')
    axes[1, 0].set_ylabel('Log Real PPSF')
    
    # Plot 5: Osmotic Flow
    v_ex_x_sub = v_ex_x[::skip, ::skip]
    v_ex_y_sub = v_ex_y[::skip, ::skip]
    
    axes[1, 1].contourf(X, Y, Z, levels=20, cmap='Greys', alpha=0.3)
    axes[1, 1].quiver(X_sub[mask_sub], Y_sub[mask_sub],
                       v_ex_x_sub[mask_sub], v_ex_y_sub[mask_sub],
                       color='blue', alpha=0.7, scale=None)
    axes[1, 1].set_title(f'Osmotic Flow v_ex\n(Structural Gradients) {title_suffix}')
    axes[1, 1].set_xlabel('Log Building Area')
    axes[1, 1].set_ylabel('Log Real PPSF')
    
    # Plot 6: P_dot (Where density is changing)
    P_dot = fields['P_dot']
    P_dot_plot = np.where(mask, P_dot, np.nan)
    p_limit = np.nanpercentile(np.abs(P_dot_plot), 95)
    
    im6 = axes[1, 2].pcolormesh(X, Y, P_dot_plot, cmap='RdBu_r', shading='auto',
                                  vmin=-p_limit, vmax=p_limit)
    axes[1, 2].contour(X, Y, Z, levels=5, colors='black', linewidths=0.5, alpha=0.4)
    axes[1, 2].set_title(f'∂ρ/∂t\n(Red=Accumulating, Blue=Dispersing) {title_suffix}')
    axes[1, 2].set_xlabel('Log Building Area')
    axes[1, 2].set_ylabel('Log Real PPSF')
    plt.colorbar(im6, ax=axes[1, 2])
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150)
    plt.show()
    
    # Print summary
    D_info = fields['D_info']
    print("\n" + "="*60)
    print("BATAILLEAN THERMODYNAMIC DECOMPOSITION")
    print("="*60)
    print(f"Diffusion Coefficient D: {D_info['D_scalar']:.6f}")
    print(f"Effective Sample Size:   {D_info['neff']:.1f}")
    print("-"*60)
    print(f"Accursed Share (Housekeeping σ_hk): {fields['accursed_share']:.6f}")
    print(f"Structural Cost (Osmotic σ_ex):     {fields['structural_cost']:.6f}")
    print(f"Total Entropy Production:           {fields['total_entropy']:.6f}")
    print(f"Bataille Ratio (σ_hk / σ_ex):       {fields['bataille_ratio']:.4f}")
    print("-"*60)
    print("Interpretation:")
    if fields['bataille_ratio'] > 1:
        print("  → Circulation dominates: Market paying high entropy tax")
        print("    to maintain non-equilibrium flows (POTLATCH CONDITION)")
    else:
        print("  → Structure dominates: Market efficiently holding form")
        print("    Gradients maintained with minimal dissipation")
    print("="*60)
    
    return fig


def compute_bataille_timeseries(kde, date_range, grid_x, grid_y):
    """
    Computes Bataille decomposition metrics over time.
    """
    results = []
    
    print(f"Computing Bataille decomposition for {len(date_range)} time steps...")
    
    for i, date in enumerate(date_range):
        if i % 20 == 0:
            print(f"  Processing {date.strftime('%Y-%m-%d')} ({i+1}/{len(date_range)})")
        
        try:
            fields = get_flux_decomposition(kde, date, grid_x, grid_y, verbose=False)
            
            results.append({
                'date': date,
                'accursed_share': fields['accursed_share'],
                'structural_cost': fields['structural_cost'],
                'total_entropy': fields['total_entropy'],
                'bataille_ratio': fields['bataille_ratio'],
                'D_scalar': fields['D_info']['D_scalar'],
                'neff': fields['D_info']['neff']
            })
        except Exception as e:
            print(f"  Warning: Failed for {date}: {e}")
            continue
    
    return pd.DataFrame(results)


def plot_bataille_timeseries(df_bataille, save_path=None):
    """
    Plots the Bataille decomposition metrics over time.
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
    
    dates = df_bataille['date']
    
    # === Plot 1: Entropy Production Components ===
    ax1 = axes[0]
    ax1.fill_between(dates, 0, df_bataille['structural_cost'], 
                     alpha=0.5, color='blue', label='Structural (Osmotic)')
    ax1.fill_between(dates, df_bataille['structural_cost'], df_bataille['total_entropy'],
                     alpha=0.5, color='red', label='Housekeeping (Accursed Share)')
    ax1.plot(dates, df_bataille['total_entropy'], 'k-', linewidth=1.5, label='Total')
    
    ax1.set_ylabel('Entropy Production Rate')
    ax1.set_title('Thermodynamic Decomposition Over Time\n(Stacked: Blue=Structure, Red=Circulation)')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # Add event markers
    events = [
        ('2018-12-01', 'Rate Hike'),
        ('2020-03-15', 'COVID'),
        ('2021-01-01', 'Stimulus')
    ]
    for date_str, label in events:
        try:
            event_date = pd.to_datetime(date_str)
            if dates.min() <= event_date <= dates.max():
                ax1.axvline(event_date, color='gray', linestyle='--', alpha=0.5)
                ax1.text(event_date, ax1.get_ylim()[1]*0.95, label, 
                        rotation=90, va='top', ha='right', fontsize=8)
        except:
            pass
    
    # === Plot 2: Bataille Ratio ===
    ax2 = axes[1]
    ax2.plot(dates, df_bataille['bataille_ratio'], 'purple', linewidth=1.5)
    ax2.axhline(1.0, color='black', linestyle='--', linewidth=1, label='Equilibrium (ratio=1)')
    ax2.fill_between(dates, df_bataille['bataille_ratio'], 1.0,
                     where=(df_bataille['bataille_ratio'] > 1),
                     color='red', alpha=0.3, label='Potlatch Condition')
    ax2.fill_between(dates, df_bataille['bataille_ratio'], 1.0,
                     where=(df_bataille['bataille_ratio'] <= 1),
                     color='blue', alpha=0.3, label='Structural Dominance')
    
    ax2.set_ylabel('Bataille Ratio (σ_hk / σ_ex)')
    ax2.set_yscale('log')
    ax2.set_title('Bataille Ratio: Circulation vs Structure\n(Above 1 = Potlatch, Below 1 = Efficient)')
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.3)
    
    # === Plot 3: Diffusion Coefficient ===
    ax3 = axes[2]
    ax3.plot(dates, df_bataille['D_scalar'], 'green', linewidth=1.5)
    ax3.set_ylabel('Diffusion Coefficient D')
    ax3.set_xlabel('Date')
    ax3.set_title('Market Temperature (Diffusion Coefficient)\n(Higher = More Liquid/Volatile)')
    ax3.grid(True, alpha=0.3)
    
    # Secondary axis for effective sample size
    ax3b = ax3.twinx()
    ax3b.plot(dates, df_bataille['neff'], 'orange', linewidth=1, alpha=0.5, linestyle='--')
    ax3b.set_ylabel('Effective Sample Size', color='orange')
    ax3b.tick_params(axis='y', labelcolor='orange')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150)
    plt.show()
    
    # Print summary statistics
    print("\n" + "="*70)
    print("BATAILLE TIMESERIES SUMMARY")
    print("="*70)
    print(f"Period: {dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')}")
    print(f"Observations: {len(df_bataille)}")
    print("-"*70)
    print(f"{'Metric':<25} | {'Mean':<12} | {'Std':<12} | {'Min':<12} | {'Max':<12}")
    print("-"*70)
    for col in ['accursed_share', 'structural_cost', 'bataille_ratio', 'D_scalar']:
        print(f"{col:<25} | {df_bataille[col].mean():<12.4f} | {df_bataille[col].std():<12.4f} | {df_bataille[col].min():<12.4f} | {df_bataille[col].max():<12.4f}")
    print("-"*70)
    pct_potlatch = (df_bataille['bataille_ratio'] > 1).mean() * 100
    print(f"Time in Potlatch Condition (ratio > 1): {pct_potlatch:.1f}%")
    print("="*70)
    
    return fig


# === EXECUTION ===

# Single snapshot analysis
print("="*70)
print("STABLE PERIOD (2018-06-01)")
print("="*70)
fields_stable = get_flux_decomposition(kde_4d, '2018-06-01', x_grid, y_grid)
plot_flux_decomposition(fields_stable, title_suffix="(2018-06)")

print("\n" + "="*70)
print("CRISIS PERIOD (2020-06-01)")
print("="*70)
fields_crisis = get_flux_decomposition(kde_4d, '2020-06-01', x_grid, y_grid)
plot_flux_decomposition(fields_crisis, title_suffix="(2020-06)")

# Timeseries analysis
print("\n" + "="*70)
print("COMPUTING FULL TIMESERIES")
print("="*70)
date_range = pd.date_range(start='2015-01-01', end='2022-01-01', freq='14D')
df_bataille = compute_bataille_timeseries(kde_4d, date_range, x_grid, y_grid)

# Plot timeseries
plot_bataille_timeseries(df_bataille, save_path='bataille_timeseries.png')

# Save data for further analysis
df_bataille.to_csv('bataille_decomposition.csv', index=False)
print(f"\nTimeseries data saved to bataille_decomposition.csv")

#%% Validation of Decomposition 

import pandas as pd
import numpy as np
import pandas_datareader.data as web
import statsmodels.api as sm
from statsmodels.tsa.stattools import grangercausalitytests, adfuller
import matplotlib.pyplot as plt

# PREPARE DATA

# A. Load calculated Bataille metrics

df_metrics = df_bataille.copy()
df_metrics['date'] = pd.to_datetime(df_metrics['date'])
df_metrics.set_index('date', inplace=True)

# Resample to Monthly Mean to match economic data
df_metrics_monthly = df_metrics.resample('MS').mean()

# B. Fetch External Market Benchmark (Case-Shiller NYC)
# Ticker: NYXRSA (S&P/Case-Shiller NY Home Price Index, Seasonally Adjusted)
print("Fetching Case-Shiller Index (NYXRSA) from FRED...")
start_date = df_metrics.index.min()
end_date = df_metrics.index.max()

try:
    df_cs = web.DataReader('NYXRSA', 'fred', start_date, end_date)
    df_cs.columns = ['Price_Index']
except Exception as e:
    print(f"Error fetching data: {e}")

# C. Merge
df_analysis = pd.merge(df_metrics_monthly, df_cs, left_index=True, right_index=True, how='inner')

# 2. TRANSFORMATIONS (Stationarity)
# Granger Causality requires stationary data. We cannot use raw levels.
# Calculate the Log Return (Percentage Change) and First Differences.

data = pd.DataFrame()

# Target: Price Momentum (Log Return of Case-Shiller)
data['Price_Change'] = np.log(df_analysis['Price_Index']).diff()

# Predictors: 
# 1. Change in Bataille Ratio (Is the market becoming more inefficient?)
data['Bataille_Ratio_Change'] = df_analysis['bataille_ratio'].diff()
# 2. Excess Heat (Is structural change accelerating?)
data['Excess_Heat_Log'] = np.log(df_analysis['structural_cost']) # Log because entropy scales widely
data['Excess_Heat_Change'] = data['Excess_Heat_Log'].diff()

data.dropna(inplace=True)

# ==========================================
# 3. VISUALIZATION
# ==========================================
fig, ax1 = plt.subplots(figsize=(12, 6))

color = 'tab:blue'
ax1.set_xlabel('Date')
ax1.set_ylabel('NYC Price Momentum (Monthly %)', color=color)
ax1.plot(data.index, data['Price_Change'], color=color, alpha=0.6, label='Price Change')
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()  
color = 'tab:red'
ax2.set_ylabel('Excess Heat (Structural Cost)', color=color)  
ax2.plot(data.index, data['Excess_Heat_Log'], color=color, linestyle='--', alpha=0.6, label='Excess Heat')
ax2.tick_params(axis='y', labelcolor=color)

plt.title('Thermodynamics vs. Market Reality\n(Does Heat Precede Price Moves?)')
fig.tight_layout()
plt.show()

# 4. GRANGER CAUSALITY TEST
# H0: The second column DOES NOT Granger Cause the first column.

maxlag = 6  # Test up to 6 months lag

print("\n" + "="*60)
print("TEST 1: Does EXCESS HEAT predict PRICE CHANGES?")
print("="*60)
# Input format: [Target, Predictor]
gc_res = grangercausalitytests(data[['Price_Change', 'Excess_Heat_Change']], maxlag=maxlag, verbose=True)

print("\n" + "="*60)
print("TEST 2: Does BATAILLE RATIO predict PRICE CHANGES?")
print("="*60)
gc_res_2 = grangercausalitytests(data[['Price_Change', 'Bataille_Ratio_Change']], maxlag=maxlag, verbose=True)

# 5. PREDICTIVE REGRESSION (OLS)
# Let's try to predict Price Change(t) using Metrics(t-1) and Metrics(t-2)

data['Excess_Heat_Lag1'] = data['Excess_Heat_Change'].shift(1)
data['Bataille_Lag1'] = data['Bataille_Ratio_Change'].shift(1)
data['Price_Lag1'] = data['Price_Change'].shift(1) # Auto-regressive term

# Drop NaNs created by shifting
reg_data = data.dropna()

X = reg_data[['Excess_Heat_Lag1', 'Bataille_Lag1', 'Price_Lag1']]
X = sm.add_constant(X)
y = reg_data['Price_Change']

model = sm.OLS(y, X).fit()

print("\n" + "="*60)
print("OLS REGRESSION RESULTS (Predicting Next Month's Price Change)")
print("="*60)
print(model.summary())