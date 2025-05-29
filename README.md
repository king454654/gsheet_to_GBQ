# gsheet_to_GBQ

import pandas as pd
import numpy as np

# Set random seed for reproducibility
np.random.seed(42)

# Create date range (weekly data from 2023-01-01 to 2025-04-30)
date_range = pd.date_range(start='2023-01-01', end='2025-04-30', freq='W')
n = len(date_range)

# Generate synthetic media spend data (in USD)
tv_spend = np.random.normal(loc=10000, scale=2000, size=n).clip(min=0)
radio_spend = np.random.normal(loc=3000, scale=800, size=n).clip(min=0)
digital_spend = np.random.normal(loc=8000, scale=1500, size=n).clip(min=0)
print_spend = np.random.normal(loc=2000, scale=500, size=n).clip(min=0)

# Simulate sales based on media spend with some random noise
sales = (
    0.05 * tv_spend +
    0.08 * radio_spend +
    0.10 * digital_spend +
    0.03 * print_spend +
    np.random.normal(loc=1000, scale=500, size=n)
)

# Create DataFrame
df = pd.DataFrame({
    'Date': date_range,
    'TV Spend': tv_spend.round(2),
    'Radio Spend': radio_spend.round(2),
    'Digital Spend': digital_spend.round(2),
    'Print Spend': print_spend.round(2),
    'Sales': sales.round(2)
})

# Show first few rows
print(df.head())

# Optional: Save to CSV
df.to_csv('marketing_spend.csv', index=False)



