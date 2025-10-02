from sierrapy.parser import ScidReader

data_path = "F:\\SierraChart\\Data"

mgr = ScidReader(data_path)

print("Loading RB data...")
data = mgr.load_front_month_series("RB", volume_per_bar=100, drop_invalid_rows=True)

print(f"Shape: {data.shape}")
print(f"Open range: [{data['Open'].min():.4f}, {data['Open'].max():.4f}]")
print(f"\nFirst 5 rows:")
print(data.head())

