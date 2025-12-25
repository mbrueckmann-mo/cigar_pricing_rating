import pandas as pd
import re
import os

excel_path = r"C:\Users\mbrue\Apps and Development\Cigar_Pricing_Rating\Cigar_Pricing_Rating_dev.xlsx"
output_folder = r"C:\Users\mbrue\Apps and Development\Cigar_Pricing_Rating\csv_exports"

os.makedirs(output_folder, exist_ok=True)

def normalize(name):
    name = name.lower()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^a-z0-9_]', '', name)
    return name

sheets = pd.read_excel(excel_path, sheet_name=None)

for sheet_name, df in sheets.items():
    csv_name = normalize(sheet_name) + ".csv"
    csv_path = os.path.join(output_folder, csv_name)
    df.to_csv(csv_path, index=False)
    print(f"Exported {csv_path}")
