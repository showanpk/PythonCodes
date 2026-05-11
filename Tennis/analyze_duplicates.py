import pandas as pd
from collections import defaultdict

excel_file = r'C:\Users\shonk\Downloads\Tennis Register 2025.xlsx'

# Find all valid rows and group by name
name_counts = defaultdict(list)

for sheet_name in pd.ExcelFile(excel_file).sheet_names:
    if sheet_name.strip() in ['Sheet1', 'Template', 'Full Register', '[1]Full Register']:
        continue
        
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=0, dtype=object)
    
    for idx, row in df.iterrows():
        activity = row.iloc[0] if len(row) > 0 else None
        date_val = row.iloc[2] if len(row) > 2 else None
        time = row.iloc[4] if len(row) > 4 else None
        card = row.iloc[5] if len(row) > 5 else None
        name = row.iloc[6] if len(row) > 6 else None
        
        # Valid row?
        if (pd.notna(activity) and str(activity).strip().lower() == 'tennis' and
            pd.notna(date_val) and str(date_val).strip() != '' and
            pd.notna(time) and str(time).strip() != ''):
            
            card_clean = pd.notna(card) and str(card).strip() not in ['no one', 'nan', '0', 'NaN']
            name_clean = pd.notna(name) and str(name).strip() not in ['nan', 'NaN']
            
            if (card_clean or name_clean):
                # This is valid - now check if it's a LITE member (no card or card is non-numeric)
                card_text = str(card).strip() if card_clean else None
                name_text = str(name).strip() if name_clean else None
                
                if name_text and not card_clean:  # LITE member (name but no valid card)
                    name_counts[name_text.lower()].append({
                        'sheet': sheet_name,
                        'row': idx + 2,
                        'name': name_text,
                        'date': date_val,
                        'time': time
                    })

# Find names that appear multiple times
duplicated_names = {name: records for name, records in name_counts.items() if len(records) > 1}

print(f'Duplicate LITE member names found: {len(duplicated_names)}')
total_duped_records = sum(len(v) for v in duplicated_names.values())
print(f'Total records with duplicate names: {total_duped_records}')

if duplicated_names:
    for name, records in list(duplicated_names.items())[:10]:
        print(f'\n"{name}": {len(records)} occurrences')
        for r in records[:2]:
            print(f'  Sheet {r["sheet"]}, Row {r["row"]}: {r["date"]} {r["time"]}')
