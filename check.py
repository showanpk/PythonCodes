import pandas as pd
path = r"C:\Users\shonk\OneDrive\Desktop\Saheli Hub\Projects\Project_Reminders_Simple.xlsx"
df = pd.read_excel(path, sheet_name="Reminders")
print(list(df.columns))
print(df.head(2))
