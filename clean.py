import pandas as pd

columns = ["first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]

df = pd.read_table("data/county.txt", low_memory=False, encoding="ANSI")
df = df[columns]

#Remove all rows with empty or removed data
df = df[df.res_street_address != "REMOVED"]
df.dropna(inplace=True)

#Use the first three digits of the zip code to be used as blocking field
df["zip_code"] = df["zip_code"].astype(str).str[:3]

#Save the dataFrame as a csv file
df.to_csv("data/clean_county.csv", index=True, index_label="id")