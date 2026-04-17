import pandas as pd
df = pd.read_csv('customer_shopping_behavior_info.csv')

df.head()
df.info()
df.describe(include='all')
df.isnull().sum() #print() to check 

#replace all mising values with median of each column
df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))  
df.isnull().sum() #print() to check  

#makes headers all lowercase and replaces all spaces with "_"
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ','_')
df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})
df.columns #print() to check 

#create a cloumn age_group
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels) #splits into four and assigns them based of data
df[['age', 'age_group']].head(10) #print() to check 

# create cloumn purchase_frequency_days
frequency_mapping = {'Fortnightly': 14, 
                     'Weekly':7,
                     'Monthly': 30,
                     'Quarterly': 90,
                     'Bi-Weekly': 14,
                     'Annually': 365,
                     'Every 3 Months': 90
                     }
df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping) 
df[['purchase_frequency_days', 'frequency_of_purchases']].head(10) #print() to check 

#checks to see if both columns carry same info IF TRUE
df[['discount_applied', 'promo_code_used']].head(10) #print() to check 
(df['discount_applied'] == df['promo_code_used']).all() #print() to check 

#Since TRUE lets remove promo_code_used
df = df.drop('promo_code_used', axis=1)
df.columns #print() to check 

from sqlalchemy import create_engine
from urllib.parse import quote_plus

username = "sa"
password = "Ethiopia2127_V2"
host = "localhost"
port = "1433"
database = "customer_analysis"

driver = quote_plus("ODBC Driver 18 for SQL Server")

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

df.to_sql("customer", engine, if_exists="replace", index=False)

pd.read_sql("SELECT TOP 5 * FROM customer;", engine)