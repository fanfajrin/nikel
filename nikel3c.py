from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt

sql_query1 = '''WITH BASE AS (
  SELECT
  loan_amount  
  FROM `project_nikel.raw_db.ln_lvl`
  WHERE partner = 'BANK_B'
)
SELECT * FROM BASE
'''

def query_insert(query):
    df = client.query(query).to_dataframe()
    return df

def make_histogram():
    plt.hist(df['loan_amount'], bins=10, color='skyblue', edgecolor='black')
    plt.xlabel('Loan Amount')
    plt.ylabel('Frequency')
    plt.title('Loan Amount of BANK_B')
    # plt.show()
    plt.savefig('histogram.png')

query_insert(sql_query1)
make_histogram()