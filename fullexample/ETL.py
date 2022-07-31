import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account



credentials = service_account.Credentials.from_service_account_file('connection/x.json')
client = bigquery.Client(credentials=credentials)
query2 = '''--Streamlit 
        SELECT hourminute,StationName,Region, Technology, sum(SCADAVALUE) as Mw,min(RRP) as RRP
        FROM `test-187010.ReportingDataset.today_Table`  group by 1,2,3,4
        '''

def Get_Bq(query,_cred) :
        df=pd.read_gbq(query,credentials=_cred)
        df.to_parquet('data.parquet')


Get_Bq(query2,credentials)