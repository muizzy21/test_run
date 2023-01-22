from matplotlib import pyplot as plt
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as ps
import matplotlib as mp
import time
import pymongo
import datetime as dt
pd.set_option('display.max_columns', None)
from bson.objectid import ObjectId
import plotly.graph_objects as go



# st.title('SME REPORT')
st.sidebar.title('BUSS_ACC')
today = dt.date.today()
start_date = st.sidebar.date_input('start date', dt.date(2021,8,15))
end_date = st.sidebar.date_input('end date', today) 

# st.sidebar.title('TXN')
# start_date_t= st.sidebar.date_input('start date_t', dt.date(2021,10,1))
# end_date_t = st.sidebar.date_input('end date_t', today) 



@st.experimental_singleton
def init_connection():
    return pymongo.MongoClient('mongodb+srv://softcom-data:1Tg1maGQm5veVrLe@eyowo.wswtr.mongodb.net/eyowo?authSource=admin&replicaSet=Eyowo-shard-0&w=majority&readPreference=primary&retryWrites=true&ssl=true')
client = init_connection()  

@st.experimental_memo(ttl=600000000)
def get_sme(collection='static_accounts'):
    db = client['eyowo']
    items = db[collection].find({'type':'business'},projection={'_id':0,'id':1,'userId':1,'accountName':1,'accountNumber':1,'balance':1,'timeCreated':1})
    items = list(items)
    df = pd.DataFrame(items)
    return df
sme = get_sme()


sme['created_at'] = sme['timeCreated'].apply(dt.datetime.fromtimestamp)
sme.loc[:,'id'] = [ObjectId(i) for i in sme.loc[:,'id'] .tolist()]
sme['balance'] = sme['balance']/100

sme['day'] = sme['created_at'].dt.day
sme['month'] = sme['created_at'].dt.month_name()
sme['year'] = sme['created_at'].dt.year

sm = sme[(sme['created_at']>=f'{start_date.year}-{start_date.month}-{start_date.day}')&(sme['created_at']<=f'{end_date.year}-{end_date.month}-{end_date.day}')]
smes = sm.id.tolist()

@st.experimental_memo(ttl=600000000)
def get_txn_sme(collection='transactions', date=dt.datetime(2021,9,30)):
    db = client['eyowo']
    items = db[collection].find({'date':{'$gte':date}, 'user':{'$in':sme.id.tolist()}}, projection={'_id':0,'user':1,'amount':1,'type':1,'date':1,'metadata.sub_type':1})
    items = list(items)  # make hashable for st.experimental_memo
    df = pd.DataFrame(items)
    return df
txn_sme = get_txn_sme()
txn_sme['amount'] = txn_sme['amount']/100
txn_sme = pd.concat([txn_sme,pd.json_normalize(txn_sme['metadata'])], axis=1)

txn_sme = txn_sme[txn_sme['user'].isin(smes)]

sme_output=sme.merge(txn_sme.groupby(['user','type']).agg({'amount':'sum','type':'count'}).unstack(),left_on='id',right_index=True,how='left')


txn=pd.DataFrame(txn_sme.groupby([txn_sme.date.dt.week,'type'])[['amount','date']].agg({'amount':'sum','date':max})).unstack().reset_index()
# txn_sme= txn_sme[(txn_sme['date']>=f'{start_date_t.year}-{start_date_t.month}-{start_date_t.day}')&(txn_sme['date']<=f'{end_date_t.year}-{end_date_t.month}-{end_date_t.day}')]

result = txn_sme.groupby('type')['amount'].aggregate('sum').reset_index()


sme_output= sme_output[(sme_output['created_at']>=f'{start_date.year}-{start_date.month}-{start_date.day}')&(sme_output['created_at']<=f'{end_date.year}-{end_date.month}-{end_date.day}')]

sme_output = sme_output.astype({"id": str})
sme_output.columns = ['id','userId','accountName','accountNumber','balance','timeCreated','created_at','day', 'month','year','amount_credit', 'amount_debit','count_credit','count_debit']
ac=sme_output.amount_credit.sum()
ad=sme_output.amount_debit.sum()
at=ac+ad

a = "{:,.2f}".format(ac)
b = "{:,.2f}".format(ad)
c = "{:,.2f}".format(at)

stx=pd.DataFrame(txn_sme.groupby('sub_type')['user'].nunique().reset_index())
stx=stx.sort_values(by=['user'],ascending=False)

atx=pd.DataFrame(txn_sme.groupby('sub_type')['amount'].sum().reset_index())
atx=atx.sort_values(by=['amount'],ascending=False)

s1,s2,s3= st.columns(3)
st.markdown("""---""")
s4,s5,s6= st.columns(3)
s1.metric('Number of Buss Acc Created',sm.id.nunique())
s2.metric('Number of Users',sm.userId.nunique())
s3.metric('Number of Buss Acc(Txn)',sme_output.count_credit.notnull().sum()) 
s4.metric('Amount of Credit Txn',a)
s5.metric('Amount of Debit Txn',b)
s6.metric('Amount of Total Txn',c)

sme_output = sme_output.drop(['timeCreated','day','month','year'],axis=1)
st.dataframe(sme_output)

c1,c2 = st.columns(2)
fig=go.Figure(
    data=[
        go.Bar(name='transaction_activity',x=stx['sub_type'],y=stx['user'])
    ]
)
fig.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Transaction Activity by User", xref="paper",x=0))
st.plotly_chart(fig,use_container_width=True)





fig2=go.Figure(
    data=[
        go.Bar(name='transaction_activity_amt',x=atx['sub_type'],y=atx['amount'])
    ]
)
fig2.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Transaction Activity by Amount", xref="paper",x=0))
st.plotly_chart(fig2,use_container_width=True)



# txn=pd.DataFrame(txn_sme.groupby([txn_sme.date.dt.week,'type'])[['amount','date']].agg({'amount':'sum','date':max})).unstack().reset_index()
# txn.rename(columns ={ 'date': 'week'},inplace=True)
# st.table(txn)

