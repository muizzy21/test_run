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


st.set_page_config(page_title="Multipage App",layout= 'wide')

st.sidebar.title('SME INSIGHT') 

# st.title('SME OVERVIEW ')
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

@st.experimental_memo(ttl=600000000)
def get_txn_sme(collection='transactions', date=dt.datetime(2021,9,30)):
    db = client['eyowo']
    items = db[collection].find({'date':{'$gte':date}, 'user':{'$in':sme.id.tolist()}}, projection={'_id':0,'user':1,'amount':1,'type':1,'date':1})
    items = list(items)  # make hashable for st.experimental_memo
    df = pd.DataFrame(items)
    return df 
txn_sme = get_txn_sme()
txn_sme['amount'] = txn_sme['amount']/100
txn_sme_2=txn_sme[txn_sme['date']>='2022-01']

sme['day'] = sme['created_at'].dt.day
sme['month'] = sme['created_at'].dt.month_name()
sme['year'] = sme['created_at'].dt.year

sme_2 = sme.loc[sme['year']==2022]

ac=sme[sme['created_at']>='2022-01'].groupby(['month']).id.count().reset_index()
months= ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

ac['month'] = pd.Categorical(ac['month'], categories=months, ordered=True)
ac.sort_values(by=['month'],inplace=True)

tx= pd.DataFrame(txn_sme_2.groupby([txn_sme_2.date.dt.month_name()])['user'].nunique().reset_index())
tx['date'] = pd.Categorical(tx['date'], categories=months, ordered=True)
tx.sort_values(by=['date'],inplace=True)

txc = txn_sme_2.groupby([txn_sme_2.date.dt.month_name(),'type']).type.count().unstack().reset_index()
txc['date'] = pd.Categorical(txc['date'], categories=months, ordered=True)
txc.sort_values(by=['date'],inplace=True)

txa = txn_sme_2.groupby([txn_sme_2.date.dt.month_name(),'type']).amount.sum().unstack().reset_index()
txa['date'] = pd.Categorical(txa['date'], categories=months, ordered=True)
txa.sort_values(by=['date'],inplace=True)

txac = pd.merge(txa,txc, on='date')
txac.columns=['month','credit(amount)','debit(amount)','credit(txn_count)','debit(txn_count)']

result = txn_sme.groupby('type')['amount'].aggregate('sum').reset_index()
result_2 = txn_sme_2.groupby('type')['amount'].aggregate('sum').reset_index()

act=result.iloc[0,1]
s = "{:,.2f}".format(act)

adt=result.iloc[1,1]
a = "{:,.2f}".format(adt)

att= act+adt
at = "{:,.2f}".format(att)


act_2=result_2.iloc[0,1]
s2 = "{:,.2f}".format(act_2)

adt_2=result_2.iloc[1,1]
a2 = "{:,.2f}".format(adt_2)

att_2= act_2+adt_2
at2 = "{:,.2f}".format(att_2)


st.subheader('2021(INCEPTION)') 
users_2,buss_2,txns_2 = st.columns(3)
buss_2.metric('Number of Buss Acc',sme.id.nunique())
users_2.metric('Number of Users',sme.userId.nunique())
txns_2.metric('Number of Buss Acc with Txn',txn_sme.user.nunique())
users_2.metric('Amount of Credit Txn',s)
buss_2.metric('Amount of Debit Txn',a)
txns_2.metric('Amount of Total Txn',at)
st.markdown("""---""")
st.subheader('2022(CURRENT YEAR)')
users,buss,txns = st.columns(3)
buss.metric('Number of Buss Acc',sme_2.id.nunique())
users.metric('Number of Users',sme_2.userId.nunique())
txns.metric('Number of Buss Acc with Txn',txn_sme_2.user.nunique())
users.metric('Amount of Credit Txn',s2)
buss.metric('Amount of Debit Txn',a2)
txns.metric('Amount of Total Txn',at2)
st.markdown("""---""")

d1,d2=st.columns(2)
fig=go.Figure(
    data=[
        go.Bar(name='account_created',x=ac['month'],y=ac['id'])
    ]
)
fig.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Business Acc Created by Month", xref="paper",x=0))
d1.plotly_chart(fig,use_container_width=True)

fig2=go.Figure(
    data=[
        go.Bar(name='No. of acc with Txn',x=tx['date'],y=tx['user'])
    ]
)
fig2.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Business Acc(Txn) by Month", xref="paper",x=0))
d2.plotly_chart(fig2,use_container_width=True)

st.subheader('Monthly Transaction Table')
st.table(txac)

st.subheader('Weekly Transaction Table')
txn=pd.DataFrame(txn_sme_2.groupby([txn_sme_2.date.dt.week,'type'])[['amount','date']].agg({'amount':'sum','date':max})).unstack().reset_index()
txn.rename(columns ={ 'date': 'week'},inplace=True)
st.table(txn)







