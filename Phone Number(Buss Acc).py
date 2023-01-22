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





@st.experimental_singleton
def init_connection():
    return pymongo.MongoClient('mongodb+srv://softcom-data:1Tg1maGQm5veVrLe@eyowo.wswtr.mongodb.net/eyowo?authSource=admin&replicaSet=Eyowo-shard-0&w=majority&readPreference=primary&retryWrites=true&ssl=true')
client = init_connection()

# st.title('SME ACCOUNT DETIALS')
number = st.text_input('Enter Phone Number')
numbers=number.split(",")
num=list(numbers)


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

sme = sme[~sme['userId'].str.contains('close', case=False)]
sme.loc[:,'userId'] = [ObjectId(i) for i in sme.loc[:,'userId'] .tolist()]

# userlist = sme[~sme['userId'].str.contains('close', case=False)]['userId'].tolist()

sme['balance'] = sme['balance']/100

@st.experimental_memo(ttl=600000000)
def get_user(collection='users'):
    db = client['eyowo']
    items = db[collection].find({'_id':{'$in' : sme.userId.tolist()}}, projection={'mobile':1})
    items = list(items)  # make hashable for st.experimental_memo
    df = pd.DataFrame(items)
    return df
users = get_user()
users = users.astype({"_id": str})


@st.experimental_memo(ttl=600000000)
def get_txn_sme(collection='transactions', date=dt.datetime(2021,10,1)):
    db = client['eyowo']
    items = db[collection].find({'date':{'$gte':date}, 'user':{'$in':sme.id.tolist()}}, projection={'_id':0,'user':1,'amount':1,'type':1,'date':1,'metadata.sub_type':1})
    items = list(items)  # make hashable for st.experimental_memo
    df = pd.DataFrame(items)
    return df
txn_sme = get_txn_sme()
txn_sme['amount'] = txn_sme['amount']/100
txn_sme = pd.concat([txn_sme,pd.json_normalize(txn_sme['metadata'])], axis=1)

sme['day'] = sme['created_at'].dt.day
sme['month'] = sme['created_at'].dt.month_name()
sme['year'] = sme['created_at'].dt.year


sme_output=sme.merge(txn_sme.groupby(['user','type']).agg({'amount':'sum','type':'count'}).unstack(),left_on='id',right_index=True,how='left')
sme_output = sme_output.astype({"id": str})
sme_output = sme_output.astype({"userId":str})
sme_output.columns = ['id','userId','accountName','accountNumber','balance','timeCreated','created_at','day', 'month','year','amount_credit', 'amount_debit','count_credit','count_debit']
final_output = users.merge(sme_output,left_on='_id',right_on='userId')
final_output = final_output[final_output['mobile'].isin(num)]
final_output.drop(['_id','id','timeCreated','day','month','year'],axis=1)
ac=final_output.amount_credit.sum()
ad=final_output.amount_debit.sum()
at=ac+ad

a = "{:,.2f}".format(ac)
b = "{:,.2f}".format(ad)
c = "{:,.2f}".format(at)

s1,s2,s3= st.columns(3)
st.markdown("""---""")
s4,s5,s6= st.columns(3)


s1.metric('Number of Buss Acc Created',final_output.id.nunique())
s2.metric('Number of Users',final_output.userId.nunique())
s3.metric('No. Buss Acc with Txn',final_output.count_credit.notnull().sum())
s4.metric('Amount of Credit Txn',a)
s5.metric('Amount of Debit Txn',b)
s6.metric('Amount of Total txn',c)


st.dataframe(final_output)
final_output =final_output.to_csv(index=False).encode('utf-8')
st.download_button(label='Download Table',data=final_output,file_name='sme_details.csv',mime='text/csv')
