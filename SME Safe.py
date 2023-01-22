from matplotlib import pyplot as plt
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as ps
import matplotlib as mp
import time
import pymongo
import datetime as dt
pd.options.display.float_format = '{:,.2f}'.format
from bson.objectid import ObjectId
import plotly.graph_objects as go
from Overview import  client,sme

sme.loc[:,'id'] = [ObjectId(i) for i in sme.loc[:,'id'] .tolist()]

def sme_safe(collection='safes'):
    db = client['eyowo']
    items = db[collection].find({'user':{'in':sme},'type':'xtra'},projection= {'createdAt':1,'principal':1,'user':1,'interestAfterTax':1})
    items = list(items)
    df = pd.DataFrame(items)
    return df

sme_safe = sme_safe()
sme_safe['principal']= sme_safe['principal']/100
sme_safe['interestAfterTax']= sme_safe['interestAfterTax']/100
sme_safe['month']=sme_safe['createdAt'].dt.month_name()


s1,s2,s3,s4 = st.columns(4)
s1.metric('SME savers',sme_safe.user.nunique())
s2.metric('Safe Created',sme_safe._id.count())
s3.metric('Total Principal Value',"{:,.2f}".format(sme_safe.principal.sum()))
s4.metric('Total Interest Value',"{:,.2f}".format(sme_safe.interestAfterTax.sum()))

sme_p = sme_safe.groupby('principal')._id.count().reset_index()
sme_p['principal'] = sme_p['principal'].astype(str)

safe_month=sme_safe.groupby(['month'])._id.count().reset_index()
months= ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

safe_month['month'] = pd.Categorical(safe_month['month'], categories=months, ordered=True)
safe_month.sort_values(by=['month'],inplace=True)

prin_month=sme_safe.groupby(['month']).principal.sum().reset_index()

prin_month['month'] = pd.Categorical(prin_month['month'], categories=months, ordered=True)
prin_month.sort_values(by=['month'],inplace=True)

fig0=go.Figure(
    data=[
        go.Bar(name='Principal Value',x=sme_p['principal'],y=sme_p['_id'])
    ]
)
fig0.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Principal Value", xref="paper",x=0))
st.plotly_chart(fig0,use_container_width=True)


d1,d2=st.columns(2)
fig=go.Figure(
    data=[
        go.Bar(name='Safe Created',x=safe_month['month'],y=safe_month['_id'])
    ]
)
fig.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Vaults Created by Month", xref="paper",x=0))
d1.plotly_chart(fig,use_container_width=True)

fig2=go.Figure(
    data=[
        go.Bar(name='Total Principal',x=prin_month['month'],y=prin_month['principal'])
    ]
)
fig2.update_layout(xaxis=dict(showgrid=False), yaxis={'showgrid':False, 'visible': False}, title=go.layout.Title(text=f"Total Principal by Month", xref="paper",x=0))
d2.plotly_chart(fig2,use_container_width=True)