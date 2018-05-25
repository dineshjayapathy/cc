
'''
Requirement: Python 3.4 or greater
Libraries to be installed: pandas
use the following command in command prompt to install it.
pip install pandas
'''

import pandas as pd
import datetime
import time
import os
import ast

inputfile = os.path.dirname(os.path.realpath('__file__'))
inputfilename = os.path.join(inputfile, '../input/input.txt')
with open(inputfilename,'r') as f:
    ip=f.read().replace('\n', '')
ip_list=ast.literal_eval(ip)

class LTV:
    def ingest(self,e):
        order=[]
        customer=[]
        site_visit=[]
        image_upload=[]
        for item in e:
            if item['type']=='ORDER':
                order.append(item)
            if item['type']=='CUSTOMER':
                customer.append(item)
            if item['type']=='SITE_VISIT':
                site_visit.append(item)
            if item['type']=='IMAGE_UPLOAD':
                image_upload.append(item)

        self.df_order=pd.DataFrame(order)
        self.df_customer=pd.DataFrame(customer)
        self.df_customer.rename(columns={'key': 'customer_id'}, inplace=True)
        self.df_site_visit=pd.DataFrame(site_visit)
        self.df_image_upload=pd.DataFrame(image_upload)

    def TopXSimpleLTVCustomers(self,n):

        self.df_order['total_amount'] = self.df_order['total_amount'].map(lambda x: float(x.rstrip(' USD')))
        sumtotal=self.df_order.groupby('customer_id')['total_amount'].sum()
        df_sumtotal=sumtotal.to_frame().reset_index()
        df_sumtotal.rename(columns={'total_amount': 'sumtotal'}, inplace=True)
        # print(df_sumtotal)

        visit_count=self.df_site_visit.groupby('customer_id')['key'].count()
        df_countv=visit_count.to_frame().reset_index()
        df_countv.rename(columns={'key': 'countv'}, inplace=True)
        # print(df_countv)

        #Expense per visit calculation
        df_epv=pd.merge(df_countv,df_sumtotal, how="inner")
        df_epv['epv']=df_epv['sumtotal']/df_epv['countv']
        # print(df_epv)

        #Site visits per week calculation
        self.df_site_visit['dt']=self.df_site_visit['event_time'].str[:10]
        self.df_site_visit['wk_intr'] = self.df_site_visit['dt'].map(lambda x: time.strptime(x, "%Y-%m-%d"))
        self.df_site_visit['year']=self.df_site_visit['event_time'].str[:4]
        self.df_site_visit['wk'] = self.df_site_visit['wk_intr'].map(lambda x: 1+int(time.strftime("%U", x)))

        vpw_inner=self.df_site_visit.groupby(['customer_id','wk','year'])['key'].count()
        df_vpw_inner=vpw_inner.to_frame().reset_index()
        df_vpw_inner.rename(columns={'key': 'pg_ct'}, inplace=True)

        vpw=df_vpw_inner.groupby('customer_id')['pg_ct'].mean()
        df_vpw=vpw.to_frame().reset_index()
        # print(df_vpw)

        #LTV calculation
        df_ltv=pd.merge(df_epv,df_vpw, how="inner")
        df_ltv['ltv']=52*df_ltv['epv']*df_ltv['pg_ct']*10
        df_ltv.sort_values(by='ltv',ascending=False,inplace=True)
        # print(df_ltv)
        df_ltv['ltv']='$'+df_ltv['ltv'].astype(str)
        df_ltv=df_ltv.drop('countv',1)
        df_ltv=df_ltv.drop('sumtotal',1)
        df_ltv=df_ltv.drop('epv',1)
        df_ltv=df_ltv.drop('pg_ct',1)
        df_topx=pd.merge(df_ltv,self.df_customer, how="inner").head(n)
        df_topx=df_topx.drop('event_time',1)
        df_topx =df_topx.drop('adr_city',1)
        df_topx =df_topx.drop('adr_state',1)
        df_topx =df_topx.drop('type',1)
        df_topx =df_topx.drop('verb',1)
        df_topx.to_csv('../output/output.txt',index=False)
        return df_topx
#Function calls
a=LTV()
a.ingest(ip_list)
print(a.TopXSimpleLTVCustomers(10))







