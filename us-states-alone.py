from tqdm import tqdm
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import time
import random
import html5lib
import re
import scipy.stats as st
from pandas.core.frame import DataFrame
import copy
import math
import datetime
import pickle
import pandas
import json
from pprint import pprint
from urllib import request
#resp = request.urlopen('https://covidtracking.com/api/v1/states/daily.json')
#proxies = {'http': 'http://proxy.example.com:8080/'}
#opener = request.FancyURLopener(proxies)
headers = { 'Connection': 'close'}
# proxies={'http':'http://127.0.0.1:10080','https':'http://127.0.0.1:10080'}
a=0
o=0
while a==0:
    o+=1
    try:
        resp = requests.get('https://covidtracking.com/api/v1/states/daily.json')
        a=1
    except:
        print('error{}'.format(o))
        a=0

state_data=resp.json()#json.loads(resp.read().decode())
print('stage 1 finished')
import datetime
x0=datetime.date.today()
x1=datetime.date.today()-datetime.timedelta(days=1)
x2=datetime.date.today()-datetime.timedelta(days=2)
x3=datetime.date.today()-datetime.timedelta(days=3)
x4=datetime.date.today()-datetime.timedelta(days=4)
x5=datetime.date.today()-datetime.timedelta(days=5)
x6=datetime.date.today()-datetime.timedelta(days=6)
x7=datetime.date.today()-datetime.timedelta(days=7)
x8=datetime.date.today()-datetime.timedelta(days=8)
x9=datetime.date.today()-datetime.timedelta(days=9)
# run_time
ts=[]
ts.append(x0.__format__('%Y%m%d'))
ts.append(x1.__format__('%Y%m%d'))
ts.append(x2.__format__('%Y%m%d'))
ts.append(x3.__format__('%Y%m%d'))
ts.append(x4.__format__('%Y%m%d'))
ts.append(x5.__format__('%Y%m%d'))
ts.append(x6.__format__('%Y%m%d'))
ts.append(x7.__format__('%Y%m%d'))
ts.append(x8.__format__('%Y%m%d'))
ts.append(x9.__format__('%Y%m%d'))
print(ts)
id_names={'Alabama': 'AL',
 'Alaska': 'AK',
 'Arizona': 'AZ',
 'Arkansas': 'AR',
 'California': 'CA',
 'Colorado': 'CO',
 'Connecticut': 'CT',
 'Delaware': 'DE',
'District Of Columbia':'DC',
 'Florida': 'FL',
 'Georgia': 'GA',
 'Hawaii': 'HI',
 'Idaho': 'ID',
 'Illinois': 'IL',
 'Indiana': 'IN',
 'Iowa': 'IA',
 'Kansas': 'KS',
 'Kentucky': 'KY',
 'Louisiana': 'LA',
 'Maine': 'ME',
 'Maryland': 'MD',
 'Massachusetts': 'MA',
 'Michigan': 'MI',
 'Minnesota': 'MN',
 'Mississippi': 'MS',
 'Missouri': 'MO',
 'Montana': 'MT',
 'Nebraska': 'NE',
 'Nevada': 'NV',
 'New Hampshire': 'NH',
 'New Jersey': 'NJ',
 'New Mexico': 'NM',
 'New York': 'NY',
 'North Carolina': 'NC',
 'North Dakota': 'ND',
 'Ohio': 'OH',
 'Oklahoma': 'OK',
 'Oregon': 'OR',
 'Pennsylvania': 'PA',
 'Rhode Island': 'RI',
 'South Carolina': 'SC',
 'South Dakota': 'SD',
 'Tennessee': 'TN',
 'Texas': 'TX',
 'Utah': 'UT',
 'Vermont': 'VT',
 'Virginia': 'VA',
 'Washington': 'WA',
 'West Virginia': 'WV',
 'Wisconsin': 'WI',
 'Wyoming': 'WY'}
from tqdm import tqdm
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import time
import random
import html5lib
import re
import scipy.stats as st
from pandas.core.frame import DataFrame
import copy
import math
import datetime

#url='https://www.worldometers.info/coronavirus/#countries'
url='https://www.worldometers.info/coronavirus/country/us/'
a=requests.get(url)
soup = BeautifulSoup(a.content,'html5lib')
x=soup.body.find_all('tr', attrs={'style': ''})
# 190 210
def find_start_yesterday(i,j):
    for start in range(i,j):
        one=x[start]
        two=x[start+1]
        l1=one.find_all('a',attrs={'class':'mt_a'})
        l2=two.find_all('a',attrs={'class':'mt_a'})
        if l1==[] or l2==[]:
            continue
        s1=str(l1[0])
        s2=str(l2[0])
        coun1=s1.split('/')
        coun2=s2.split('/')
        if coun1[3]=='texas' or coun1[3]=='california':
            return start
#385 410
def find_end_yesterday(i,j):
    for end in range(i,j):
        final_pre=x[end-1]
        final=x[end]
        l1=final_pre.find_all('a',attrs={'class':'mt_a'})
        l2=final.find_all('a',attrs={'class':'mt_a'})
        if l1==[] or l2==[]:
            continue
        s1=str(l1[0])
        s2=str(l2[0])
        coun1=s1.split('/')
        coun2=s2.split('/')
        if (coun1[3]=='district-of-columbia' and coun2[3]=='vermont') or (coun2[3]=='district-of-columbia' and coun1[3]=='vermont'):
            return end+1
end=find_end_yesterday(80,200)
start=find_start_yesterday(64,80)
print('start:{}\tend:{}'.format(start,end))
col_name=['0','#','2','Country,Other','TotalCases',
          '5','NewCases','7','TotalDeaths',
          'NewDeaths','10','TotalRecovered','12','ActiveCases','Tot Cases/1M pop',
 'Deaths/1M pop','16','TotalTests','Tests/1M pop','19','Pop','21','source','23','24','Cases Per 100K Population',
          'Tests Per 100K Population','Active Cases Per 100k Population','Total Test:Positive Ratio','New Positive%',
          'Case Fatality Rate%','New Confirmed Case Growth Rate','New Death Case Growth Rate','Average daily cases per 100,000 people in the past week',
 'New Test','NPI','key-id','Country/District','Region','field','7 days inc cases','7 days inc deaths']
raw_data=[]
for i in tqdm(range(start,end)):
#    time.sleep(2)
    text_source=x[i]
    l=text_source.find_all('a',attrs={'class':'mt_a'})
    if l==[]:
        continue
    s=str(l[0])
    coun=s.split('/')
    url='https://www.worldometers.info/coronavirus/usa/'+coun[3]+'/'
    # a=requests.get(url,proxies=proxies,headers = headers)
    a=''
    while a=='':
        try:
            a=requests.get(url,headers=headers)
        except:
            a=''
    soup = BeautifulSoup(a.content,'html5lib')
    r=soup.body.find_all('script',attrs={'type':'text/javascript'})
    p=re.compile(r'categories: \[(.*?)\]',re.S)
    rs=re.findall(p,r[1].text)
    d=rs[0]
    str_pat = re.compile(r'\"(.*?)\"')
    d = str_pat.findall(d)
    date=d
    p1=re.compile(r'name: \'Cases\'.*?\[(.*?)\]',re.S)
    for j in range(10):
        try:
            rs=re.findall(p1,r[j].text)
            d=rs[0]
            d=re.sub(r'\"','',d)
            case=d.split(',')
        except:
            # print('{} cases is not{}'.format(coun[1],j))    
            continue
    p1=re.compile(r'name: \'Deaths\'.*?\[(.*?)\]',re.S)
    for j in range(10):
        try:
            rs=re.findall(p1,r[j].text)
            d=rs[0]
            d=re.sub(r'\"','',d)
            TD=d.split(',')
        except:
            continue
    j={'Date':date,'Total Cases':case,'Total Deaths':TD}
    hist_data_of_coun_i=pd.DataFrame(j)
    for k in range(len(hist_data_of_coun_i['Total Deaths'])):
        if hist_data_of_coun_i['Total Deaths'][k]=='null':
            data['Total Deaths'][k]=0
    hist_data_of_coun_i['Total Cases']=hist_data_of_coun_i['Total Cases'].astype(int)
    hist_data_of_coun_i['Total Deaths']=hist_data_of_coun_i['Total Deaths'].astype(int)
    hist_data_of_coun_i['case inc']=hist_data_of_coun_i['Total Cases'].diff()
    hist_data_of_coun_i['death inc']=hist_data_of_coun_i['Total Deaths'].diff()   
    #七日新增死亡与cases
    seven_cases=sum([hist_data_of_coun_i.loc[len(date)-i,'case inc'] for i in range(1,8)])
    seven_deaths=sum([hist_data_of_coun_i.loc[len(date)-i,'death inc'] for i in range(1,8)])

    inc1=hist_data_of_coun_i.loc[len(date)-1,'case inc']/(7*hist_data_of_coun_i.loc[len(date)-8,'case inc'])
    inc2=hist_data_of_coun_i.loc[len(date)-1,'death inc']/(7*hist_data_of_coun_i.loc[len(date)-8,'death inc'])

    inc_1=sum([hist_data_of_coun_i.loc[len(date)-i,'case inc'] for i in range(1,8)])/sum([hist_data_of_coun_i.loc[len(date)-i,'case inc'] for i in range(8,15)])
    inc_2=sum([hist_data_of_coun_i.loc[len(date)-i,'death inc'] for i in range(1,8)])/sum([hist_data_of_coun_i.loc[len(date)-i,'death inc'] for i in range(8,15)])
    
    adcp=sum([hist_data_of_coun_i.loc[len(date)-i,'case inc'] for i in range(1,8)])/7
    dd=hist_data_of_coun_i.shift(5)
    hist_data_of_coun_i['inc_p']=np.log(hist_data_of_coun_i['case inc']/dd['case inc'])/5
    hist_data_of_coun_i=hist_data_of_coun_i[~hist_data_of_coun_i.isin([np.nan, np.inf, -np.inf]).any(1)]
    da=hist_data_of_coun_i['inc_p'].values
    try:
        slope,intercept, r_value, p_value, std_err=st.linregress(list(range(30)), da[:30])
    except:
        slope=None
#     print(x[i].text)
    bo=x[i].text.split('\n')
    # print(bo)
    
    for h in range(len(bo)):
        bo[h]=bo[h].replace(',','')
        bo[h]=bo[h].replace('+','')
        bo[h]=bo[h].strip()
    
    bo[3]=bo[3].strip()
    try:
        region=id_names[bo[3]]
    except:
        region='missing'
#     print(region)
    if bo[4]=='':
        del bo[4]
    if bo[11]=='':
        del bo[11]
    if bo[20]=='':
        del bo[20]
    
    if bo[6]=='':
        new_cases=0
        for t in state_data:
            if t['state']==region:
                date_time=str(t['date'])
                if date_time == ts[1]:
                    new_cases=t['positiveIncrease']
                    break
        bo[6]=new_cases
    print(bo)
    if bo[9]=='':
        new_cases=0
        for t in state_data:
            if t['state']==region:
                date_time=str(t['date'])
                print(date_time)
                if date_time == ts[1]:
                    new_cases=t['deathIncrease']
                    break
        bo[6]=new_cases
        
    if bo[22]!='[projections]':
        del bo[22]
    #match-json
#     bo[3]=bo[3].strip()
#     try:
#         region=id_names[bo[3]]
#     except:
#         region='missing'
# #     print(region)
    new_test=1

    # test_7 days
    for t in state_data:
        if t['state']==region:
            date_time=str(t['date'])
            if date_time == ts[1]:
                new_test=t['totalTestResultsIncrease']
                break
    print(bo)
    #Cases Per 100K Population
    try:
        bo.append(int(bo[14])/10)
    except:
        continue
        # bo.append(np.nan)
        # print('lack one')
    #Tests Per 100K Population
    try: 
        bo.append(int(bo[18])/10)
    except:
        continue
        # bo.append(np.nan)
        # print('lack one')   
    #'Active Cases Per 100k Population'
    try:
        bo.append(int(bo[13])*100000/int(bo[20]))
    except:
        bo.append(np.nan)
        # print('lack one')
    
    #Total Test:Positive Ratio      
    bo.append(int(bo[4])/int(bo[17]))
    #'New Positive%'
    print(region)
    try:
        bo.append(int(bo[6])/new_test)
    except:
        bo.append(0)
    #Case Fatality Rate%  
    try:
        if bo[8]=='': 
            bo.append(0)
        else:
            bo.append(int(bo[8])/int(bo[4]))
    except:
        bo.append(np.nan)
#New Confirmed Case Growth Rate
#     try:
#         q=2
#         while (math.isnan(inc1) or inc1==np.inf) and q<=9:
#             inc1=hist_data_of_coun_i.loc[len(date)-q,'case inc']/(7*hist_data_of_coun_i.loc[len(date)-q-7,'case inc'])
# #             c=hist_data_of_coun_i.loc[len(date)-q,'case inc']
#             q+=1
# #         print(c)
#         if math.isnan(inc1):
#             bo.append(0)
#         elif inc1==np.inf:
#             bo.append(0.01)
#         else:
#             bo.append(inc1)
# #         print(inc1)
#     except:
#         bo.append(0)
#         # print('lack one')
# #     print(bo[27])
#     #New Death Case Growth Rate   
#     try:
#         q=2
#         while (math.isnan(inc2) or inc2==np.inf) and q<=9:
# #             print(inc2)
#             inc2=hist_data_of_coun_i.loc[len(date)-1,'death inc']/(7*hist_data_of_coun_i.loc[len(date)-8,'death inc'])
#             q+=1
# #         print(inc2)
#         if math.isnan(inc2):
#             bo.append(0)
#         elif inc2==np.inf:
#             bo.append(0.1)
#         else:
#             bo.append(inc2)
#     except:
#         bo.append(0)
    if math.isnan(inc_1) or inc_1=='':
        bo.append(0)
    elif inc_1==np.inf:
        bo.append(0.01)
    else:
        bo.append(inc_1)
    print(bo[-1])
    #New Sum Death Case Growth Rate
    if math.isnan(inc_2) or inc_2=='':
        bo.append(0)
    elif inc_2==np.inf:
        bo.append(0.1)
    else:
        bo.append(inc_2)
    print(bo[-1])
    #Average daily cases per 100,000 people in the past week
    bo.append(adcp*100000/int(bo[20]))
    # New Test
    bo.append(new_test)
    #NPI
    if slope==np.inf or math.isnan(slope):
        bo.append(0)
    else:
        bo.append(slope)
    bo.append(coun[3])
    bo.append(region)
    bo.append('No')
    bo.append('us')
    bo.append(seven_cases)
    bo.append(seven_deaths)
    # if bo[20]=='':
    #     del bo[20]
    print(len(bo))
    print(bo)
    raw_data.append(bo)
raw_data=DataFrame(raw_data,columns=col_name)
brief_raw_data=raw_data[['Country,Other','key-id','Country/District','Region','field','TotalCases',
          'NewCases','TotalDeaths',
          'NewDeaths','ActiveCases','Tot Cases/1M pop',
 'Deaths/1M pop','TotalTests','Tests/1M pop','Pop','Cases Per 100K Population',
          'Tests Per 100K Population','Active Cases Per 100k Population','Total Test:Positive Ratio','New Positive%',
          'Case Fatality Rate%','New Confirmed Case Growth Rate','New Death Case Growth Rate','Average daily cases per 100,000 people in the past week',
 'New Test','NPI','7 days inc cases','7 days inc deaths']]
brief_raw_data['week death rate']=brief_raw_data['7 days inc deaths']/brief_raw_data['7 days inc cases']
tf=copy.deepcopy(brief_raw_data)
tf3=tf[['Country,Other','key-id','Country/District','Region','field','TotalCases','Cases Per 100K Population',
          'Tests Per 100K Population','Active Cases Per 100k Population','Total Test:Positive Ratio','New Positive%',
          'Case Fatality Rate%','New Confirmed Case Growth Rate','New Death Case Growth Rate','Average daily cases per 100,000 people in the past week','NPI']]
# frames=[tf2,tf3]
# tf3 = pd.concat(frames)


x='Tests Per 100K Population'
df=tf3[['Country,Other',x]]
df2=df.sort_values(x,ascending=False,inplace=False)
df2 = df2.reset_index(drop=True)
df2['cum']=df.index+1
df2['cum_prob']=100*df2['cum']/max(df2['cum'])
df3=pd.merge(df,df2,on=['Country,Other'])
tf3['IND_'+x]=0
for h in list(tf3['Country,Other'].values):
    tf3.loc[tf3['Country,Other']==h,'IND_'+x]=df3.loc[df3['Country,Other']==h,'cum_prob'].values[0]

for x in ['Cases Per 100K Population','Active Cases Per 100k Population','Total Test:Positive Ratio','New Positive%','Case Fatality Rate%','New Confirmed Case Growth Rate','New Death Case Growth Rate','Average daily cases per 100,000 people in the past week','NPI']:
    df=tf3[['Country,Other',x]]
    print(df[x])
    df2=df.sort_values(x,inplace=False)
    df2 = df2.reset_index(drop=True)
    df2['cum']=df.index+1
    df2['cum_prob']=100*df2['cum']/max(df2['cum'])
    df3=pd.merge(df,df2,on=['Country,Other'])
    tf3['IND_'+x]=0
    for h in list(tf3['Country,Other'].values):
        tf3.loc[tf3['Country,Other']==h,'IND_'+x]=df3.loc[df3['Country,Other']==h,'cum_prob'].values[0]


tf3['Comprehensive Index']=0.15*tf3['IND_Cases Per 100K Population']+0.08*tf3['IND_Tests Per 100K Population']+0.2*tf3['IND_Active Cases Per 100k Population']+0.1*tf3['IND_Total Test:Positive Ratio']+0.13*tf3['IND_New Positive%']+0.05*tf3['IND_Case Fatality Rate%']+ 0.22*tf3['IND_New Confirmed Case Growth Rate']+0.07*tf3['IND_New Death Case Growth Rate']
today=datetime.datetime.now()

rrr=tf3[tf3['field']=='us']
tf4=rrr[['Country/District','TotalCases','IND_Cases Per 100K Population','IND_Tests Per 100K Population','IND_Total Test:Positive Ratio',
'IND_New Positive%','IND_Case Fatality Rate%','IND_New Confirmed Case Growth Rate','IND_New Death Case Growth Rate','IND_Active Cases Per 100k Population',
'IND_NPI','IND_Average daily cases per 100,000 people in the past week','Comprehensive Index']]
tf_c=copy.deepcopy(tf4)
tf_c_rename=tf_c.rename({'TotalCases':'TOTAL CASE','IND_Cases Per 100K Population':'IND1_Cases Per 100K Population','IND_Tests Per 100K Population':'IND2_Tests Per 100K Population',
'IND_Active Cases Per 100k Population':'IND8_Active Cases Per 100k Population','IND_Total Test:Positive Ratio':'IND3_Total Test:Positive Ratio',
'IND_New Positive%':'IND4_New Positive%','IND_Case Fatality Rate%':'IND5_Case Fatality Rate%','IND_New Confirmed Case Growth Rate':'IND6_New Confirmed Case Growth Rate',
'IND_New Death Case Growth Rate':'IND7_New Death Case Growth Rate','IND_NPI':'NPI'},axis='columns')


tf_c_rename.to_excel('US_alone_index_{}.xlsx'.format(today),sheet_name=ts[1],index=False)
tf3.to_excel('US_alone_raw_index_{}.xlsx'.format(today),sheet_name=ts[1],index=False)

url='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/us_state_vaccinations.csv'
a=requests.get(url,headers=headers)
with open("us_vacc.csv",'wb') as f:
    f.write(a.content)
vacc = pd.read_csv('us_vacc.csv',keep_default_na=False)
ct = list(dict(vacc['location'].value_counts()).keys())
name= list(brief_raw_data['Country,Other'].values)
name=list(set(name).intersection(set(ct)))
name.append('New York State')
name.append('District of Columbia')
for x in ['total_vaccinations','people_vaccinated','people_fully_vaccinated']:
    vacc[x]=vacc[x].replace('',0)
    vacc[x]=vacc[x].astype(float)
    vacc[x]=vacc[x].astype(int)
img = dict()
for i in name:
    dt = vacc[vacc['location']==i]
    
    d=[]
    for x in ['total_vaccinations','people_vaccinated','people_fully_vaccinated']:
        d.append(max(dt[x]))
    if i == 'New York State':
        img['New York']=d
    if i == 'District of Columbia':
        img['District Of Columbia']=d
    else:
        img[i]=d


brief_raw_data['total_vaccinations']=0
brief_raw_data['people_vaccinated']=0
brief_raw_data['people_fully_vaccinated']=0
for i in img.keys():
    brief_raw_data.loc[(brief_raw_data['Country,Other']==i),'total_vaccinations'] = int(img[i][0])
    brief_raw_data.loc[(brief_raw_data['Country,Other']==i),'people_vaccinated'] = int(img[i][1])
    brief_raw_data.loc[(brief_raw_data['Country,Other']==i),'people_fully_vaccinated'] = int(img[i][2])
brief_raw_data['Pop']=brief_raw_data['Pop'].astype(int)
brief_raw_data['vacc_per_100']=brief_raw_data['total_vaccinations']*100/brief_raw_data['Pop']
brief_raw_data['cases_per_100']=brief_raw_data['Cases Per 100K Population']/1000
brief_raw_data['total_immune']=brief_raw_data['cases_per_100']+brief_raw_data['vacc_per_100']*0.9   
brief_raw_data.to_excel('US_alone_rawdata_{}.xlsx'.format(today),sheet_name=ts[1],index=False)
