# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 20:28:00 2020

@author: Shriram
"""
import pandas as pd
import numpy as np
from sklearn import preprocessing
import matplotlib.pyplot as plt 
plt.rc("font", size=14)
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import seaborn as sns
sns.set(style="white")
sns.set(style="whitegrid", color_codes=True)
import matplotlib.pyplot as plt

from sklearn.metrics import classification_report, confusion_matrix

full_data = pd.read_csv('C:/Users/shrir/OneDrive - Northeastern University/CSYE7200 Big Data Systems Enginnering with Scala - Project/Data/person/sub/2017_sub.csv')
full_data['FATALITY_BIN'] = np.where(full_data['NUMB_FATAL_INJR']> 0, 1, 0)


data = full_data[["LAT","LON","DISTRICT_NUM","LCLTY_NAME","OWNER_ADDR_CITY_TOWN","OWNER_ADDR_STATE","VEHC_REG_STATE","WEATH_COND_DESCR","ROAD_SURF_COND_DESCR","MANR_COLL_DESCR","FIRST_HRMF_EVENT_DESCR","DRVR_CNTRB_CIRC_CL","VEHC_CONFG_DESCR","HIT_RUN_DESCR","AGE_DRVR_YNGST","AGE_DRVR_OLDEST","DRVR_DISTRACTED_CL","DRIVER_AGE","DRIVER_DISTRACTED_TYPE_DESCR","DRVR_LCN_STATE","DRUG_SUSPD_TYPE_DESCR","SFTY_EQUP_DESC_1","SFTY_EQUP_DESC_2","ALC_SUSPD_TYPE_DESCR","FATALITY_BIN"]]
data['DISTRICT_NUM'] = data['DISTRICT_NUM'].astype(str)

cat_cols_data = data[["DISTRICT_NUM","LCLTY_NAME","OWNER_ADDR_CITY_TOWN","OWNER_ADDR_STATE","VEHC_REG_STATE","WEATH_COND_DESCR","ROAD_SURF_COND_DESCR","MANR_COLL_DESCR","FIRST_HRMF_EVENT_DESCR","DRVR_CNTRB_CIRC_CL","VEHC_CONFG_DESCR","HIT_RUN_DESCR","AGE_DRVR_YNGST","AGE_DRVR_OLDEST","DRVR_DISTRACTED_CL","DRIVER_DISTRACTED_TYPE_DESCR","DRVR_LCN_STATE","DRUG_SUSPD_TYPE_DESCR","SFTY_EQUP_DESC_1","SFTY_EQUP_DESC_2","ALC_SUSPD_TYPE_DESCR"]]
dum_data = pd.get_dummies(cat_cols_data)
from sklearn.impute import SimpleImputer
imp = SimpleImputer(missing_values=np.nan, strategy='mean')
num_cols_data = data.drop(columns = ["DISTRICT_NUM","LCLTY_NAME","OWNER_ADDR_CITY_TOWN","OWNER_ADDR_STATE","VEHC_REG_STATE","WEATH_COND_DESCR","ROAD_SURF_COND_DESCR","MANR_COLL_DESCR","FIRST_HRMF_EVENT_DESCR","DRVR_CNTRB_CIRC_CL","VEHC_CONFG_DESCR","HIT_RUN_DESCR","AGE_DRVR_YNGST","AGE_DRVR_OLDEST","DRVR_DISTRACTED_CL","DRIVER_DISTRACTED_TYPE_DESCR","DRVR_LCN_STATE","DRUG_SUSPD_TYPE_DESCR","SFTY_EQUP_DESC_1","SFTY_EQUP_DESC_2","ALC_SUSPD_TYPE_DESCR","FATALITY_BIN"])
num_cols_data.isnull().values.any()
imp.fit(num_cols_data)
imp_num_cols_data = imp.transform(num_cols_data)
print(type(imp_num_cols_data))
imp_num_cols_data_df = pd.DataFrame(imp_num_cols_data, columns = ['LAT','LON','DRIVER_AGE'])
new_data_with_dum = data.join(dum_data)
final_table = new_data_with_dum.drop(columns = ["DISTRICT_NUM","LCLTY_NAME","OWNER_ADDR_CITY_TOWN","OWNER_ADDR_STATE","VEHC_REG_STATE","WEATH_COND_DESCR","ROAD_SURF_COND_DESCR","MANR_COLL_DESCR","FIRST_HRMF_EVENT_DESCR","DRVR_CNTRB_CIRC_CL","VEHC_CONFG_DESCR","HIT_RUN_DESCR","AGE_DRVR_YNGST","AGE_DRVR_OLDEST","DRVR_DISTRACTED_CL","DRIVER_DISTRACTED_TYPE_DESCR","DRVR_LCN_STATE","DRUG_SUSPD_TYPE_DESCR","SFTY_EQUP_DESC_1","SFTY_EQUP_DESC_2","ALC_SUSPD_TYPE_DESCR"])
final_table_num_imp = final_table.join(imp_num_cols_data_df, lsuffix='_left', rsuffix='_right')
final_table_num_imp = final_table_num_imp.drop(columns = ['LAT_left', 'LON_left', 'DRIVER_AGE_left'])
X=final_table_num_imp.drop(columns = ["FATALITY_BIN"])

y = final_table.loc[:, final_table.columns == 'FATALITY_BIN']
from imblearn.over_sampling import SMOTE
os = SMOTE(random_state=0)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
columns = X_train.columns
os_data_X,os_data_y=os.fit_sample(X_train, y_train)
os_data_X = pd.DataFrame(data=os_data_X,columns=columns )
os_data_y= pd.DataFrame(data=os_data_y,columns=['FATALITY_BIN'])
# we can Check the numbers of our data
print("length of oversampled data is ",len(os_data_X))
print("Number of no subscription in oversampled data",len(os_data_y[os_data_y['FATALITY_BIN']==0]))
print("Number of subscription",len(os_data_y[os_data_y['FATALITY_BIN']==1]))
print("Proportion of no subscription data in oversampled data is ",len(os_data_y[os_data_y['FATALITY_BIN']==0])/len(os_data_X))
print("Proportion of subscription data in oversampled data is ",len(os_data_y[os_data_y['FATALITY_BIN']==1])/len(os_data_X))

data_final = final_table_num_imp
data_final_vars=data_final.columns.values.tolist()
y=['FATALITY_BIN']
X=[i for i in data_final_vars if i not in y]
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression(max_iter=10)
rfe = RFE(logreg, 20)
rfe = rfe.fit(os_data_X, os_data_y.values.ravel())
print(rfe.support_)
print(rfe.ranking_)

##################################################################

import statsmodels.api as sm 
  
   
# building the model and fitting the data 
log_reg = sm.Logit(y_train, X_train).fit() 
print(log_reg.summary()) 

