import pandas as pd 
import numpy as np
import os 
from env import username, host, password
from sklearn.model_selection import train_test_split


    # Acquiring telco_churn data
def get_connection(db, username=username, host=host, password=password):
    '''
    Creates a connection URL
    '''
    return f'mysql+pymysql://{username}:{password}@{host}/{db}'


def new_telco_churn_data():
    '''
    Returns telco_churn into a dataframe
    '''
    sql_query = '''select * from customers
    join internet_service_types using(internet_service_type_id)
    join contract_types using(contract_type_id)
    join payment_types using(payment_type_id)'''
    df = pd.read_sql(sql_query, get_connection('telco_churn'))
    return df 


def get_telco_churn_data():
    '''get connection, returns telco_churn into a dataframe and creates a csv for us'''
    if os.path.isfile('telco_churn.csv'):
        df = pd.read_csv('telco_churn.csv', index_col=0)
    else:
        df = new_telco_churn_data()
        df.to_csv('telco_churn.csv')
    return df
    
    
    def telco_churn_split(df):
    #splitting our data  
        train_validate, test = train_test_split(df, test_size=.2, 
                                        random_state=123, 
                                        stratify=df.churn)
        train, validate = train_test_split(train_validate, test_size=.3, 
                                   random_state=123, 
                                   stratify=train_validate.churn)
    return train, validate, test


def prep_telco_churn(df):
    train, validate, test = telco_churn_split(df)
    return train, validate, test
    
    
    def clean_telco_churn(df):
        '''cleans our telco churn data for us, and makes dummies'''
    #df.replace('', np.nan, regex = True)
    #is.na.sum()
    #total_charges had 1 null values. I changed them into a float, and filled nulls with the mean of all total charges.
    df.total_charges = pd.to_numeric(df.total_charges, errors='coerce').astype('float64')
    df.total_charges = df.total_charges.fillna(value=df.total_charges.mean()).astype('float64')
    df.replace('No internet service', 'No', inplace=True)
    df.replace('No phone service', 'No', inplace = True)

    #making dummies for columns I found appropriate to do so. I just went down the list. 
    df["is_female"] = df.gender == "Female"
    df['is_female'] = (df['is_female']).astype(int)

    df["partner"] = df.partner == "Yes"
    df['partner'] = (df['partner']).astype(int)

    df["dependents"] = df.dependents == "Yes"
    df['dependents'] = (df['dependents']).astype(int)

    df["phone_service"] = df.phone_service == "Yes"
    df['phone_service'] = (df['phone_service']).astype(int)

    df["streaming_tv"] = df.streaming_tv == "Yes"
    df['streaming_tv'] = (df['streaming_tv']).astype(int)

    df["streaming_movies"] = df.streaming_movies == "Yes"
    df['streaming_movies'] = (df['streaming_movies']).astype(int)

    df["paperless_billing"] = df.paperless_billing == "Yes"
    df['paperless_billing'] = (df['paperless_billing']).astype(int)

    df["churn"] = df.churn == "Yes"
    df['churn'] = (df['churn']).astype(int)

    df["multiple_lines"] = df.multiple_lines == "Yes"
    df['multiple_lines'] = (df['multiple_lines']).astype(int)

    df["online_security"] = df.online_security == "Yes"
    df['online_security'] = (df['online_security']).astype(int)

    df["online_backup"] = df.online_backup == "Yes"
    df['online_backup'] = (df['online_backup']).astype(int)

    df["device_protection"] = df.device_protection == "Yes"
    df['device_protection'] = (df['device_protection']).astype(int)

    df["tech_support"] = df.tech_support == "Yes"
    df['tech_support'] = (df['tech_support']).astype(int)

    #dropping redundant columns
    df = df.drop(columns =['payment_type_id', 'contract_type_id', 'internet_service_type_id'])
    df = df.drop(columns=['gender'])
    #making a dummy df, and combining it back to the original df. Dropping redundant columns again.
    dummy_df = pd.get_dummies(df[['internet_service_type', 'contract_type','payment_type']], drop_first=False)
    dummy_df = dummy_df.rename(columns={'internet_service_type_DSL': 'dsl',
                                   'internet_service_type_Fiber optic': 'fiber_optic',
                                   'internet_service_type_None': 'no_internet',
                                   'contract_type_Month-to-month': 'monthly',
                                   'contract_type_One year': 'one_year',
                                   'contract_type_Two year': 'two_year',
                                   'payment_type_Bank transfer (automatic)': 'bank_transfer',
                                   'payment_type_Credit card (automatic)': 'credit_card',
                                   'payment_type_Electronic check': 'electronic_check',
                                   'payment_type_Mailed check': 'mailed_check'})
    df = pd.concat([df, dummy_df], axis =1)
    df = df.drop(columns=['internet_service_type','contract_type','payment_type'])

    return df 


#Zillow

def new_zillow():
    sql_query ='''select bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount, fips from properties_2017
 	join propertylandusetype using(propertylandusetypeid)
 	where propertylandusetypeid = 261'''
    df = pd.read_sql(sql_query, get_connection('zillow'))
    return df 

def get_zillow_data():
    '''get connection, returns Zillow into a dataframe and creates a csv for us'''
    if os.path.isfile('zillow.csv'):
        df = pd.read_csv('zillow.csv', index_col=0)
    else:
        df = new_zillow()
        df.to_csv('zillow.csv')
    return df

def wrangle_zillow():
    '''
    Read zillow csv file into a pandas DataFrame,
    only returns desired columns and single family residential properties,
    drop any rows with Null values, drop duplicates,
    return cleaned zillow DataFrame.
    '''
    # Acquire data from csv file.
    df = pd.read_csv('zillow.csv')
    
    # Drop nulls
    df = df.dropna()
    
    # Drop duplicates
    df = df.drop_duplicates()
    # drop unamed column
    df = df.drop(columns=['Unnamed: 0'])
    return df

def split_zillow(df):
    '''
    Takes in a cleaned df of zillow data and splits the data appropriatly into train, validate, and test.
    '''
    
    train_val, test = train_test_split(df, train_size =  0.8, random_state = 123)
    train, validate = train_test_split(train_val, train_size =  0.7, random_state = 123)
    return train, validate, test

def remove_outliers(df, k, col_list):
    ''' remove outliers from a list of columns in a dataframe 
        and return that dataframe
    '''
    
    for col in col_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles
        
        iqr = q3 - q1   # calculate interquartile range
        
        upper_bound = q3 + k * iqr   # get upper bound
        lower_bound = q1 - k * iqr   # get lower bound

        # return dataframe without outliers
        
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        
    return df

