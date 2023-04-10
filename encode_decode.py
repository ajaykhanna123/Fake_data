from clean_pandas import CleanPandas
import pandas as pd
import datetime
import logging
import time
from faker import Faker
import json
import ast
import pickle
from sklearn.preprocessing import LabelEncoder
from dateutil.relativedelta import relativedelta
import azure_actions

le = LabelEncoder()
fake = Faker()
fake.seed_instance(4321)

class Encode_decode:
    def __init__(self, json_request, real_df, fake_df):
        '''
        Initializing variables
        '''
        logging.info('Encoding/decoding in progress...')
        self.cat_col_map = {}
        self.anonymize_fields = ast.literal_eval(json_request.get("anonymize_fields"))
        self.cat_cols = ast.literal_eval(json_request.get("cat_cols"))
        self.num_cols = ast.literal_eval(json_request.get("num_cols"))
        
        self.real_df = real_df
        self.fake_df = fake_df
        
      
    @staticmethod
    def fakes(category):
        mapper = {'first_name' : fake.unique.first_name(),
                  'last_name' : fake.unique.last_name(),
                  'phone_number' : fake.unique.phone_number(),
                  'credit_card_number' : fake.unique.credit_card_number(),
                  'address' : fake.unique.address(),
                  'postcode' : fake.postcode(),
                  'ssn' : fake.ssn(),
                  'email' : fake.ascii_free_email(),
                  'bank_acc_no' : fake.bban()
                    }
        return mapper[category]
    
    
        
    def encode(self):       
        '''
        Encoding PHI/PII  
        Handling dates (01/01/1960 is treated as 0)
        
        Input:
        # anonymize_fields :{'myaddress':'address','myphone' :'phone'}                   
        # cat_cols = ['region', 'has_children?']
        # num_cols =['age', 'amount']

        Output:
        #creates encoded_df (to be passed to core)
        #stores mappings
        
        '''
        if (self.anonymize_fields!= {}):
            
            
            if ('date' in self.anonymize_fields.values()):
                date_col =list(self.anonymize_fields.keys())[list(self.anonymize_fields.values()).index('date')]

                self.real_df[date_col] = pd.to_datetime(self.real_df[date_col])
                self.real_df[date_col]  = ( self.real_df[date_col]  - pd.to_datetime('01/01/1960')).dt.days

            for column,category in self.anonymize_fields.items(): 
                if (category=='date'):
                    pass
                else:
                    values = [ Encode_decode.fakes(category) for i in range(len(self.real_df))]
                    self.real_df[column] = values
     
        return self.real_df

    def handle_cat_cols(self):
        '''
        Convert cat_cols to numeric
        Save mappings
        '''
        for col in self.cat_cols:     
          
            self.real_df[col] = le.fit_transform(self.real_df[col])
            self.cat_col_map[col] = dict(zip(le.classes_, range(len(le.classes_))))

        azure_actions.upload_df_to_blob('enc_df.csv',  self.real_df, azure_actions.container_client_in)
       
        return self.cat_col_map
     
    def decode(self, mappings):
        '''
        Convert numeric cols to cat
        Saves synthetic df
                
        # Input:
        #takes synthetic df as input
        #reads mappings

        # Output:
        #creates decoded synthetic df in OUT
        '''
        #{'area': {0: 0, 1: 1}, 'email': {'ayanand93@gmail.com': 0, 'aykohli@yahoo.com': 1}}
        if (self.anonymize_fields!= {}):
            if ('date' in self.anonymize_fields.values()):
                date_col =list(self.anonymize_fields.keys())[list(self.anonymize_fields.values()).index('date')]
                self.fake_df[date_col] = self.fake_df[date_col].apply(lambda x : datetime.date(1960, 1, 1) +  relativedelta(days=x))
                
        if (self.cat_cols!=[]):
            mappings = mappings
            for col in self.cat_cols:
                      mapping = mappings[col]
                      mapping = {v:k for k,v in mapping.items()}
                      self.fake_df[col] = self.fake_df[col].replace(mapping)
        return self.fake_df