
import pandas as pd
import ast
import warnings, os
warnings.filterwarnings('ignore')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' 
from ydata_synthetic.synthesizers.regular import RegularSynthesizer
from ydata_synthetic.synthesizers import ModelParameters, TrainParameters


def gen(enc_df,samples,primary_key, anonymize_fields,num_cols, cat_cols):

    #enc_df =encoded_train_df 
    anonymize_fields = ast.literal_eval(anonymize_fields)
    num_cols = ast.literal_eval(num_cols)
    cat_cols = ast.literal_eval(cat_cols)
    samples = int(samples)
   
    batch_size = 500
    epochs = 10
    learning_rate = 2e-4
    beta_1 = 0.5
    beta_2 = 0.9

    ctgan_args = ModelParameters(batch_size=batch_size,
                                 lr=learning_rate,
                                 betas=(beta_1, beta_2))

    train_args = TrainParameters(epochs=epochs)
    model = RegularSynthesizer(modelname='ctgan', model_parameters=ctgan_args)
   
    #data = pd.read_csv('TMP/enc_df.csv')
    model.fit(data=enc_df, train_arguments=train_args, num_cols=num_cols, cat_cols=cat_cols)

    #model.save('GAN/insurance_ctgan_model.pkl')
  
    
    
    #########################################################
    #    Loading and sampling from a trained synthesizer    #
    #########################################################
    #model = RegularSynthesizer.load('GAN/insurance_ctgan_model.pkl')
    fake_data_df = model.sample(samples)
       
   
    #new_data.to_csv('OUT/fake_data.csv',index=False)
    return(fake_data_df)

    