import os
from flask import Flask,render_template, request,jsonify
from core import gen
from encode_decode import Encode_decode
from io import StringIO
import ast
import pandas as pd
from comparison_report import chisq,ks_test,cat_cols_visual,dist_plot_num1,within_cols,plots,stat_test,PDF,construct
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chisquare
from scipy.stats import ks_2samp
import azure_actions
from azure_actions import upload_file_to_blob,download_blob_into_df, upload_df_to_blob, upload_file_to_blob
import pickle
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

@app.route('/upload_file', methods =['POST'])
def upload_file():

        file = request.files['file']   
        df = pd.read_csv(file)
        #global orig_dtypes       
        orig_dtypes = df.dtypes.to_dict()
        with open('orig_dtypes.pickle', 'wb') as pkl:
            pickle.dump(orig_dtypes, pkl) 
        upload_df_to_blob('real.csv', df, azure_actions.container_client_in)
        return jsonify({"upload status":"success"})

    

@app.route('/encode_data', methods =['POST'])
def encode_data():
    samples =  request.args.get('samples')
    primary_key = request.args.get('primary_key')
    anonymize_fields = request.args.get('anonymize_fields')
    num_cols =  request.args.get('num_cols')
    cat_cols =  request.args.get('cat_cols')
    params = {'samples':samples,'primary_key':primary_key,'anonymize_fields':anonymize_fields,'num_cols':num_cols,'cat_cols':cat_cols}      
    real_df = download_blob_into_df('synth-input','real.csv')  
    params = {"anonymize_fields" :anonymize_fields,  "cat_cols" :cat_cols, "num_cols" : num_cols}   
    fake_df = pd.DataFrame()    
    ed = Encode_decode(params,real_df,fake_df)
    #global enc_df
    enc_df = ed.encode()
    with open('enc_df.pickle', 'wb') as pkl:
         pickle.dump(enc_df, pkl) 
    upload_df_to_blob('enc_df.csv', enc_df, azure_actions.container_client_in)    
    #global cat_col_map
    cat_col_map = ed.handle_cat_cols()
    with open('cat_col_map.pickle', 'wb') as pkl:
         pickle.dump(cat_col_map, pkl) 
    
    return jsonify({"encode status":"success"})


@app.route('/train_model', methods =['POST'])
def train_model():
           
    samples =  request.args.get('samples')
    primary_key = request.args.get('primary_key')
    anonymize_fields = request.args.get('anonymize_fields')
    num_cols =  request.args.get('num_cols')
    cat_cols =  request.args.get('cat_cols')
    with open('enc_df.pickle', 'rb') as handle:
         enc_df = pickle.load(handle)
         enc_train_df = enc_df
         fake_df_raw = gen(enc_train_df, samples,primary_key, anonymize_fields,num_cols, cat_cols)
         upload_df_to_blob('fake_df_raw.csv', fake_df_raw, azure_actions.container_client_out)
    return jsonify({"training status":"complete"})


@app.route('/gen_output', methods =['POST'])
def view_output():

    anonymize_fields = request.args.get('anonymize_fields')
    num_cols =  request.args.get('num_cols')
    cat_cols =  request.args.get('cat_cols')      
    params = {"anonymize_fields" :anonymize_fields,  "cat_cols" :cat_cols, "num_cols" : num_cols}   
    real_df = pd.DataFrame()
    fake_df_raw = download_blob_into_df('synth-output','fake_df_raw.csv')
    ed = Encode_decode(params,real_df,fake_df_raw)
    with open('cat_col_map.pickle', 'rb') as handle:
         cat_col_map = pickle.load(handle)
         fake_final =ed.decode(cat_col_map)
    with open('orig_dtypes.pickle', 'rb') as handle:
         orig_dtypes = pickle.load(handle)
         fake_final = fake_final.astype(orig_dtypes)
         upload_df_to_blob('fake_final.csv', fake_final, azure_actions.container_client_out)
    return jsonify({"decode status":"success"})

@app.route('/',methods=['GET'])
def hello():
    return jsonify({"result":"app is deployed","Steps":"1.Upload file using upload_file url\n2.Encode file using encode\n3.Train model on encoded data using train_model\n4.Decode data using gen_output\n5.Generate statistical reports using post_processing url"})

@app.route('/post_processing', methods =['POST'])
def post_processing():
    
    file = request.files['file']
    #file2=request.files['synthetic_data']
    
    num_cols =  request.args.get('num_cols')
    cat_cols =  request.args.get('cat_cols')
    num_cols1=ast.literal_eval(num_cols)
    cat_cols1=ast.literal_eval(cat_cols)
    real_df=pd.read_csv(file)
    synthetic_df=download_blob_into_df('synth-output','fake_df_raw.csv')
    
    plots(real_df,synthetic_df,cat_cols1,num_cols1)
    stat_test(real_df,synthetic_df,cat_cols1,num_cols1)
    plots_per_page = construct()
    pdf = PDF()

    for elem in plots_per_page:
        pdf.print_page(elem)
    
    pdf.output('Comparison_Visual_Report.pdf', 'F')
    upload_file_to_blob('Comparison_Visual_Report.pdf', 'Comparison_Visual_Report.pdf', azure_actions.container_client_out)
    upload_file_to_blob('statistical_comparison.txt', 'statistical_comparison.txt', azure_actions.container_client_out)
    return jsonify({"Post Processing Status":"success"})
    


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug = True, host="0.0.0.0", port=port)
        
  
