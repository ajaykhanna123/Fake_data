import requests


###******************** ENTER USER PARAMETERS HERE**********************###

files ={ 'file': open('/home/Ayush23/synthetic_project/ydata/INPUT/claims.csv','rb')}

samples = '200'
primary_key = 'ClaimID'
anonymize_fields = '{"ClaimStartDt":"date"}'
#cat_cols="['DiagnosisGroupCode','Provider', 'AttendingPhysician']"
#num_cols="['InscClaimAmtReimbursed', 'BeneID']"

#Testing
cat_cols="['DiagnosisGroupCode','Provider']"
num_cols="['InscClaimAmtReimbursed']"


#*******************************************************************###


params ={'samples':samples,\
           'primary_key' :primary_key,\
           'anonymize_fields':anonymize_fields,\
           'num_cols':num_cols,\
           'cat_cols':cat_cols
             }

params1 ={'num_cols':num_cols,\
           'cat_cols':cat_cols
             }


###-----------------------Upload file-------------------###
r = requests.post(url = 'http://127.0.0.1:5000/upload_file',files=files)
#-----------File will be uploaded to UPLOAD folder------####
r.text


###----------------------Encode data-------------------###
r = requests.post(url = 'http://127.0.0.1:5000/encode_data',params = params)
###--------------Encoded file stored in TMP folder------------###
r.text


###----------------------Train model-------------------###
r = requests.post(url = 'http://127.0.0.1:5000/train_model',params = params)
###-----------Trained model binary stored in GAN folder------###
###-----------View epochs in app.ipynb------------------###
r.text


###----------------------Generate fake data-------------###
r = requests.post(url = 'http://127.0.0.1:5000/gen_output',params = params)
r.text


###---------------------Generate Report-----------------###
r = requests.post(url = 'http://127.0.0.1:5000/post_processing',files=files,params=params1)

r.text
#-------------------------------------------------------###


