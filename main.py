
from flask import Flask,jsonify
from flask import request
app = Flask(__name__)
from Project_relational import relational_data
import pandas as pd
import json
@app.route('/split_csvs/', methods=['POST'])
def split_csvs():
    if request.method == 'POST':
        # FileStorage object wrapper
        files_list=[]
        files = request.files

        for key in files.keys():
            files_list.append((key,files[key]))

        # a=request.args.get('relationships')
        relation_list=[]
        param_values=request.args
        print(param_values)


        # for key in relations.keys():
        #     if key.startswith("relation"):
        #         relation_list.append(relations[key])
        # user_input_multi_table={'files':files_list,"relationships":relation_list}
        #
        #
        # b=relational_data.RelationalData(user_input=user_input_multi_table,is_multi_table=True)
        df_text=param_values['df_text_data']

        final_data = [i.lstrip(",").split(",") for i in list(filter(None, df_text.split("\n")))]
        combined_df = pd.DataFrame(columns=final_data[0], data=final_data[1:])

        cols = param_values['columns']
        cols_dict=json.loads(cols)
        final_df = []
        for key in cols_dict.keys():
            final_df.append(combined_df[[col for col in cols_dict[key]]].to_string(index=False))
        final_df = "::".join(final_df)
        return jsonify({"combined_file":final_df,"success":"True"})


        for i in cols:
            print(i)
            final_df.append(combined_df[[col for col in i]].to_string(index=False))

        # file=files_list[0]
        # print("file",file[0],file[1])
        # df=pd.DataFrame(file[1])
        # print(df.head())
        # df = b.split_dataframes(user_df)
        # # # replacing ',' by space
        # # # text = text.replace(",", " ")
        # #
        # final_df=b.split_dataframes(user_df)
        # final_df="::".join(final_df)
        # # return jsonify({"done":"do"})
        return jsonify({"combined_file":final_df,"success":"True"})

@app.route('/combine_csvs/', methods=['POST'])
def combine_csvs():
    if request.method == 'POST':
        # FileStorage object wrapper
        files_list=[]
        files = request.files

        for key in files.keys():

            files_list.append((key,files[key]))


        # a=request.args.get('relationships')
        print("files_list",files_list)
        relation_list=[]
        relations=request.args
        for key in relations.keys():
            if key.startswith("relation"):
                relation_list.append(relations[key])
        user_input_multi_table={'files':files_list,"relationships":relation_list}
        a=relational_data.RelationalData(user_input_multi_table,is_multi_table=True)

        df=a.handle_dataframes()
        # df.index.name="index"


        df.to_csv("combined_df.csv",index=False)
        text = open("combined_df.csv", "r")

        # joining with space content of text
        text = ','.join([i for i in text])

        # replacing ',' by space
        # text = text.replace(",", " ")
        final_data = [i.lstrip(",").split(",") for i in list(filter(None, text.split("\n")))]
        # displaying result


        return jsonify({"combined_file":final_data})


if __name__ == '__main__':
    app.run()