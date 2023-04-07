import os
import pandas as pd
import re
from io import StringIO
user_input_multi_table = {"root_path": "",
              "files": ['users.csv', 'sessions.csv', 'transactions.csv'],
              "relationships":
                  ["sessions.user_id -> users.user_id",
                   "transactions.session_id -> sessions.session_id"]}
user_input_multi_table = {"root_path": "",
              "files": [('users.csv', open('users.csv', 'rb')),
                        ('sessions.csv', open('sessions.csv', 'rb')),
                         ('transactions.csv', open('transactions.csv', 'rb'))],
              "relationships":
                  ["sessions.user_id -> users.user_id",
                   "transactions.session_id -> sessions.session_id"]}

class RelationalData:

    def __init__(self, user_input, is_multi_table=False):
        self.user_input = user_input
        self.is_multi_table = is_multi_table
        self.imp_keys_for_multi_table=['files','relationships']
        self.imp_keys_for_single_table=['root_path', 'file']
        self.relationship_expression="->"
        self.list_of_dfs=[]
        self.metadata_df=[]


    def validate_user_syntax_for_multi_table(self,user_input):
        if  isinstance(user_input,dict):
            if  set(self.imp_keys_for_multi_table).issubset(set(user_input.keys())):

                if isinstance(user_input[self.imp_keys_for_multi_table[0]],list):

                    if isinstance(user_input[self.imp_keys_for_multi_table[1]],list):
                        files_path=self.get_files_path_list_for_multi_table(user_input)
                        list_of_dfs,metadata_df=self.create_dataframe_and_metadata_list(files_path)
                        print(list_of_dfs,metadata_df)
                        df_names=list(metadata_df.keys())
                        table_relationships = user_input[self.imp_keys_for_multi_table[1]]
                        count=0
                        for entry in table_relationships:
                            if re.search(self.relationship_expression,entry):
                                regular_expression="\.|".join(df_names)+"\."+"\s*"+self.relationship_expression+"\s*"+"\.|".join(df_names)+"\."
                                table_names=re.findall(regular_expression,entry)
                                if len(table_names)==2:
                                    data=re.sub(regular_expression,"",entry)
                                    key_names=re.split(self.relationship_expression,data)
                                    if key_names[0].strip()==key_names[1].strip():
                                        cols1=list(list_of_dfs[metadata_df[table_names[0].replace(".","")]].columns)
                                        cols2 = list(list_of_dfs[metadata_df[table_names[1].replace(".", "")]].columns)
                                        if key_names[0].strip() in set(cols1).intersection(set(cols2)):
                                            count+=1



                                        else:
                                            print("No common key found ->"+key_names[0]+" in tables ->"+" and ".join(table_names))



                                    else:
                                        print("issue with foreign key in expression ->> "+entry)
                                    print(key_names)

                                    print(data)
                                else:
                                    print("issue with relationship expression")
                                    return 0
                            else:
                                print("No '->' found in relationship expression")
                                return 0
                        if count == len(table_relationships):
                            print("Multi table data is validated")
                            return 1
                        else:
                            print("primary key not same in tables")
                            return 0
                        # return 1
                    else:
                        print(self.imp_keys_for_multi_table[2]+ " are not of list datatype")


                else:
                    print(self.imp_keys_for_multi_table[0]+" is not a list datatype")
                    return 0
            else:
                print("|".join(self.imp_keys_for_multi_table)+" keys are not there in user input dictionary ")
                return 0
        else:
            print("user input is not of dict/json format")
            return 0

    def validate_user_syntax_for_single_table(self,user_input):
        if isinstance(user_input, dict):
            if set(self.imp_keys_for_single_table).issubset(set(user_input.keys())):
                if isinstance(user_input[self.imp_keys_for_single_table[0]], str):
                    if isinstance(user_input[self.imp_keys_for_single_table[1]], list):
                        return 1
                    else:
                        print(self.imp_keys_for_single_table[1] + " are not of list datatype")
                        return 0

                else:
                    print(self.imp_keys_for_single_table[0] + " is not a string datatype")
                    return 0
            else:
                print("|".join(self.imp_keys_for_single_table) + " keys are not there in user input dictionary ")
                return 0
        else:
            print("user input is not of dict/json format")
            return 0

    def get_file_path_for_single_table(self,user_input):
        try:
            data_from_user_input_file=user_input['file']
            if isinstance(data_from_user_input_file,list):
                file=data_from_user_input_file[-1]
            else:
                file=data_from_user_input_file
        except KeyError:
            print("key entered for file name is wrong ..right key is 'file'")
            return 0
        try:
            root_path=user_input['root_path']
        except KeyError:
            print("key entered for root path is wrong ..right key is 'root_path'")
        print(file,root_path)
        return os.path.join(root_path, file)

    def create_dataframe_for_single_table(self,file_path):
        df = pd.read_csv(file_path)

        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.name = file_path.split("\\")[-1].split(".")[0]
        return df

    def get_files_path_list_for_multi_table(self,user_input):
        try:
            files = user_input['files']
        except KeyError:
            print("Issue while fetching files in user input")
            return 0

        return files
        # return [os.path.join(root_path, file) for file in files]

    def get_relationships_between_files(self,user_input):
        try:
            relationships = user_input['relationships']
            print(relationships)
        except:
            print("Issue while fetching relationships between tables")
            return 0
        return relationships

    def create_dataframe_and_metadata_list(self,files_path_list):
        list_of_dfs = []
        metadata_df = {}
        i = 0
        print("files_path_list",files_path_list)

        for file in files_path_list:
            # data = file[1].read()
            #
            # s = str(data, 'utf-8')
            print("file 1",file[1])

            if file[1]!="":

                # df=pd.DataFrame(file[1])
                df=pd.read_csv(file[1])
                # print(df.head())

                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                df.name = file[0].split("\\")[-1].split(".")[0]

                list_of_dfs.append(df)
                metadata_df[df.name] = i
                i += 1
        self.list_of_dfs=list_of_dfs
        self.metadata_df=metadata_df
        return list_of_dfs, metadata_df

    def combine_dataframes(self,table_relationships,list_of_dfs,metadata_df):
        # current_df
        first_time = True

        for i in range(0, len(table_relationships)):
            if first_time:
                print("meta",metadata_df,table_relationships)
                df1_name, df2_name = re.findall("|".join(list(metadata_df.keys())), table_relationships[i])
                key = table_relationships[i].split("->")[0].split(".")[-1].strip()
                df1, df2 = list_of_dfs[metadata_df[df1_name]], list_of_dfs[metadata_df[df2_name]]
                current_df = pd.merge(df1, df2, on=key, how="left")
                current_df.name = df1.name
                first_time = False
            else:
                df1_name, df2_name = re.findall("|".join(list(metadata_df.keys())), table_relationships[i])
                key = table_relationships[i].split("->")[0].split(".")[-1].strip()

                if df1_name == current_df.name:
                    df1, df2 = current_df, list_of_dfs[metadata_df[df2_name]]

                elif df2_name == current_df.name:
                    df1, df2 = list_of_dfs[metadata_df[df1_name]], current_df
                else:
                    print("something unusual in relationships")
                    return -1
                current_df = pd.merge(df1, df2, on=key, how="left")
                current_df.name = df1.name
        return current_df

    def split_dataframes(self,combined_df):
        # if self.is_multi_table:
        #     if not self.validate_user_syntax_for_multi_table(self.user_input):
        #         print("User input is not in proper format for multi table")
        #         return 0

        table_relationships = self.get_relationships_between_files(self.user_input)
        file_paths = self.get_files_path_list_for_multi_table(self.user_input)
        list_of_dfs, metadata_df = self.create_dataframe_and_metadata_list(file_paths)

        print("list of dataframes",list_of_dfs)
        cols = [list(df.columns) for df in list_of_dfs]

        final_df = []
        for i in cols:
            final_df.append(combined_df[[col for col in i]].to_string(index=False))
        return final_df

    def handle_dataframes(self):
        if self.is_multi_table:
            # if not self.validate_user_syntax_for_multi_table(self.user_input):
            #     print("User input is not in proper format for multi table")
            #     return 0
            table_relationships = self.get_relationships_between_files(self.user_input)
            file_paths=self.get_files_path_list_for_multi_table(self.user_input)
            list_of_dfs, metadata_df = self.create_dataframe_and_metadata_list(file_paths)
            current_df=self.combine_dataframes(table_relationships,list_of_dfs,metadata_df)
            # string_df=current_df
            return current_df
        else:
            if not self.validate_user_syntax_for_single_table(self.user_input):
                print("User input is not in proper format for single table")
                return 0
            file_path=self.get_file_path_for_single_table(self.user_input)
            df=self.create_dataframe_for_single_table(file_path)
            return df

user_input_multi_table = {
              "files": [('users.csv', open('users.csv', 'rb')),('sessions.csv', open('sessions.csv', 'rb')),
                         ('transactions.csv', open('transactions.csv', 'rb'))],
              "relationships":
                  ["sessions.user_id -> users.user_id",
                   "transactions.session_id -> sessions.session_id"]}
# a = RelationalData(user_input_multi_table,is_multi_table=True)
#
# # a.validate_user_syntax_for_multi_table(user_input_multi_table)
# file_paths=a.get_files_path_list_for_multi_table(user_input_multi_table)
# print("file paths",file_paths)
# df_combined=a.handle_dataframes()
# print(type(df_combined))
# list_of_dfs, metadata_df = a.create_dataframe_and_metadata_list(file_paths)
# print(list_of_dfs,metadata_df)