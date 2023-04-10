import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter("ignore", UserWarning)
from scipy.stats import chisquare
from scipy.stats import ks_2samp
from fpdf import FPDF
import os

def chisq(real_df,synthetic_df,col,level_of_significance):
    from scipy.stats import chisquare
    f=open('statistical_comparison.txt','a')
    textlist=[]
    textlist.append("Test for {}".format(col)+'\n')
    textlist.append("H0: The two distributions are same"+'\n')
    textlist.append("H1: The two distributions are different"+'\n')
    real_df[col]=real_df[col].astype('str')
    synthetic_df[col]=synthetic_df[col].astype('str')
    real_df_freq=real_df.groupby(col).size().reset_index(name='expected_frequency')
    real_df_freq['expected_freq_percentage']=real_df_freq.iloc[:,1]/real_df_freq.iloc[:,1].sum()
    synthetic_df_freq=synthetic_df.groupby(col).size().reset_index(name='observed_frequency')
    synthetic_df_freq['observed_freq_percentage']=synthetic_df_freq.iloc[:,1]/synthetic_df_freq.iloc[:,1].sum()
    final_df=real_df_freq.merge(synthetic_df_freq[[col,'observed_freq_percentage']],on=col,how='left')
    chisq,pvalue=chisquare(final_df['observed_freq_percentage'],final_df['expected_freq_percentage'])
    textlist.append("Chi Square statistic: {}".format(str(chisq))+'\n')
    textlist.append("Pvalue: {}".format(str(pvalue))+'\n')
    if pvalue<level_of_significance:
        textlist.append("Since, the p-value is less than the specified level of significance, we can reject the null hypothesis"+'\n')
    else:
        textlist.append("Since,the p-value is greater than the specified level of significance,we fail to reject the null hypothesis"+'\n')
        
    f.writelines(textlist)
    f.close()
    
def ks_test(real_df,synthetic_df,col,level_of_significance):
    from scipy.stats import ks_2samp
    f=open('statistical_comparison.txt','a')
    textlist=[]
    textlist.append("Test for {}".format(col)+'\n')
    textlist.append("H0: The two distributions are same"+'\n')
    textlist.append("H1: The two distributions are different"+'\n')
    real_df_norm=(real_df[col]-real_df[col].mean())/real_df[col].std()
    synthetic_df_norm=(synthetic_df[col]-synthetic_df[col].mean())/synthetic_df[col].std()
    ksstat,pvalue=ks_2samp(real_df_norm,synthetic_df_norm)
    textlist.append("KS Test Statistic : {}".format(str(ksstat))+'\n')
    textlist.append("Pvalue: {}".format(str(pvalue))+'\n')
    if pvalue<level_of_significance:
        textlist.append("Since, the p-value is less than the specified level of significance, we can reject the null hypothesis"+'\n')
    else:
        textlist.append("Since,the p-value is greater than the specified level of significance,we fail to reject the null hypothesis"+'\n')
    
    f.writelines(textlist)
    f.close()
    
def cat_cols_visual(real_df,synthetic_df,col):
    real_df_freq=real_df.groupby(col).size().reset_index(name='Real_data')
    real_df_freq['real_data_percentage']=real_df_freq.iloc[:,1]/real_df_freq.iloc[:,1].sum()
    real_df_freq.sort_values(by=['real_data_percentage'],inplace=True,ascending=False)
    synthetic_df_freq=synthetic_df.groupby(col).size().reset_index(name='Synthetic_data')
    synthetic_df_freq['synthetic_data_percentage']=synthetic_df_freq.iloc[:,1]/synthetic_df_freq.iloc[:,1].sum()
    real_df_freq[col]=real_df_freq[col].astype('str')
    synthetic_df_freq[col]=synthetic_df_freq[col].astype('str')
    max_cat_cnt=10
    cat_cnt=real_df_freq[col].nunique()
    if cat_cnt<=max_cat_cnt:
        real_df_freq1=real_df_freq
    else:
        categories1=real_df_freq[col].unique()
        categories2=synthetic_df_freq[col].unique()
        categories3=[value for value in categories1 if value in categories2]
        real_df_freq1=real_df_freq[real_df_freq[col].isin(categories3[0:9])]
    
    final_df=real_df_freq1[[col,'real_data_percentage']].merge(synthetic_df_freq[[col,'synthetic_data_percentage']],on=col,how='left')
    fig=plt.figure(figsize=(9,6))
    ax=fig.add_axes([1,1,1,1])
    span=np.arange(len(final_df))
    width=0.25
    bar_width = 0.4
    opacity = 1.0
    ylabel="The % distibution of data across "+col
    real_data_plot = plt.bar(span, final_df['real_data_percentage'], bar_width,alpha=opacity,color='blue',label='Real Data')
    synthetic_data_plot = plt.bar(span + bar_width, final_df['synthetic_data_percentage'] , bar_width,alpha=opacity,color='g',label='Synthetic Data')
    plt.xlabel(col,size=15)
    plt.ylabel(ylabel,size=15)
    plt.xticks(span + bar_width, final_df[col])
    plt.legend()
    vals=ax.get_yticks()
    ax.set_yticklabels(['{:,.2%}'.format(x) for x in vals])
    for fl in ax.get_yticklabels():
        fl.set_fontsize(13)
    for fl in ax.get_xticklabels():
        fl.set_fontsize(13)
    title="Comparing the distribution of {} in Real and Synthetic Data".format(col)
    plt.title(title,size=15)
    filename='cat_'+col+'.png'
    fig.savefig(filename)
    

def dist_plot_num1(real_df,synthetic_df,col):
    fig=plt.figure(figsize=(13,15))
    fig.add_subplot(221)
    #ax=fig.add_axes([1,1,1,1])
    sns.distplot(real_df[col],color='b',label='Real Data')
    ylabel="The distibution of data across "+col
    plt.xlabel(col,size=15)
    plt.ylabel(ylabel,size=15)
    plt.legend()
    #for fl in ax1.get_yticklabels():
     #   fl.set_fontsize(13)
    #for fl in ax1.get_xticklabels():
     #   fl.set_fontsize(13)

    fig.add_subplot(222)
    #ax=fig.add_axes([1,1,1,1])
    sns.distplot(synthetic_df[col],color='g',label='Synthetic Data')
    ylabel="The distibution of data across "+col
    plt.xlabel(col,size=15)
    plt.ylabel(ylabel,size=15)
    plt.legend()
    
    title="Comparing the distribution of {} in Real and Synthetic Data".format(col)
    plt.title(title,size=15)
    filename='num_'+col+'.png'
    fig.savefig(filename)
    #for fl in ax2.get_yticklabels():
     #   fl.set_fontsize(13)
    #for fl in ax2.get_xticklabels():
      #  fl.set_fontsize(13)

def within_cols(real_df,synthetic_df):
    fig=plt.figure(figsize=(12,12))
    fig.add_subplot(221)
    #ax=fig.add_axes([1,1,1,1])
    #sns.distplot(real_df[col],color='b',label='Real Data')
    sns.heatmap(real_df.corr(),annot=True)
    plt.xlabel('Real Data',size=15)
    fig.add_subplot(222)
    sns.heatmap(synthetic_df.corr(),annot=True)
    plt.xlabel('Synthetic Data',size=15)
    
    title="Comparing the correlation among numerical variables in Real and Synthetic Data"
    plt.title(title,size=15)
    filename='within.png'
    fig.savefig(filename)

from ast import literal_eval


def plots(real_df,synthetic_df,cat_cols,num_cols):
    
    for col in cat_cols:
         cat_cols_visual(real_df,synthetic_df,col)
         
    for col in num_cols:
         dist_plot_num1(real_df,synthetic_df,col)
         
    within_cols(real_df,synthetic_df)

    
def stat_test(real_df,synthetic_df,cat_cols,num_cols):
    
    for col in cat_cols:
        chisq(real_df,synthetic_df,col,0.05)
    
    for col in num_cols:
        ks_test(real_df,synthetic_df,col,0.1)
        


class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.WIDTH = 210
        self.HEIGHT = 297
    
    def header(self):
        # Custom logo and positioning
        
        self.set_font('Arial', 'B', 11)
        self.cell(self.WIDTH - 80)
        self.cell(60, 1, 'THE COMPARISON REPORT OF REAL vs SYNTHETIC DATA', 0, 0, 'R')
        self.ln(20)
        
    def footer(self):
        # Page numbers in the footer
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, 'Page ' + str(self.page_no()), 0, 0, 'C')
        
    def page_body(self, images):
        # Determine how many plots there are per page and set positions
        # and margins accordingly
        if len(images) == 3:
            self.image(images[0], 15, 25, self.WIDTH - 30)
            self.image(images[1], 15, self.WIDTH / 2 + 5, self.WIDTH - 30)
            self.image(images[2], 15, self.WIDTH / 2 + 90, self.WIDTH - 30)
        elif len(images) == 2:
            self.image(images[0], 15, 25, self.WIDTH - 30)
            self.image(images[1], 15, self.WIDTH / 2 + 5, self.WIDTH - 30)
        else:
            self.image(images[0], 15, 25,self.WIDTH - 30)
            
    def print_page(self, images):
        # Generates the report
        self.add_page()
        self.page_body(images)
        

def construct():
        
    # Construct data shown in document
    counter = 0
    pages_data = []
    temp = []
    # Get all plots
    files = os.listdir()
    files1=[x for x in files if x.split('.')[-1]=='png']
    #print(files1)
    
    # Iterate over all created visualization
    for fname in files1:
        # We want 3 per page
        if counter == 3:
            pages_data.append(temp)
            temp = []
            counter = 0

        temp.append(f'{fname}')
        counter += 1

    return [*pages_data, temp]
            
    
