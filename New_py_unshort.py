import requests
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing as mp
import os
from datetime import datetime
import numpy as np

# """____for input and output path____"""

global root_path
global input_path
global output_path
global output_file_name
global url_count
global url_processed
global url_failed
global f_data


root_path = os.path.dirname(os.path.abspath(__file__))
#root_path = os.getcwd()
input_path = os.path.join(root_path, "input")
output_path = os.path.join(root_path, "output")
input_file = 'urls.csv'
output_file_name = 'output_'

f_data = pd.DataFrame(columns=['URL','Status','Number of redirects','Redirect Chain','Final URL'])
try:
    list_urls = pd.read_csv(os.path.join(input_path, input_file),
                                        delimiter="\t")['url'].values
except:
    list_urls = pd.read_csv(os.path.join(input_path, output_file),
                                    delimiter="\t")['URL'].values
url_count = len(list_urls)
print(url_count)
url_processed = 0
url_failed = 0
#____initializing date & time for output file name
today = datetime.now().strftime('%m%d_%H%M')

def get_status_code(url):
    global root_path
    global input_path
    global output_path
    global output_file_name
    global url_count
    global url_processed
    global url_failed
    global f_data
    url_count-=1
    url_processed +=1
        
    try:
        r = requests.get(url)
        print("Processing " + str(url))

        if len(r.history) > 0:
            chain = ""
            code = r.history[0].status_code
            final_url = r.url
            for resp in r.history:
                chain += resp.url + " | "
            print(str(url_processed)+" URLs Processed | "+str(url_count)+" Remaining | "+str(url_failed)+" Failed")
            df = pd.DataFrame([[url,str(code),str(len(r.history)),chain,final_url]], columns=['URL','Status','Number of redirects','Redirect Chain','Final URL'])
            f_data = f_data.append(df)
            f_data.to_csv(os.path.join(output_path, output_file_name+today+".csv"), sep='\t', encoding='utf-8', index=False)
            return [url,str(code),str(len(r.history)),chain,final_url]
        
        else:
            df = pd.DataFrame([[url,str(r.status_code),'n/a','n/a','n/a']], columns=['URL','Status','Number of redirects','Redirect Chain','Final URL'])
            f_data = f_data.append(df)
            f_data.to_csv(os.path.join(output_path, output_file_name+today+".csv"), sep='\t', encoding='utf-8', index=False)
            return [url,str(r.status_code),'n/a','n/a','n/a']
        
    except requests.ConnectionError:
        print("Error: failed to connect.")
        url_failed +=1
        print(str(url_processed)+" URLs Processed | "+str(url_count)+" Remaining | "+str(url_failed)+" Failed")
        df = pd.DataFrame([[url,'0','n/a','n/a','n/a']], columns=['URL','Status','Number of redirects','Redirect Chain','Final URL'])
        f_data = f_data.append(df)
        f_data.to_csv(os.path.join(output_path, output_file_name+today+".csv"), sep='\t', encoding='utf-8', index=False)
        return [url,'0','n/a','n/a','n/a']


def final_redirect(urls):
    data = []
    for i in urls:
        efr = get_status_code(i)
        data.append(efr)
                
    return data
 
    
#____Preparing arguments for multiprocessing
cpu_count = 4
print(cpu_count)
pool = ThreadPool(cpu_count)
list_split = np.array_split(list_urls, cpu_count)
general_data = pool.map(final_redirect, list_split)


#____merging list 
data_combined = []
for i in general_data:
    for j in i:
        data_combined.append(j)
        
#____transforming list data to dataframe

dataf = pd.DataFrame(data_combined,columns=['URL','Status','Number of redirects','Redirect Chain','Final URL'])
                         
#___writing csv file "tab separated"
dataf.to_csv(os.path.join(output_path, "final_"+output_file_name+today+".csv"), sep='\t', encoding='utf-8', index=False)


