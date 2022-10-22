#!/usr/bin/env python
# coding: utf-8

# In[ ]:


############# ############# ############# ############# 
#############    Autor : Amin Beikzadeh   ############# 
#############    Date  : 16 Jan 2022      ############# 
############# ############# ############# ############# 
import os 
import re
import json
import tarfile
from tqdm import tqdm_notebook

import pickle
import lzma

import numpy as np
import pandas as pd
from flatten_dict import flatten

import matplotlib.pyplot as plt
import seaborn as sb
sb.set()

import rasterio





class ascii_data_reader():
    
    '''
    ascii_data_reader: This calass consist of three functions:
    
    1. data_reader: This function reads the ascii data due to the chosen time interval and 
    generate a nested dictionary and returns it. 
    The keys of the dictionary are timestamps y/m/d/h/m.
    Inputs and Labels are conconated with keys input and label in the last level of the nested dictionary
    
    2.date_saver: It is used to save the generated nested dictionary for future works as raw data.
    
    
    3.data_loader: It is used to load the raw data for future works.
    
    4. min_15_date_picker : It pickes date from data. If time is none it means we dont manipualate 
        the date and we get 5mins data. If time is sum it  means each 15min date that we get are  
        sum of the last 5mins ( 15min = 5min + 5min +5min)
        # If time is mean it  means each 15mins date that we get are average
        of last 5mins date( 15min =  1/3 * (5min + 5min +5min))
    
    '''
    
    def __init__(self):
        
        pass    

    def data_reader(self, config_file):

        readed_src_data={}
        years = []
        months = []
        days = []
        hours = []
        minutes = []

        with open(config_file) as config_file:

            config = json.load(config_file)
            directory = config['directory']
            start_time = config['start_time']
            end_time = config['end_time']    
            germany_map = config['germany_map']

            if not start_time == None:
                #assert   type(start_time) == str, 'start_time gets string'
                #assert   type(end_time) == str, 'end_time gets string'
                listdirs = os.listdir(directory)
                index_start_time = listdirs.index(re.findall(r'.W20.*', start_time)[0])
                index_end_time = listdirs.index(re.findall(r'.W20.*', end_time)[0])
                listdirs = listdirs[index_start_time:index_end_time + 1]            
            else:
                listdirs = os.listdir(directory)            

        for file in tqdm_notebook(listdirs):          
            # Change the current working Directory to file.
                # The aim of it: in the chosen directory we have subdirectories and we are going to use the tar files 
                # of this subdirectories
            #print(file)
            path_name = directory + '/' + str(file)
            # tarfile opening
            tar = tarfile.open(path_name)
            # Calling each member of tarfile           

            for member in tqdm_notebook(tar.getmembers()):
                #print(member)
                untar = tar.extractfile(member)                                             

                # Extraction of year, month, day, hour and minute to use them for labeling
                year = member.name[12:16]
                if year not in years:
                    years.append(year)
                month = member.name[16:18]
                if month not in months:
                    months.append(month)
                day = member.name[18:20]
                if day not in days:
                    days.append(day)
                hour = member.name[21:23]
                if hour not in hours:
                    hours.append(hour)
                minute = member.name[23:25]  
                if minute not in minutes:
                    minutes.append(minute)
                #print(data)
                label = year + '.' + month + '.' + day + '.' + hour + '.' + minute
                # Reading the ascii file using rasterio package
                with rasterio.open(untar) as src:
                    src_data = src.read()
                    # Reset the preconceived nodata_values in the ascii data with 0

                    if np.unique(src_data)[0] == -9:
                        src_data = np.where(src_data == -9, 0, src_data)

                    if not germany_map:
                        src_data = np.where(src_data == 0.000e+00, 0, src_data)


                    readed_src_data[label] = src_data 


        # Generating of a nested empty dictionary
        data ={}
        for year in  years:
            data[year]={}
            for month in  months:
                data[year][month]={}
                for day in days:
                    data[year][month][day] = {}
                    for hour in hours:
                        data[year][month][day][hour] = {}
                        for minute in minutes:
                            data[year][month][day][hour][minute] = {'input':0, 'label':0}

        # Insterting the values into the created nested dictionary
        key = list(readed_src_data.items())
        for i in range(len(readed_src_data)):
            key_i = key[i][0]       
            data[key_i[0:4]][key_i[5:7]][key_i[8:10]][key_i[11:13]][key_i[14:16]] = {'input':key[i][1], 'label':0}


        return  data
    
    def data_saver(self, data, file_name):
        
        # Saving the data
        with lzma.open(file_name, 'wb') as f:
            # Pickle the 'data' dictionary using the highest protocol available.
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL) 
            
    def data_loader(self, file_name):       
    
        # Loading the data
        with lzma.open(file_name, 'rb') as f:
            data = pickle.load(f)
    
        return data
    
    def min_15_date_picker(self, data, time=None):
        # The nested Dictionary is returned into a normal dictionary using flatten package 
        #  The out  is still a dictionary but out keys is now nested.
        out = flatten(data)
        dataframe = []
        # Searching in out1 keys to extract values.
        for i in range(len(list(out.keys()))):

            keys = list(out.keys())[i] 
            # Converting the values into dataframe with the new keys: year, month, day, hour, minute
            df = pd.DataFrame({"year": [int(keys[0])],
                               "month": [int(keys[1])],
                               "day": [int(keys[2])],
                               "hour": [int(keys[3])],
                               "minute": [int(keys[4])]})

            # Because of duplication of the date, the if condition has used.
            if (i) % 2 == 0:
                inputs = out[keys] 
                # Index creation
                index = pd.to_datetime(df, format="%Y-%m-%d %H:%M")[0]# 

                # Make a array in form of (index, input, lable)
                data = [index, inputs.reshape(-1), index]
                dataframe.append(data)

        df = pd.DataFrame(dataframe, columns=['date', 'input', 'lable'])
        df = df.sort_values(by=['date'], ignore_index=True)
        df = df.set_index(df['date'])
        df = df.drop('date', axis=1)
        
        # If time is none it means we dont manipualate the date and we get 5mins data
        if time == None:
            
            index_min_15 = pd.date_range(start=df.index[0], end=df.index[-1], freq='15min').strftime(('%Y-%m-%d %H:%M:%S'))
            df_15_min = df.loc[list(index_min_15)]
            
            return df_15_min
        
        # If time is sum it  means each 15mins date that we get are sum of last 5mins date( 15min =  5min + 5min +5min)
        
        if time=='sum':
            sum_of_5min = df.resample('15min').sum()
        
            return sum_of_5min   
        
        # If time is mean it  means each 15mins date that we get are average of last 5mins date( 15min =  1/3 * (5min + 5min +5min))
        if time=='mean':
            mean_of_5min = df.resample('15min').mean()
        
            return mean_of_5min 
        
         






