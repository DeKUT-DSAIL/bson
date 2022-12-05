import pandas as pd
import numpy as np
import sys
import json
import requests
import calendar
sys.path.insert(0, r'C:\Users\Austin\Desktop\sensordx\cli_test\API-V2-Python-examples')
import TAHMO
import argparse
from calendar import monthrange, month_name
from collections import Counter

# Getting authorization
api = TAHMO.apiWrapper()
api.setCredentials('SensorDxKenya', '6GUXzKi#wvDvZ')

def evaluate_models(model_csv):
    total2 = pd.read_csv(model_csv)
    total2 = total2[[ 'station',
                'k_stations',
                'station_2021',
                'k_station_2021',
                'station_January_2021',
                'k_station_January_2021',
                'station_February_2021',
                'k_station_February_2021',
                'station_March_2021',
                'k_station_March_2021',
                'station_April_2021',
                'k_station_April_2021',
                'station_May_2021',
                'k_station_May_2021',
                'station_June_2021',
                'k_station_June_2021',
                'station_July_2021',
                'k_station_July_2021',
                'station_August_2021',
                'k_station_August_2021',
                'station_September_2021',
                'k_station_September_2021',
                'station_October_2021',
                'k_station_October_2021',
                'station_November_2021',
                'k_station_November_2021',
                'station_December_2021',
                'k_station_December_2021']]

    total2_cols = total2.columns.to_list()
    total2_ks = [total2_cols[ks] for ks in range(len(total2_cols)) if ks%2 != 0]
    total2_s = [total2_cols[s] for s in range(len(total2_cols)) if s%2 == 0]
    for i,j in enumerate(zip(total2_ks[1:], total2_s[1:])):
        k_stat = total2[total2_ks[0]]
        stat = total2[total2_s[0]]
        ks_col, ss_cols = total2[j[0]].to_list(), total2[j[1]].to_list()
        within_range2 = []
        outside_range2 = []
        for run in range(len(ks_col)):
            ks_col_clean = ks_col[run].split('[')[1].split(']')[0].split(',') # One entire column to one station
            k_stat_clean = [n for nh, n in enumerate(k_stat[run].split('[')[1].split(']')[0].split("'")) if nh%2 !=0]
   
        # Handling errors in stations input from API
            try:
                ss_cols[run] = float(ss_cols[run])

            except ValueError as e:
                try:
                    if i != 0:
                        startyear = int(j[1].split('_')[-1])
                        endday = monthrange(startyear, i)[1]
                    # print(j[1])
                        ss_cols[run] = float(api.getMeasurements(stat[run], f'{startyear}-{i}-01', f'{startyear}-{i}-{endday}', variables=['pr']).mean()[0])
                    else:
                        ss_cols[run] = float(api.getMeasurements(stat[run], f'{startyear}-01-01', f'{startyear}-12-31', variables=['pr']).mean()[0])
                except IndexError:
                    ss_cols[run] = None
        
        # Evaluating the threshold 
            thresh = ss_cols[run] * 0.4
        
            within_range = []
            outside_range = []
            for num, kss in enumerate(ks_col_clean): # In each column point to the list
                if kss == 'None':
                        kss = None
                try:
                    if abs(float(kss) - ss_cols[run]) <= thresh:
                        within_range.append(k_stat_clean[num])
                        outside_range.append(None)
                    else:
                        outside_range.append(k_stat_clean[num])
                        within_range.append(None)

                except ValueError as v: # Capturing the API RESPONSE ERROR
                    try:
                        if i != 0:
                            startyear = int(j[1].split('_')[-1])
                            endday = monthrange(startyear, i)[1]
                            kss = float(api.getMeasurements(stat[run], f'{startyear}-{i}-01', f'{startyear}-{i}-{endday}', variables=['pr']).mean()[0])
                            if abs(float(kss) - ss_cols[run]) <= thresh:
                                within_range.append(k_stat_clean[num])
                                outside_range.append(None)
                            else:
                                outside_range.append(k_stat_clean[num])
                                within_range.append(None)
                        else:
                            kss = float(api.getMeasurements(stat[run], f'{startyear}-01-01', f'{startyear}-12-31', variables=['pr']).mean()[0])
                            if abs(float(kss) - ss_cols[run]) <= thresh:
                                within_range.append(k_stat_clean[num])
                                outside_range.append(None)
                            else:
                                outside_range.append(k_stat_clean[num])
                                within_range.append(None)
                    except IndexError:
                        within_range.append(None)
                except TypeError as t: # Capturing NoneType Error
                    within_range.append(None)

        # Getting the entire row
            within_range2.append(within_range)
            outside_range2.append(outside_range)

    # Creating alternative columns with the stations data
        if i != 0:
            total2[f'{month_name[i]}_within_range'] = within_range2
            total2[f'{month_name[i]}_outside_range'] = outside_range2
        else:
            total2['annual_within_range'] = within_range2
            total2['annual_outside_range'] = outside_range2
    total2.to_csv('Evaluation.csv', index=False)

# Evaluating per month for the excerpt
'''Using the saved json file'''
# Evaluating for the entirety of the stations 
'''import get models from the bson.py file'''
def stations_evaluate(eval_csv, indexed=True):
    eval = pd.read_csv(eval_csv)
    if indexed: # Evaluating from the excerpt
        # Load the json file
        excerpts = []
        within_range_dict = dict()
        outside_range_dict = dict()
        with open('sample_models.json') as f:
            excerpt = list(json.load(f))
        # Stations without models
        excerpt2 = ['TA00094', 'TA00278', 'TA00091', 'TA00392', 'TA00389', 'TA00650', 
                    'TA00237', 'TA00180', 'TA00203', 'TA00393', 'TA00466', 'TA00109', 
                    'TA00128', 'TA00236', 'TA00279', 'TA00277', 'TA00467', 'TA00255', 
                    'TA00434', 'TA00664']
        for i in range(len(excerpt)):
            if excerpt[i] not in excerpt2:
                excerpts.append(excerpt[i])
        
    eval_within_annual = eval['annual_outside_range']
    eval_within = eval[['January_within_range', 'February_within_range', 'March_within_range',
                        'April_within_range', 'May_within_range', 'June_within_range', 
                        'July_within_range', 'August_within_range', 'September_within_range',
                        'October_within_range', 'November_within_range','December_within_range']]
    eval_outside = eval[['January_outside_range', 'February_outside_range', 'March_outside_range',
                        'April_outside_range', 'May_outside_range', 'June_outside_range', 
                        'July_outside_range', 'August_outside_range', 'September_outside_range',
                        'October_outside_range', 'November_outside_range','December_outside_range']]

    # Checking the occurrence of a station within
    print()
    # print(len())
    for cols in eval_within.columns:
        for run in range(len(eval_within)):
            try:
                rows = [eval_within[cols][run].split('[')[1].split(']')[0].split(",")[i].split("'")[1] 
                        for i in range(len(eval_within[cols][run].split('[')[1].split(']')[0].split(",")))]
                for row in rows:
                    try:
                        within_range_dict[row] += 1
                    except KeyError:
                        within_range_dict[row] = 0
                        within_range_dict[row] += 1
            except IndexError as ind:
                for ro in eval_within[cols][run].split('[')[1].split(']')[0].split(','):
                    if ro.strip() != "None":
                        ro = ro.split("'")[1]
                        try:
                            within_range_dict[ro] += 1
                        except KeyError:
                            within_range_dict[ro] = 0
                            within_range_dict[ro] += 1
    for cols in eval_outside.columns:
        for run in range(len(eval_outside)):
            try:
                rows = [eval_outside[cols][run].split('[')[1].split(']')[0].split(",")[i].split("'")[1] 
                        for i in range(len(eval_outside[cols][run].split('[')[1].split(']')[0].split(",")))]
                # break
                for row in rows:
                    try:
                        outside_range_dict[row] += 1
                    except KeyError:
                        outside_range_dict[row] = 0
                        outside_range_dict[row] += 1
                
                # print(rows)
            except IndexError as ind:
                # pass
                # print(eval_within[cols][run].split('[')[1].split(']')[0].split(','))
                for ro in eval_outside[cols][run].split('[')[1].split(']')[0].split(','):
                    if ro.strip() != "None":
                        if len(ro.split("'")) ==3:
                            ro = ro.split("'")[1]
                        try:
                            outside_range_dict[ro] += 1
                        except KeyError:
                            outside_range_dict[ro] = 0
                            outside_range_dict[ro] += 1
    
        
    print(outside_range_dict)
    print(within_range_dict)

    with open('outside_range.json', 'w') as out:
        json.dump(outside_range_dict, out, indent=4)    






if __name__== '__main__':
    evaluate_models('Combined_check.csv')
    stations_evaluate('Evaluation.csv')
