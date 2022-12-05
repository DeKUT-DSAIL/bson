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

# Getting authorization
api = TAHMO.apiWrapper()
api.setCredentials('SensorDxKenya', '6GUXzKi#wvDvZ')

# Evaluating 40% threshold after running get_low_models_data
from evaluate_threshold import evaluate_models


# Requesting the models returning a dataframe of preferred columns
def get_models(columns_to_retrieve):
    reqUrl = "https://tahmorqctest.eu-de.mybluemix.net/api/models"
    headersList = {
                    "Accept": "*/*",
                    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
                    "Authorization": "Basic U2Vuc29yRHhLZW55YTo2R1VYektpI3d2RHZa" }
    payload = ""
    response = requests.request("GET", reqUrl, data=payload,  headers=headersList)
    return pd.DataFrame(response.json())[columns_to_retrieve]

# Get low data 
def low_data_models():
    '''Look into IMAP Module/GMAIL API to search and extract email from daily status messages'''
    # Text file based on emails low data models
    with open('model_test.txt', 'r') as f:
        model = f.read().split('\n')
    mods = dict()
    for i in model:
        i = list(i.split('LOW/NO data station impact on models: '))
        l = [k.split('(')[1].split("'")[1] for j,k in enumerate(i[1].split(',')) if j%2 == 0]
        for mod in l:
            if mod not in list(mods.keys()):
                mods[mod] = 1
            else:
                mods[mod] += 1
    return list(mods)


# Getting specific stations data and linking with their neighbouring stations
# Acquire both the stations and the neighbouring stations monthly data and finally the annual rainfall
'''2021 and 2022'''
def get_low_models_data(columns_to_retrieve, startDate, endDate, indexed_stations=True, data_name='email_models', annual=True):
    df = get_models(columns_to_retrieve)
    # If a sample list is retrieved
    if indexed_stations:
        count = 0
        low_models_data = []
        low_models_no_data = []
        low_models = list(low_data_models())
        # Getting low neighbours stations
        
        for i in low_models:
            if i in list(df['station']):
                low_models_data.append(i)
            else:
                low_models_no_data.append(i)
                count += 1
        df = df.set_index('station')
        df = (df.loc[low_models_data]).reset_index()

    # Getting stations monthly rainfall
    print(startDate.split('-'))
    start_year = startDate.split('-')[0]
    start_day = startDate.split('-')[-1]
    end_year = endDate.split('-')[0]
    end_day = endDate.split('-')[-1]
    stations_list = df.station.to_list()
    neighbours_list = df.k_stations.to_list()
    
    if annual:
        if start_year == end_year:
            for month in range(int(startDate.split('-')[1]), int(endDate.split('-')[1])+1):
                print(calendar.month_name[month])
                # Per station
                stations = []
                for station in stations_list:
                    try:
                        
                        stations.append(api.getMeasurements(station, f'{start_year}-{month}-{start_day}', f'{end_year}-{month}-{end_day}', variables=['pr']).mean()[0])
                    except IndexError:
                        print(station)
                        stations.append(None)
                    except:
                        stations.append('API RESPONSE ERROR')
                        print('ERROR!!')
                df[f'station_{calendar.month_name[month]}_{start_year}'] = stations
                print(df)
                df.to_csv(f'{data_name}.csv')
                print('Neighbours: ')
                # For the neighbours
                df_neighbours = []
                for k_station in neighbours_list:
                    per_neighbour = []
                    
                    for k in k_station:
                        try:
                            per_neighbour.append(api.getMeasurements(k, f'{start_year}-{month}-{start_day}', f'{end_year}-{month}-{end_day}', variables=['pr']).mean()[0])
                        except IndexError:
                            print(k)
                            per_neighbour.append(None)
                        except:
                            per_neighbour.append('API RESPONSE ERROR')
                            print('ERROR!!')
                    df_neighbours.append(per_neighbour)
                df[f'k_station_{calendar.month_name[month]}_{start_year}'] = df_neighbours
                print(df)
                df.to_csv(f'{data_name}.csv')
        
        else:
            print('To get Monthly Data set the dates for the same year')
            mon = sorted([int(startDate.split('-')[1]), int(endDate.split('-')[1])])
            for mont in range(mon[0], mon[1]+1):
                print(calendar.month_name[mont])

    # Getting annual data
    
    stations = []
    for station in stations_list:
        try:
            
            stations.append(api.getMeasurements(station, startDate, endDate, variables=['pr']).mean()[0])
        except IndexError:
            print(station)
            stations.append(None)
        except:
            stations.append('API RESPONSE ERROR')
            print('ERROR!!')
    df[f'station_{start_year}'] = stations
    df.to_csv(f'{data_name}.csv')

    
    df_neighbours = []
    for k_station in neighbours_list:
        per_neighbour = []
        for k in k_station:
            try:
                per_neighbour.append(api.getMeasurements(k, startDate, endDate, variables=['pr']).mean()[0])
            except IndexError:
                print(k)
                per_neighbour.append(None)
            except:
                per_neighbour.append('API RESPONSE ERROR')
                print('ERROR!!')
        df_neighbours.append(per_neighbour)
    df[f'k_station_{start_year}'] = df_neighbours

    
    df.to_csv(f'{data_name}.csv')
    

# Ability to use the command line
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--columns', default=['station', 'k_stations'], type=list, help='Columns to add to final CSV File')
    parser.add_argument('--startDate', default='2021-01-01', type=str, help='StartDate to begin retrieving data')
    parser.add_argument('--endDate', default='2021-12-28', type=str, help='End date currently not more than 28 days')
    parser.add_argument('--data_name', default='email_models', type=str, help='Name to save the extracted CSV File')
    parser.add_argument('--indexed_stations', default=True, type=bool, help='Set to False if all models')
    parser.add_argument('--annual', default=True, type=bool, help='Set to False to obtain only annual data')

    return parser.parse_args()


def main():
    args = parse_args()
    # if args.columns and args.startDate and args.endDate and args.data_name and args.indexed_stations and args.annual:
    #     print(args.columns and args.startDate and args.endDate and args.data_name and args.indexed_stations and args.annual)
    #     get_low_models_data(args.columns, args.startDate, args.endDate, args.indexed_stations, args.data_name, args.annual)

if __name__ == '__main__':
    main()
    get_low_models_data(['station', 'k_stations'], '2021-01-01', '2021-12-28', data_name='total_csv', indexed_stations=False)
    evaluate_models('total_csv.csv')