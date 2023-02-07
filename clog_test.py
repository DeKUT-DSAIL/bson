import sys
import pandas as pd
import json
import os

import time
import dateutil.parser
import datetime
import gc
import requests
import numpy as np
import argparse

# Constants
API_BASE_URL = 'https://datahub.tahmo.org'
API_MAX_PERIOD = '365D'

# Load json cofig file
with open('config.json') as f:
    conf = json.load(f)

apiKey = conf['apiKey']
apiSecret = conf['apiSecret']

endpoints = {'VARIABLES': 'services/assets/v2/variables', # 28 different variables
             'STATION_INFO': 'services/assets/v2/stations',
             'WEATHER_DATA': 'services/measurements/v2/stations', # Configured before requesting
             'DATA_COMPLETE': 'custom/sensordx/latestmeasurements',
             'STATION_STATUS': 'custom/stations/status'}




def __handleApiError(apiRequest):
    json =None
    try:
        json = apiRequest.json()
    finally:
        if json and 'error' in json and 'message' in json['error']:
            print(json)
            raise Exception(json['error']['message'])
        else:
            raise Exception(f'API request failed with status code {apiRequest.status_code}')

def __request(endpoint, params):
    print(f'API request: {endpoint}')
    apiRequest = requests.get(f'{API_BASE_URL}/{endpoint}',
                                params=params,
                                auth=requests.auth.HTTPBasicAuth(
                                apiKey,
                                apiSecret
                            )
    )
    if apiRequest.status_code == 200:
        return apiRequest.json()
    else:
        return __handleApiError(apiRequest)

def getVariables():
    # endpoints['VARIABLES']
    response = __request(endpoints['VARIABLES'], {})
    variables = {}
    if 'data' in response and isinstance(response['data'], list):
        for element in response['data']:
            variables[element['variable']['shortcode']] = element['variable']
    return variables

def getStations(longitude=None, latitude=None, countrycode=None):

    response = __request(endpoints['STATION_INFO'], {'sort':'code'})

    stations = pd.json_normalize(response['data'])
    # print(stations['code'])
    if countrycode:
        return stations[stations['location.countrycode'] == f'{countrycode.upper()}']
    # Retrieving by latitude and longitude
    elif isinstance(latitude, list) and isinstance(longitude, list): 
      if len(latitude)==1 and len(longitude)==1:
        return stations[(stations['location.longitude'] == longitude[0]) & (stations['location.latitude'] == latitude[0])]
    # Given a range to look at
      elif len(latitude)==2 and len(longitude)==2:
        latitude = sorted(latitude)
        longitude = sorted(longitude)
        return stations[(stations['location.longitude'] >= longitude[0]) & (stations['location.longitude'] <= longitude[1]) & (stations['location.latitude'] >= latitude[0]) & (stations['location.latitude'] <= latitude[1])]
    else:
        # print('SDFGHJKDFGHJDFGHJ')
        return stations

def __splitDateRange(inputStartDate, inputEndDate):
    try:
        startDate = dateutil.parser.parse(inputStartDate)
        endDate = dateutil.parser.parse(inputEndDate)
        
    except ValueError:
        raise ValueError('Invalid data parameters')
    
    # Split into intervals of 365 days
    dates = pd.date_range(start=startDate.strftime('%Y%m%d'), end=endDate.strftime('%Y%m%d'), freq=API_MAX_PERIOD)
    df = pd.DataFrame([[i, x] for i, x in
                           zip(dates, dates.shift(1) - datetime.timedelta(seconds=1))],
                          columns=['start', 'end'])

    # Set start and end date to their provided values.
    df.loc[0, 'start'] = pd.Timestamp(startDate)
    df['end'].iloc[-1] = pd.Timestamp(endDate)
    return df

# Aggregate per day with flags
def aggregate_with_flags(csv_file):
    df = pd.read_csv(csv_file)
    df.rename(columns = {'Unnamed: 0':'Date'}, inplace = True)
    df.Date = df.Date.astype('datetime64')
    df2 = df.copy()
    # Adding the daily rainfall
    df = df.groupby(pd.Grouper(key='Date', axis=0, 
                      freq='1D')).sum()
    df2 = df2.groupby(pd.Grouper(key='Date', axis=0, 
                      freq='1D')).mean()

# Pure aggregates
def aggregate_variables(dataframe):
    dataframe = dataframe.reset_index()
    dataframe.rename(columns = {'index':'Date'}, inplace = True)
    return dataframe.groupby(pd.Grouper(key='Date', axis=0, 
                      freq='1D')).sum()




        
# Get both the rainfall and the precipitation
def getMeasurements_and_Flags(station, startDate=None, endDate=None, variables=None, dataset='controlled'):
    endpoints = f'services/measurements/v2/stations/{station}/measurements/{dataset}'
    datesplit = __splitDateRange(startDate, endDate)
    series = []
    seriesHolder = {}

    # retrieving the rows for the dates 
    for index, row in datesplit.iterrows():
        params = {
            'start': row['start'].strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end'  : row['end'].strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        if variables and isinstance(variables, list) and len(variables) == 1:
            params['variable'] = variables[0]
        response = __request(endpoints, params)

        # checking for values within the json
        if 'results' in response and len(response['results']) >= 1 and 'series' in response[
            'results'][0] and len(response['results'][0]['series']) >= 1 and 'values' in response['results'
            ][0]['series'][0]:
            # values stored in result key series, values

            for result in response['results']:
                if 'series' in result and len(result['series']) >= 1 and 'values' in result['series'][0]:
                    for serie in result['series']:
                        columns = serie['columns']
                        observations = serie['values']

                        time_index = columns.index('time')
                        quality_index = columns.index('quality')
                        variable_index = columns.index('variable')
                        sensor_index = columns.index('sensor')
                        value_index = columns.index('value')

                        # Create list of unique variables within the retrieved observations.
                        if not isinstance(variables, list) or len(variables) == 0:
                            shortcodes = list(set(list(map(lambda x: x[variable_index], observations))))
                        else:
                            shortcodes = variables
                        
                        for shortcode in shortcodes:
    
                            # Create list of timeserie elements for this variable with predefined format [time, value, sensor, quality].
                            timeserie = list(map(lambda x: [x[time_index], x[value_index] if x[quality_index] == 1 else np.nan, x[sensor_index], x[quality_index]],
                                                    list(filter(lambda x: x[variable_index] == shortcode, observations))))
                            
                            if shortcode in seriesHolder:
                                seriesHolder[shortcode] = seriesHolder[shortcode] + timeserie
                            else:
                                seriesHolder[shortcode] = timeserie
                                
                            # Clean up scope.
                            del timeserie

                        # Clean up scope.
                        del columns
                        del observations
                        del shortcodes

                # Clean up scope and free memory.
            del response
            gc.collect()
    for shortcode in seriesHolder:

        # Check if there are duplicate entries in this timeseries (multiple sensors for same variable).
        # [time, value, sensor, quality]
        timestamps = list(map(lambda x: x[0], seriesHolder[shortcode]))
        
        def element_1(x):
            return x[1],x[3]
        # if multiple sensors
        if len(timestamps) > len(set(timestamps)):
            # Split observation per sensor
            print('Split observations for %s per sensor' % shortcode)
            sensors = list(set(list(map(lambda x: x[2], seriesHolder[shortcode]))))

            for sensor in sensors:
                sensorSerie = list(filter(lambda x: x[2] == sensor, seriesHolder[shortcode]))
                timestamps = list(map(lambda x: pd.Timestamp(x[0]), sensorSerie))
                values = list(map(lambda x: x[1], sensorSerie))
                flags = list(map(lambda x: x[3], sensorSerie))
                # print(sensorSerie)
                flags_vals = list(map(element_1, sensorSerie))

                code = list(map(lambda x: x[0], flags_vals))
                quality = list(map(lambda x: x[1], flags_vals))

                code_series = pd.Series(code, index=pd.DatetimeIndex(timestamps))
                quality_series = pd.Series(quality, index=pd.DatetimeIndex(timestamps))

                serie = pd.Series(flags_vals, index=pd.DatetimeIndex(timestamps))

                both = pd.concat(objs=[code_series, quality_series], axis=1)
                both.columns = [f'{shortcode}_{station}_{sensor}', f'Qc_{station}']
                
                # series.append(serie.to_frame('%s_%s' % (shortcode, sensor)))
                series.append(both)

                # Clean up scope.
                del sensorSerie
                del timestamps
                del values
                del serie
        else:
            values = list(map(element_1, seriesHolder[shortcode]))
            serie = pd.Series(values, index=pd.DatetimeIndex(timestamps))

            code = list(map(lambda x: x[0], values))
            quality = list(map(lambda x: x[1], values))

            code_series = pd.Series(code, index=pd.DatetimeIndex(timestamps))
            quality_series = pd.Series(quality, index=pd.DatetimeIndex(timestamps))

            both = pd.concat(objs=[code_series, quality_series], axis=1)
            both.columns = [f'{shortcode}_{station}_{list(set(list(map(lambda x: x[2], seriesHolder[shortcode]))))[0]}', f'Qc_{station}']
            
            # series.append(serie.to_frame('%s_%s' % (shortcode, sensor)))
            series.append(both)

            # Clean up scope.
            del values
            del serie

        # Clean up memory.
        gc.collect()

    # # Clean up.
    del seriesHolder
    gc.collect()

    # Merge all series together.
    if len(series) > 0:
        df = pd.concat(series, axis=1, sort=True)
    else:
        df = pd.DataFrame()

    # Clean up memory.
    del series
    gc.collect()

    return df

# Get the variables only
def getMeasurements(station, startDate=None, endDate=None, variables=None, dataset='controlled'):
        #print('Get measurements', station, startDate, endDate, variables)
        endpoint = 'services/measurements/v2/stations/%s/measurements/%s' % (station, dataset)

        dateSplit = __splitDateRange(startDate, endDate)
        series = []
        seriesHolder = {}

        for index, row in dateSplit.iterrows():
            params = {'start': row['start'].strftime('%Y-%m-%dT%H:%M:%SZ'), 'end': row['end'].strftime('%Y-%m-%dT%H:%M:%SZ')}
            if variables and isinstance(variables, list) and len(variables) == 1:
                params['variable'] = variables[0]
            response = __request(endpoint, params)
            if 'results' in response and len(response['results']) >= 1 and 'series' in response['results'][0] and len(
                response['results'][0]['series']) >= 1 and 'values' in response['results'][0]['series'][0]:

                for result in response['results']:
                    if 'series' in result and len(result['series']) >= 1 and 'values' in result['series'][0]:
                        for serie in result['series']:

                            columns = serie['columns']
                            observations = serie['values']

                            time_index = columns.index('time')
                            quality_index = columns.index('quality')
                            variable_index = columns.index('variable')
                            sensor_index = columns.index('sensor')
                            value_index = columns.index('value')

                            # Create list of unique variables within the retrieved observations.
                            if not isinstance(variables, list) or len(variables) == 0:
                                shortcodes = list(set(list(map(lambda x: x[variable_index], observations))))
                            else:
                                shortcodes = variables

                            for shortcode in shortcodes:

                                # Create list of timeserie elements for this variable with predefined format [time, value, sensor, quality].
                                timeserie = list(map(lambda x: [x[time_index], x[value_index] if x[quality_index] == 1 else np.nan, x[sensor_index], x[quality_index]],
                                                     list(filter(lambda x: x[variable_index] == shortcode, observations))))

                                if shortcode in seriesHolder:
                                    seriesHolder[shortcode] = seriesHolder[shortcode] + timeserie
                                else:
                                    seriesHolder[shortcode] = timeserie

                                # Clean up scope.
                                del timeserie

                            # Clean up scope.
                            del columns
                            del observations
                            del shortcodes

                # Clean up scope and free memory.
                del response
                gc.collect()

        for shortcode in seriesHolder:
            # Check if there are duplicate entries in this timeseries (multiple sensors for same variable).
            timestamps = list(map(lambda x: x[0], seriesHolder[shortcode]))

            if len(timestamps) > len(set(timestamps)):
                # Split observations per sensor.
                print('Split observations for %s per sensor' % shortcode)
                sensors = list(set(list(map(lambda x: x[2], seriesHolder[shortcode]))))
                for sensor in sensors:
                    sensorSerie = list(filter(lambda x: x[2] == sensor, seriesHolder[shortcode]))
                    timestamps = list(map(lambda x: pd.Timestamp(x[0]), sensorSerie))
                    values = list(map(lambda x: x[1], sensorSerie))
                    serie = pd.Series(values, index=pd.DatetimeIndex(timestamps))
                    series.append(serie.to_frame(f'{station}_{sensor}'))

                    # Clean up scope.
                    del sensorSerie
                    del timestamps
                    del values
                    del serie
            else:
                values = list(map(lambda x: x[1], seriesHolder[shortcode]))
                serie = pd.Series(values, index=pd.DatetimeIndex(timestamps))

                if len(values) > 0:
                    sensors = list(set(list(map(lambda x: x[2], seriesHolder[shortcode]))))
                    serie = pd.Series(values, index=pd.DatetimeIndex(timestamps))
                    series.append(serie.to_frame(f'{station}_{sensors[0]}'))

                # Clean up scope.
                del values
                del serie

            # Clean up memory.
            gc.collect()

        # Clean up.
        del seriesHolder
        gc.collect()

        # Merge all series together.
        if len(series) > 0:
            df = pd.concat(series, axis=1, sort=True)
        else:
            df = pd.DataFrame()

        # Clean up memory.
        del series
        gc.collect()

        return df


def getMultiples(stations_list, csv_file, startDate, endDate, variables, dataset='controlled'):
    error_list = []
    if isinstance(stations_list, list):
        problems = []
        df_stats = []
        
        for station in stations_list:

            print(stations_list.index(station))
            # if station not in problem:
            try:
                data = getMeasurements(station, startDate, endDate, variables)
                agg_data = aggregate_variables(data)
                # df_stats.append(data)
                df_stats.append(agg_data)
                df = pd.concat(df_stats, axis=1)
                df.to_csv(f'{csv_file}.csv')
            except KeyboardInterrupt:
                break
                
                # print(df)
            # except UnboundLocalError:
            #     problems.append(station)
            #     print(problems)
            # except requests.exceptions.ConnectTimeout:
            #     error_list.append(station)
            except:
                error_list.append(station)
                
        
        if len(error_list) >= 1:
            with open('Station.txt', 'w') as dfb:
                  dfv = dfb.write(f'{error_list}')
        #     getMultiples(error_list, 'connectionLost')
        
    else:
        raise ValueError('Pass in a list')
    
    return df
        



# Measurements for multiple stations 
def getMultiples_plus_flags(stations_list, csv_file, startDate, endDate, variables, dataset='controlled'):
    error_list = []
    if isinstance(stations_list, list):
        problems = []
        df_stats = []
        
        for station in stations_list:

            print(station)
            # if station not in problem:
            try:
                data = getMeasurements_and_Flags(station, startDate, endDate, variables)
                # df_stats.append(data)
                df_stats.append(data)
                df = pd.concat(df_stats, axis=1)
                
                # print(df)
            except UnboundLocalError:
                problems.append(station)
                print(problems)
            except requests.exceptions.ConnectTimeout:
                error_list.append(station)
            df = pd.concat(df_stats, axis=1)
            df.to_csv(f'{csv_file}.csv')
        
        
        if len(error_list) > 1:
          with open('Station.txt', 'w') as dfb:
            dfv = dfb.write(f'{error_list}')
          getMultiples_plus_flags(error_list, 'connectionLost')
        return df

        
    else:
        raise ValueError('Pass in a list')

# Loading the json file
def load_json(json_file):
    json_data = pd.read_json(json_file)
    clog = json_data[~json_data['description'].str.contains('batter')][['endDate', 'startDate', 'sensorCode', 'stationCode']]
    other_failure = json_data[json_data['description'].str.contains('batter')][['endDate', 'startDate', 'sensorCode', 'stationCode']]
    return clog, other_failure



def getClogs(startdate, enddate, longitude=[], latitude=[], countrycode=None, station=None, multipleStations=None, json_file='qualityobjects.json', csv_file='ClogFlags2', variables=['pr']):
    
    json_data = pd.read_json(json_file)
    if multipleStations:
        stations_pr = getMultiples(multipleStations, csv_file, startdate, enddate, variables, dataset='controlled')
    elif station:
        
        stations_pr = getMultiples([station], csv_file, startdate, enddate, variables, dataset='controlled')
    else:
        stations_ = getStations(longitude, latitude, countrycode)
        stations = list(stations_['code'])
        # print(stations_)
        stations_pr = getMultiples(stations, csv_file, startdate, enddate, variables, dataset='controlled')

    df_oth= []
    df_cl = []

    # cols = station_sensorcode
    other_failure = list(json_data[json_data['description'].str.contains('batter')].index)
    clog = list(json_data[~json_data['description'].str.contains('batter')].index)

    for cols in stations_pr.columns:
        for ind, row in json_data.iterrows():
            station_sensor = f'{row["stationCode"]}_{row["sensorCode"]}'
            if station_sensor == cols:

                # print(row['startDate'])
                '''Add to chose the range to filter'''
                startDate = dateutil.parser.parse(startdate)
                endDate = dateutil.parser.parse(enddate)
                # startDate = dateutil.parser.parse('2017-01-01T00:00:00.000Z')
                # endDate = dateutil.parser.parse('2022-10-31T00:00:00.000Z')
                rowStartDate = dateutil.parser.parse(dateutil.parser.parse(row['startDate']).strftime('%Y-%m-%d'))
                rowEndDate = dateutil.parser.parse(dateutil.parser.parse(row['endDate']).strftime('%Y-%m-%d'))
                # rowEndDate = pd.to_datetime(row['endDate']).dt.tz_localize(None)
                # rowStartDate = pd.to_datetime(row['startDate']).dt.tz_localize(None)

                if startDate < rowStartDate and endDate > rowEndDate:                    
                    if ind in other_failure:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        others = [2 for i in range(len(dates))] # 2 for battery/other failure
                        others_df = pd.DataFrame(zip(dates, others), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        others_df.index = others_df.index.astype('datetime64[ns, UTC]')
                        df_oth.append(others_df)
                        df_other = pd.concat(df_oth, axis=1, sort=True)
                        
                    elif ind in clog:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        clogggs = [1 for i in range(len(dates))] # 1 for clogged station
                        clogggs_df = pd.DataFrame(zip(dates, clogggs), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        clogggs_df.index = clogggs_df.index.astype('datetime64[ns, UTC]')
                        df_cl.append(clogggs_df)
                        df_clog = pd.concat(df_cl, axis=1, sort=True)
                    
                elif startDate > rowStartDate and endDate > rowEndDate:
                    print(f'Clogging/Failure Began before {startdate}')
                    rowStartDate = startDate
                    if ind in other_failure:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        others = [2 for i in range(len(dates))] # 2 for battery/other failure
                        others_df = pd.DataFrame(zip(dates, others), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        others_df.index = others_df.index.astype('datetime64[ns, UTC]')
                        df_oth.append(others_df)
                        df_other = pd.concat(df_oth, axis=1, sort=True)
                        
                    elif ind in clog:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        clogggs = [1 for i in range(len(dates))] # 1 for clogged station
                        clogggs_df = pd.DataFrame(zip(dates, clogggs), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        clogggs_df.index = clogggs_df.index.astype('datetime64[ns, UTC]')
                        df_cl.append(clogggs_df)
                        df_clog = pd.concat(df_cl, axis=1, sort=True)
                elif startDate > rowStartDate and endDate < rowEndDate:
                  print('Clogging persisted for the duration')
                  rowEndDate = endDate
                  rowStartDate = startDate
                  if ind in other_failure:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        others = [2 for i in range(len(dates))] # 2 for battery/other failure
                        others_df = pd.DataFrame(zip(dates, others), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        others_df.index = others_df.index.astype('datetime64[ns, UTC]')
                        df_oth.append(others_df)
                        df_other = pd.concat(df_oth, axis=1, sort=True)
                        
                  elif ind in clog:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        clogggs = [1 for i in range(len(dates))] # 1 for clogged station
                        clogggs_df = pd.DataFrame(zip(dates, clogggs), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        clogggs_df.index = clogggs_df.index.astype('datetime64[ns, UTC]')
                        df_cl.append(clogggs_df)
                        df_clog = pd.concat(df_cl, axis=1, sort=True)

                else:
                    print(f'Clogging/Failure continued after {enddate}')
                    rowEndDate = endDate
                    
                    # print(rowEndDate)
                    if ind in other_failure:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        others = [2 for i in range(len(dates))] # 2 for battery/other failure
                        others_df = pd.DataFrame(zip(dates, others), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        others_df.index = others_df.index.astype('datetime64[ns, UTC]')
                        df_oth.append(others_df)
                        df_other = pd.concat(df_oth, axis=1, sort=True)
                        #
                    elif ind in clog:
                        dates = pd.date_range(start=rowStartDate.strftime('%Y%m%d'), end=rowEndDate.strftime('%Y%m%d'), freq='1D').strftime('%Y-%m-%dT%H:%M:%SZ')
                        clogggs = [1 for i in range(len(dates))] # 1 for clogged station
                        clogggs_df = pd.DataFrame(zip(dates, clogggs), columns=['Date', f'{station_sensor}_clogFlag']).set_index('Date')
                        clogggs_df.index = clogggs_df.index.astype('datetime64[ns, UTC]')
                        df_cl.append(clogggs_df)
                        df_clog = pd.concat(df_cl, axis=1, sort=True)
                try:
                    dfcv = pd.concat(objs=[stations_pr, df_other, df_clog], axis=1, sort=True)
                except NameError:
                    dfcv = pd.concat(objs=[stations_pr, df_clog], axis=1, sort=True)
                    # except UnboundLocalError as f:
            
    
    # #define function to merge columns with same names together
    def same_merge(x): return ';'.join(x[x.notnull()].astype(str))

    # #define new DataFrame that merges columns with same names together
    try:
        df_new = dfcv.groupby(level=0, axis=1).apply(lambda x: x.apply(same_merge, axis=1))
    except UnboundLocalError:
        df_new = stations_pr

    df_new = pd.DataFrame([pd.to_numeric(df_new[i]) for i in df_new.columns]).T    

    for cl in df_new.columns:
    
        if cl.split('_')[-1] != 'clogFlag':
            
            if f'{cl}_clogFlag' not in df_new.columns:
                df_new[f'{cl}_clogFlag'] = [0 for i in range(len(df_new))]
                
            else:
                df_new[f'{cl}_clogFlag'] = df_new[f'{cl}_clogFlag'].fillna(0, axis=0) 

    # Rearranging the columns and saving the file
    df_new = df_new.reindex(sorted(df_new.columns), axis=1)
   
    df_new = df_new.reset_index()
    df_new.to_csv(f'{csv_file}2.csv', index=False)             


# Adding arguments to getclogs via terminal
# Get stations 
# Get location given stations
def parse_args():
    parser = argparse.ArgumentParser(description='A dataset to build a clogging Model')

    # Start and end date
    parser.add_argument('--startDate', type=str, help='StartDate to retrieve the data')
    parser.add_argument('--endDate', type=str, help='EndDate to retrieve the data')

    # Latitude and longitude
    parser.add_argument('--latitude', type=float, nargs= '+', help='Pass one for a particular station and two for a range in the region')
    parser.add_argument('--longitude', type=float, nargs= '+', help='Pass one for a particular station and two for a range in the region')

    # Countrycode
    parser.add_argument('--countrycode', type=str, help='Retrieve stations by their country code')

    # Station
    parser.add_argument('--station', type=str, help='Retrieve a particular station')
    parser.add_argument('--MultipleStations', nargs= '+', help='Retrieve multiple stations')

    # CSV FILE TO Save 
    parser.add_argument('--csvfile', type=str, help='CSV file to store the information')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    # getClogs(startdate='2017-01-01', enddate='2022-10-31', countrycode='ke')
    # getClogs(startdate='2017-01-01', enddate='2021-10-31', latitude=[-6.848668], longitude=[39.082174])
    if args.startDate and args.endDate or args.latitude or args.longitude or args.countrycode or args.csvfile or args.station or args.MultipleStations:
        getClogs(startdate=args.startDate, enddate=args.endDate, 
                 latitude=args.latitude, longitude=args.longitude, countrycode=args.countrycode, 
                 csv_file=args.csvfile, station=args.station, multipleStations=args.MultipleStations)

