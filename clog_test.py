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

# Constants
API_BASE_URL = 'https://datahub.tahmo.org'
API_MAX_PERIOD = '365D'
apiKey = 'SensorDxKenya'
apiSecret = '6GUXzKi#wvDvZ'

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

def getStations(longitude=[], latitude=[], countrycode=None, stat=1):
    reqUrl = "https://datahub.tahmo.org/services/assets/v2/stations"

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Authorization": "Basic U2Vuc29yRHhLZW55YTo2R1VYektpI3d2RHZa" 
    }

    payload = ""

    response = requests.request("GET", reqUrl, data=payload, headers=headersList).json()

    stations = pd.json_normalize(response['data'])
    if countrycode:
        return stations[stations['location.countrycode'] == f'{countrycode.upper()}']
    elif latitude and longitude and stat ==1:
        return stations[(stations['location.longitude'] == longitude[0]) & (stations['location.latitude'] == latitude[0])]
    elif latitude and longitude and stat != 1:
        latitude = sorted(latitude)
        longitude = sorted(longitude)
        return stations[(stations['location.longitude'] >= longitude[0]) & (stations['location.longitude'] <= longitude[1]) & (stations['location.latitude'] >= latitude[0]) & (stations['location.latitude'] <= latitude[1])]
    else:
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

# Aggregate per day
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
                    series.append(serie.to_frame('%s_%s_%s' % (shortcode, sensor, station)))

                    # Clean up scope.
                    del sensorSerie
                    del timestamps
                    del values
                    del serie
            else:
                values = list(map(lambda x: x[1], seriesHolder[shortcode]))
                serie = pd.Series(values, index=pd.DatetimeIndex(timestamps))

                if len(values) > 0:
                    serie = pd.Series(values, index=pd.DatetimeIndex(timestamps))
                    series.append(serie.to_frame('%s_%s' % (shortcode, station)))

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


def getMultiples(stations_list, csv_file):
    error_list = []
    if isinstance(stations_list, list):
        problems = []
        df_stats = []
        
        for station in stations_list:

            print(station)
            # if station not in problem:
            try:
                data = getMeasurements(station, '2017-01-01', '2022-10-31', variables=['pr'], dataset='controlled')
                agg_data = aggregate_variables(data)
                # df_stats.append(data)
                df_stats.append(agg_data)
                df = pd.concat(df_stats, axis=1)
                df.to_csv(f'{csv_file}.csv')
                
                # print(df)
            except UnboundLocalError:
                problems.append(station)
                print(problems)
            except requests.exceptions.ConnectTimeout:
                error_list.append(station)
        
        
        if len(error_list) > 1:
            getMultiples_plus_flags(error_list, 'connectionLost')
        return df

        
    else:
        raise ValueError('Pass in a list')
        



# Measurements for multiple stations 
def getMultiples_plus_flags(stations_list, csv_file):
    error_list = []
    if isinstance(stations_list, list):
        problems = []
        df_stats = []
        
        for station in stations_list:

            print(station)
            # if station not in problem:
            try:
                data = getMeasurements_and_Flags(station, '2017-01-01', '2022-10-31', variables=['pr'], dataset='controlled')
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
            getMultiples_plus_flags(error_list, 'connectionLost')
        return df

        
    else:
        raise ValueError('Pass in a list')

# Loading the json file
def load_json(json_file):
    json_data = pd.read_json(json_file)
    clog = json_data[(json_data['description'].str.contains('clog')) | (json_data['description'].str.contains('block'))][['endDate', 'startDate', 'sensorCode', 'stationCode']]
    other_failure = json_data[~((json_data['description'].str.contains('clog')) | (json_data['description'].str.contains('block')))]
    return clog, other_failure



clog, other_failure = load_json('qualityobjects.json')
clog_list = list(clog.stationCode.unique())
other_failure = list(clog.stationCode.unique())

if __name__ == '__main__':
    getMultiples(clog_list, 'clogged_stations')
