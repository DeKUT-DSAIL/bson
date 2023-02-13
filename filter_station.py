import requests
import urllib.parse
import pandas as pd
import argparse


def getLocation(address):
    url = 'https://nominatim.openstreetmap.org/search/' + urllib.parse.quote(address) +'?format=json'
    return requests.get(url).json()


def getStationsInfo():
    reqUrl = "https://datahub.tahmo.org/services/assets/v2/stations"
    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Authorization": "Basic U2Vuc29yRHhLZW55YTo2R1VYektpI3d2RHZa" 
    }

    payload = ""
    response = requests.request("GET", reqUrl, data=payload,  headers=headersList).json()
    return pd.json_normalize(response['data']).drop('id', axis=1)

def filterStations(address, csvfile='KEcheck3.csv'):
    location = getLocation(address)
    boundingbox = list(map(float, location[0]['boundingbox']))
    boundingbox_lat = sorted(boundingbox[0:2])
    boundingbox_lon = sorted(boundingbox[2:])
    stations = getStationsInfo()
    bounds = list(stations['code'][(stations['location.longitude'] >= boundingbox_lon[0])
                                    & (stations['location.longitude'] <= boundingbox_lon[1])
                                      & (stations['location.latitude'] >= boundingbox_lat[0])
                                        & (stations['location.latitude'] <= boundingbox_lat[1])])
    
    # read the csv file
    ke_chec = pd.read_csv(csvfile)
    ke_chec = ke_chec.set_index('Date')

    return ke_chec[[col for bbox in bounds for col in ke_chec if bbox in col]].reset_index().to_csv(f'{address}.csv', index=False)

def parse_args():
    parser = argparse.ArgumentParser(description='Locating the different stations')

    parser.add_argument('--address', type=str, required=True, help='Write the address to filter the stations')
    parser.add_argument('--csvfile', default='KEcheck3.csv', type=str, help='File to be filtered from default KEcheck3.csv')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    
    if args.address or args.csvfile:
        filterStations(address=args.address, csvfile=args.csvfile)