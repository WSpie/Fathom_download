import requests
import pandas as pd
import json
from shapely.geometry import LineString, Point, Polygon, box
import shapely
import geopandas as gp
import os, time, sys, random
from tqdm import tqdm, trange
from argparse import ArgumentParser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

def GetCookies(ck):       # This function provides the login token (cookie) required to access the download URL.
                        # The token (cookie) expires after a day or so.
                        # So, follow the steps in SOP to get the token.
    cookie =  {
        "Cookie": ck
    }
    return cookie

def log_error(savepath, message):
    log_file = os.path.join(savepath, "download_errors.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}\n"
    with open(log_file, 'w+') as log:
        log.write(full_message)

def DownloadTWDBTile(url, filename, savepath, ck):
    time.sleep(random.uniform(1, 3))  # Initial random delay

    max_attempts = 5
    attempt = 0
    newfile = os.path.join(savepath, filename)

    while attempt < max_attempts:
        try:
            response = requests.get(url, headers=GetCookies(ck))
        except Exception as e:
            msg = f"Exception while downloading {filename} (attempt {attempt+1}): {str(e)}"
            log_error(savepath, msg)
            print(msg)
            time.sleep(20)
            attempt += 1
            continue

        if response.status_code == 200 and len(response.content) > 411422:
            try:
                with open(newfile, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=512 * 1024):
                        if chunk:
                            f.write(chunk)
                return  # Success
            except Exception as e:
                msg = f"Error saving {filename}: {str(e)}"
                log_error(savepath, msg)
                print(msg)
                return

        elif response.status_code == 429:
            print(f"429 Too Many Requests for {filename} (attempt {attempt+1}), retrying...")
            time.sleep(random.uniform(2, 5))
            attempt += 1
            continue

        else:
            msg = f"Download failed or empty file: {filename} (Status {response.status_code})"
            log_error(savepath, msg)
            print(msg)
            return

    # Final failure (including repeated 429s)
    msg = f"Failed to download {filename} after {max_attempts} attempts."
    log_error(savepath, msg)
    print(msg)

def CreateBaseFolders(savepath, scenario, flood_type, frequency):
    foldername = os.path.join(savepath, f'Scenario {scenario}', flood_type, frequency)
    os.makedirs(foldername, exist_ok=True)
    return foldername

def build_url(SCEN, TYPE, FREQ, TILE, code):
    scen_str = f"{SCEN} - Existing Conditions" if SCEN == 5 else str(SCEN)
    peril_type = "Combined" if TYPE == "Combined" else "Individual"
    type_folder = f"{TYPE}%2F" if TYPE != 'Combined' else ""
    sub_url = (
        f"Scenario%20{scen_str.replace(' ', '%20')}%2F"
        f"Fathom%5F3m%5F{peril_type}%5FPeril%5FScenario%20{SCEN}%5FDepth%20Raster%20Tiles%2F"
        f"{type_folder}"
        f"{FREQ}%2F"
        f"{TILE}%5F{code.replace('_', '%5F')}%5F{FREQ}%2Etif"
    )
    base_url = (
        "https://twdb.sharepoint.com/teams/Flood_Planning_Submission_Collector/_layouts/15/download.aspx?"
        "SourceUrl=%2Fteams%2FFlood%5FPlanning%5FSubmission%5FCollector%2FShared%20Documents%2F"
        "Cursory%20Floodplain%20Dataset%20Phase%202%2FCursory%20Floodplain%20Phase%202%20%2D%20Downloads%2F"
    )
    url = base_url + sub_url
    return url


def DownloadAllTiles(savepath, scenario, flood_type, code, frequencies, ck, max_workers=8):
    if flood_type not in ["Pluvial", "Fluvial", "Combined"]:
        print("Please specify a valid flood type from either Pluvial or Fluvial or Combined")
        return
    
    #list of 1 degree lat,lon tilenames that cover Texas
    tiles = ['n25w98', 'n26w98', 'n26w99', 'n26w100', 'n27w97', 'n27w98', 'n27w99', 'n27w100', 'n28w96', 'n28w97', 'n28w98', 'n28w99', 'n28w100', 'n28w101', 'n28w104', 'n29w94', 'n29w95', 'n29w96', 'n29w97', 'n29w98', 'n29w99', 'n29w100', 'n29w101', 'n29w102', 'n29w103', 'n29w104', 'n29w105', 'n30w94', 'n30w95', 'n30w96', 'n30w97', 'n30w98', 'n30w99', 'n30w100', 'n30w101', 'n30w102', 'n30w103', 'n30w104', 'n30w105', 'n30w106', 'n31w94', 'n31w95', 'n31w96', 'n31w97', 'n31w98', 'n31w99', 'n31w100', 'n31w101', 'n31w102', 'n31w103', 'n31w104', 'n31w105', 'n31w106', 'n31w107', 'n32w95', 'n32w96', 'n32w97', 'n32w98', 'n32w99', 'n32w100', 'n32w101', 'n32w102', 'n32w103', 'n32w104', 'n32w105', 'n32w106', 'n32w107', 'n33w95', 'n33w96', 'n33w97', 'n33w98', 'n33w99', 'n33w100', 'n33w101', 'n33w102', 'n33w103', 'n33w104', 'n34w98', 'n34w99', 'n34w100', 'n34w101', 'n34w102', 'n34w103', 'n34w104', 'n35w100', 'n35w101', 'n35w102', 'n35w103', 'n35w104', 'n36w100', 'n36w101', 'n36w102', 'n36w103', 'n36w104']
    tiles = tiles[:]
    for frequency in frequencies:
        output_folder = CreateBaseFolders(savepath, scenario, flood_type=flood_type, frequency=frequency)
        futures = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for tile in tiles:
                url = build_url(scenario, flood_type, frequency, tile, code)
                output_file = f"{flood_type}_{tile}_{frequency}.tif"
                output_path = os.path.join(output_folder, output_file)
                if not os.path.exists(output_path):
                    futures.append(executor.submit(DownloadTWDBTile, url, output_file, output_folder, ck))

            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading {frequency}"):
                try:
                    future.result()
                except Exception as e:
                    print("Error in parallel download:", e)

if __name__ == "__main__":
    args = ArgumentParser()
    args.add_argument('--savepath', default='src')
    args.add_argument('--scenario', type=int, default=5)
    args.add_argument('--flood-type', default='Combined', help='Fluvial, Pluvial, Coastal, Combined')
    args.add_argument('--code', default='2020_0p50_combined')
    args.add_argument('--ck', default='MSFPC=GUID=c34dfa05ef8341bebecbbcf2b2aa0779&HASH=c34d&LV=202401&V=4&LU=1704552823465; rtFa=zHx1L3HAB988zuNV8OOqTw3GoOwPTP+rt9Xb3hQCkV4mNjhmMzgxZTMtNDZkYS00N2I5LWJhNTctNmYzMjJiOGYwZGExIzEzMzk0MTM1ODUxNjY4NzAyMyNjMTIyYTdhMS02MDI1LTgwMDAtZjg1Yy0xMmRiYzMwM2FmOGEjbGlwYWkuaHVhbmclNDB0YW11LmVkdSMxOTM3OTUjbFN0MnFyUjVWVUhBbTJyc09NdEVEc19MTl9BI7B9MU6BWunrXEqVkhvXG4gnLfA9GcqybfB8qdK5nXeLaMM3bqv3TezLPZRSnlDNRNixRGFSswo1K1waVc5IB12BQwAF0KP6zV0hezBVO7kljyFSq/1ooEnhNGJkTN5E+Fr3/SEtY2LPOCZYVvlOqum3zL+zHhgsrX/bFYgKY0JzMB79wTir4Ehv79LR99TqOFURHb7YL2t1XtuJTcbs58yB9k/cZik1KLb5waqHX3DxAlMWXrKYga01OcbR4yP0ZvsbeH8qqJ0CdzCTkpeeXgy/n7X3mGJksKZ991quoN5JvwXmuspFW5p62UEMpLzDrncVrAqrZUXKxUlTEyR5D+i4AAAA; MicrosoftApplicationsTelemetryDeviceId=213cd521-b6b6-4a8c-8c9a-318a58422e32; FedAuth=77u/PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48U1A+VjE0LDBoLmZ8bWVtYmVyc2hpcHx1cm4lM2FzcG8lM2Fhbm9uIzA4MmFhZDVhNTUwNzBjMzkzZThiNTViODdmNjg0NTYxNzU2MzMyNDg1NDdjOWUyMzY2M2FiYzMwYmJiNjE4MGMsMCMuZnxtZW1iZXJzaGlwfHVybiUzYXNwbyUzYWFub24jMDgyYWFkNWE1NTA3MGMzOTNlOGI1NWI4N2Y2ODQ1NjE3NTYzMzI0ODU0N2M5ZTIzNjYzYWJjMzBiYmI2MTgwYywxMzM5NDY2NDAxODAwMDAwMDAsMCwxMzM5NDc1MDExODQ3Mzk2OTMsMC4wLjAuMCwyNTgsMTJlNjZhMjQtMWRkOS00OTJlLThjYWUtMDFjNTFiODkxMzVmLCwsOTA5MmFjM2UtNWYxMC00MDY4LTg0OTMtNjI2ZTVkMDFlY2Q4LDkwOTJhYzNlLTVmMTAtNDA2OC04NDkzLTYyNmU1ZDAxZWNkOCxrTWtvYjR1VmMwNkkzNHptZ3k5MmV3LDAsMCwwLCwsLDI2NTA0Njc3NDM5OTk5OTk5OTksMCwsLCwsLCwwLCwxOTI1MTUsZk5BMmNFU0RqNWdQRll0LVlPU0xNelZuSEZRLCxHSlNxNmxFYmQzclRvUXNwMmxRWHVqOUtwaXY4KzNWblZHVnNQWEZhU0JvYmcrcGZab0VEc1Z5MlZCSjdkc0RmZ01HbmV3M0M1OE9CVmZ1a3MwVHczVi92bDlpWEVtbk1TVU54U0hMTmNhbDZXSlBaNjJZWG1rb2U0dmpVR3UxZWQvc3BGcjBtNUdWSmdZeDdGUXRwdkRYaFhuanVlWkZQRGpGaGN3Rm5BRHE2S0c0aG1xZkZrMHIxWWx1Y2tTU1dDYjVqOGxBNmlhNmxxZWhucVdjdWV2WTMrL3NEZEFLMFJhL0Z0c09pT29RQ0JUL3hxWkFtV29pZFhWYlJzL0RtVGxlVEVRYkc5U25xdFI5eWVGQmJLNDN4UUFlVTIvV1Rod1FXWkpmdzdMSmtWd2dBb00wSXdIbUVYaEF4NkZpKzYvT1F5SlEzY3RiUGkxQ05uYzQ0Rnc9PTwvU1A+; FeatureOverrides_experiments=[]; ai_session=c0/YqKckWKyXQVic+p/c0e|1750190118142|1750190894293')
    args.add_argument('--workers', type=int, default=8, help='Number of parallel downloads (default: 8)')
    opt = args.parse_args()
    frequencies = ["1in10", "1in100", "1in25", "1in5", "1in500"]
    #Modify the list above to select the flood frequencies
    DownloadAllTiles(opt.savepath, opt.scenario, opt.flood_type, opt.code, frequencies, opt.ck)
