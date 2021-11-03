import splunklib.client as client
import splunklib.results as results
import pandas as pd
import argparse
import netrc
import progressbar
import time



def read_data_from_splunk(handle, query, dataframe_column):

    query = results.ResultsReader(handle.jobs.export(query))

    df=pd.DataFrame(columns=dataframe_column)

    return query, df


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Talk To Splunk To Grab Data')
    parser.add_argument('-l', '--host', type=str, default='splunk.oss.rogers.com',
                        help='enter the host name of splunk cluster')
    parser.add_argument('-p', '--port', type=int, default='8089',
                        help='enter the exposed port for the splunk cluster')
    args = parser.parse_args()
    HOST = args.host # Host Name
    secret= netrc.netrc()
    u, a, p = secret.authenticators(HOST)
    PORT = args.port
    USERNAME = u # UserName
    PASSWORD = p # Password
    service = client.connect(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD)

    query = open("query.txt", "r").read()

    columns = ['GW_MAC_ADDRESS', 'DEVICE_MAC', 'DEVICE_VENDOR', 'DEVICE_NAME', 'LAST_24H_DEVICE_PACKETS', 'LAST_24H_DEVICE_RET',
               'LAST_24H_MAX_DEVICE_SNR','LAST_24H_MEDIAN_DEVICE_MAX_RX','LAST_24H_MEDIAN_DEVICE_MAX_TX',
               'LAST_24H_MEDIAN_DEVICE_RSSI', 'LAST_24H_MEDIAN_DEVICE_RX','LAST_24H_MEDIAN_DEVICE_SNR',
               'LAST_24H_MEDIAN_DEVICE_TX','LAST_24H_MIN_DEVICE_SNR','LAST_24H_WIFI_DISCONNECTS',
               'WIFI_HAPPINESS_INDEX']
    query, df = read_data_from_splunk(service, query, columns)
    bar = progressbar.ProgressBar(maxval=80000, widgets=[progressbar.Bar('=', 'Pulling Records From Splunk:[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    counter = 0
    for row in query:
        if isinstance(row, results.Message):
            print('%s: %s' % (row.type, row.message))
        elif isinstance(row, dict):
            df = df.append(pd.Series(
                [row['GW_MAC_ADDRESS'], row['DEVICE_MAC'], row['DEVICE_VENDOR'], row['DEVICE_NAME'], row['LAST_24H_DEVICE_PACKETS'], row['LAST_24H_DEVICE_RET'],
                 row['LAST_24H_MAX_DEVICE_SNR'], row['LAST_24H_MEDIAN_DEVICE_MAX_RX'],row['LAST_24H_MEDIAN_DEVICE_MAX_TX'],
                 row['LAST_24H_MEDIAN_DEVICE_RSSI'],row['LAST_24H_MEDIAN_DEVICE_RX'],row['LAST_24H_MEDIAN_DEVICE_SNR'],
                 row['LAST_24H_MEDIAN_DEVICE_TX'],row['LAST_24H_MIN_DEVICE_SNR'],row['LAST_24H_WIFI_DISCONNECTS'],
                 row['WIFI_HAPPINESS_INDEX']], index=df.columns), ignore_index=True)
            if counter < 80000:
                bar.update(counter)
            counter += 1
    bar.finish()
    print("Writing Output to CSV")
    time.sleep(0.2)
    df.to_csv("../output.csv", sep=',')
    time.sleep(0.2)
    print("Done")
    assert query.is_preview == False