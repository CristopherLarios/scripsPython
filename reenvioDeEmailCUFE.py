import csv
import logging
import requests
import configparser
from db.psqlserver import SqlServerConnection
import json as payload

# logging setting
LOG_SCRIPT = "logScript"
LOG_NAME = "EnvioEmailCufe"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')


class sendEmailCufe:

    def __init__(self):
        super().__init__()
        self.enviroment = 'prod'
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = CONFIG[self.enviroment]['SERVER_SQL']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']
        self.csvName = 'D:\Escritorio\Libro3.csv'

        self.domain_api = CONFIG[self.enviroment]['URL_API_SendEmail']

        self.instance_sql.createPool()

    def run(self):
        database = self.instance_sql.database

        LOGGER.info('TENANT {0}'.format(database.replace('DB_', '')))
        headers = {
            'Content-Type': 'application/json',
            'api-key': CONFIG[self.enviroment]['API_KEY']
        }

        with open(self.csvName, 'r',encoding='utf-8-sig') as csvFile:
            reader = csv.DictReader(csvFile)
            for row in reader:
                print(str(row['CUFE']).replace('FE', ''))

                df_selectCufe = self.instance_sql.queryToPandas("""
                select cufe , payload, ElectronicInvoiceId from mdl11.tblElectronicInvoice
                where Cufe = '{0}' and Status = 1 and Response_Code = '0920'
                """.format(str(row['CUFE']).replace('FE', '')))

                LOGGER.info('Records founds {0}'.format(df_selectCufe.shape[0]))



                for index, cufe in df_selectCufe.iterrows():
                    print(cufe['cufe'])
                    payload1 = payload.loads(str(cufe['payload']))

                    chanel = payload1['gExtra']['gNotification']['dChannels']

                    for x in chanel:
                        if  x['dChannelName'] == 'Email':
                            chanel = [x]
                            break

                    print(chanel)

                    sendPayload = {
                        "data": {
                            "ElectronicInvoiceId":cufe['ElectronicInvoiceId'],
                            "gExtra": {
                                "gCompanyCode": payload1['gExtra']['gCompanyCode'],
                                "gNotification": {
                                    "dAttachments": {
                                        "dResponseAuthXML": True,
                                        "dResponseXML": True,
                                        "dCFe": True,
                                        "dCAFE": True
                                    },
                                    "dChannels": chanel
                                }
                            }
                        }
                    }

                    print(sendPayload)

                    resp = requests.post(
                        self.domain_api,
                        headers=headers, auth=None, verify=False, json=sendPayload)

                    if resp.status_code != 200:
                        LOGGER.error(
                            'Cufe:{0}, CODE: {1}, Error: {2}, content: {3}'.format(row['CUFE'], resp.status_code, resp.reason,resp.content))
                        print(resp.status_code )
                        print(resp.reason)
                    else:
                        LOGGER.info(
                            'Cufe:{0}, CODE: {1}, OK: {2}, Content: {3}'.format(row['CUFE'], resp.status_code, resp.reason, resp.content))
                        print(resp.status_code)
                        print(resp.reason)



if __name__ == "__main__":
    instance_update = sendEmailCufe()
    instance_update.run()
