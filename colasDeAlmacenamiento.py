
import logging
import requests
import configparser
import time
from db.psqlserver import SqlServerConnection
from progress.bar import Bar

# logging setting
LOG_SCRIPT = "logScript"
LOG_NAME = "ColasAlmacenamiento"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')

class colasAlmacenamiento:

    def __init__(self):
        super().__init__()
        self.tenantId = 86
        self.enviroment = 'prod'
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = CONFIG[self.enviroment]['SERVER_SQL']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']

        self.domain_api = CONFIG[self.enviroment]['URL_API']

        self.instance_sql.createPool()
    def run(self):

        database = self.instance_sql.database

        LOGGER.info('TENANT {0}'.format(database.replace('DB_','')))

        df_almacenamiento = self.instance_sql.queryToPandas("""
        select cufe, Payload, ElectronicInvoiceId, Resolution from MDL11.tblElectronicInvoice
        where EInvoiceStatus = 3 
        and Status = 1 
        """)

        LOGGER.info('Records founds {0}'.format(df_almacenamiento.shape[0]))

        bar = Bar('Processing', max=df_almacenamiento.shape[0])

        for index, row in df_almacenamiento.iterrows():
            #print(row)


            LOGGER.info('------------------------------')
            LOGGER.info('Cufe: {0}'.format(row['cufe']))

            payload = {
                'Cufe':row['cufe'],
                'TenatId': self.tenantId
            }

            #print(f'payload: {payload}')

            headers = {
                'Content-Type': 'application/json'
            }

            resp = requests.post(
                self.domain_api,
                headers=headers, auth=None, verify=False,json=payload)
            if resp.status_code != 200:
                LOGGER.error('Cufe:{0}, CODE: {1}, Error: {2}'.format(row['cufe'],resp.status_code, resp.reason))
            else:
                if 'proccesResul' in resp.json():
                    #print(resp.json())
                    proccesResul = resp.json()['proccesResul']

                    if proccesResul == True:
                        LOGGER.info('Cufe: {0} SALIO DE LA COLA'.format(row['cufe']))
                    # else:
                    #     LOGGER.info('Cufe: {0} NO SE ENCUENTRA EN LA COLA'.format(row['cufe']))

                    #     if row['Resolution'] == 'Factura recibida con éxito':
                    #         command = """
                    #         UPDATE MDL11.tblElectronicInvoice
                    #         SET Response_Code = '0920',
                    #          InvoiceStatus ='Procesado',
                    #          EInvoiceStatus = '0', 
                    #          Status = 1
                    #         WHERE ElectronicInvoiceId= '{0}'
                    #         """.format(row['ElectronicInvoiceId'])

                    #         #print(command)

                    #         df_updateFE = self.instance_sql.executeCommand(query=command)

                    #         LOGGER.info('{1}- Actualizado {0}, Factura recibida con éxito'.format(row['cufe'], index))
            bar.next()
        bar.finish()




# Press the green button in the gutter to run the script.

if __name__ == "__main__":
    instance_update = colasAlmacenamiento()
    instance_update.run()
