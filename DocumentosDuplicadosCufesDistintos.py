import logging
import requests
import configparser
import time
from db.psqlserver import SqlServerConnection
from progress.bar import Bar
import csv

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


class cufesDiferentes:
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

        self.domain_api = CONFIG[self.enviroment]['URL_API']
        self.urlDGI = 'https://dgi-fep.mef.gob.pa/Consultas/FacturasPorCUFE?CUFE=FE'
        self.csvName = 'Duplicadas Hypernovals.csv'

        self.instance_sql.createPool()

    def run(self):

        database = self.instance_sql.database

        LOGGER.info('TENANT {0}'.format(database.replace('DB_', '')))

        # Abriendo CSV
        with open(self.csvName, newline='') as File:
            print(File)
            reader = csv.DictReader(File)


            for row in reader:

                cufe = str(row['\ufeffcufe']).replace('FE', '')
                print('Cufe a trabajar: {0}'.format( cufe))
                df_FindDocument = self.instance_sql.queryToPandas("""
                SELECT cufe, ElectronicInvoiceId
                from mdl11.tblElectronicInvoice
                where cufe = '{0}' and status = 1 """.format(cufe))





                print('Cufe encontrado: {0}'.format(df_FindDocument.shape[0]))
                for index, document in df_FindDocument.iterrows():
                    

                    dgi, qr = self.funcionValidarCufe(document['cufe'])
                    print(document['ElectronicInvoiceId'])
                    if not dgi:
                        updateStatus = self.instance_sql.executeCommand(query= """
                        UPDATE MDL11.tblElectronicInvoice 
                        SET Status = 0 
                        WHERE ElectronicInvoiceId = '{0}' """.format(document['ElectronicInvoiceId']))
                        print('desactivado')
                    else:
                        print('Cufe valido')



    def funcionValidarCufe(self, cufe):
        qr = self.urlDGI + cufe

        response = requests.get(qr)

        if 'FACTURA' in response.text or "NOTA" in response.text:
            return True, qr
        else:
            return False, 'No se encuentra en DGI'


if __name__ == "__main__":
    instance_update = cufesDiferentes()
    instance_update.run()
