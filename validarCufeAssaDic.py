import logging
import configparser
from db.psqlserver import SqlServerConnection
from progress.bar import Bar
import xmltodict
import json as payload
import requests


# logging setting
LOG_SCRIPT = "validarCufeAssanetDicLOG"
LOG_NAME = "validarCufeAssanet"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')


class validarcufes():

    def __init__(self):
        super().__init__()
        self.enviroment = 'prod'
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = 'sql-aludra-prod-01.database.windows.net'
        self.instance_sql.driver = '{ODBC Driver 17 for SQL Server}'
        self.instance_sql.password = 'Linaka10'
        self.instance_sql.username = 'hnladmin'
        self.instance_sql.port = '1433'
        self.instance_sql.database = "DB_assanet"

        self.instance_sql.createPool()


    def validarCufesDGI(self):

        global doc
        try:
            database = self.instance_sql.database
            LOGGER.info('TENANT {0} - Proccess {1}'.format(database.replace('DB_',''),"Validar CUFE"))

            commadSelectCufes = """
                 SELECT  t.ElectronicInvoiceId,  t.Cufe as Cufe, isnull(t.QRcode,'') as QRcode, isnull(t.Response_Code,'') as Response_Code ,
                        isnull(t.Resolution,'') as Resolution
                FROM dbo.FacturasDiciembre t
                WHERE t.Status = 1 and isnull(validate, 0) = 0
            """

            dataCufes = self.instance_sql.queryToPandas(commadSelectCufes)

            LOGGER.info('Registros a validar {0}'.format(len(dataCufes)))

            bar = Bar('Processing', max=dataCufes.shape[0])

            for index, row in dataCufes.iterrows():

                LOGGER.info('------------------------------')
                LOGGER.info('Cufe: {0}'.format(row['Cufe']))
                
                bar.next()
                dgi,qr,aludra = self.funtionValidateCufeDGI(row['Cufe'], row['QRcode'])

                LOGGER.info('Cufe: {0} - DGI {1} - Aludra {2} - Resolution {3} - Response_Code {4}'.format(row['Cufe'],dgi,aludra, row['Resolution'], row['Response_Code']))

                commandUpdate =  """
                    UPDATE dbo.FacturasDiciembre
                    set validate = 1, DGI = {1}, QRAludra = {2}
                    WHERE ElectronicInvoiceId= '{0}'  
                """.format(row['ElectronicInvoiceId'],1 if dgi else 0, 1 if aludra else 0)
                self.instance_sql.executeCommand(commandUpdate)    

        except Exception as e:
            print(e)


    def funtionValidateCufeDGI(self,cufe,qr):

        urlDGI = 'https://dgi-fep.mef.gob.pa/Consultas/FacturasPorCUFE?CUFE=FE'

        if qr is not None and qr != '':
            response = requests.get(qr)
            qrAludra = True
        else:
            urlDGI += cufe
            response = requests.get(urlDGI)
            qrAludra = False

        if 'CUFE, digestValue o Ambiente inexistente.' in response.text and qr is not None and qr != '' :

            urlDGI += cufe

            response = requests.get(urlDGI)

            qrAludra = False

            if 'FACTURA' in response.text or "NOTA" in response.text:
                return True,urlDGI,qrAludra
            else:
                return False,'',qrAludra
        elif ('FACTURA' in response.text or "NOTA" in response.text) and qr is not None and qr != '':
            return True,qr,qrAludra
        elif 'FACTURA' in response.text or "NOTA" in response.text:
                return True,urlDGI,qrAludra
        else:
                return False,'',qrAludra





if __name__ == "__main__":
    instance_update = validarcufes()
    instance_update.validarCufesDGI()