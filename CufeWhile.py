import logging
import configparser
from db.psqlserver import SqlServerConnection
from progress.bar import Bar
import xmltodict
import json as payload
import requests
import time


# logging setting
LOG_SCRIPT = "logScript"
LOG_NAME = "validarCufe"
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
        self.instance_sql.server = CONFIG[self.enviroment]['SERVER_SQL']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']

        self.instance_sql.createPool()

    def validarCufesDGI(self):

        global doc
        try:
            database = self.instance_sql.database

            commadSelectCufes = """
                select  count(*) as conteo from MDL11.conteoFmottaJunio where validate = 0 AND cufe IS NOT  NULL
            """

            dataCufes = self.instance_sql.queryToPandas(commadSelectCufes)

            bar = Bar('Processing', max=dataCufes.shape[0])

            print(dataCufes['conteo'].loc[0])
            #for index, row in dataCufes.iterrows():
            while dataCufes['conteo'].loc[0] > 0:
                print('Conteo: {0}'.format(dataCufes['conteo'].loc[0]))
                commandTop1Cufe = """
                
                    select  top 1 cantidad, cufe, validate,urlQr,inDGI,qrAludra,Auth_Protocol from MDL11.conteoFmottaJunio where validate = 0 AND cufe IS NOT  NULL
                
                """

                FeActive = self.instance_sql.queryToPandas(commandTop1Cufe)

                for index, row in FeActive.iterrows():

                    cufe = str(row['cufe']).replace('FE', '')
                    commadfindCufeALudra = """
                                            select   t.Cufe as Cufe, isnull(t.QRcode,'') as QRcode, isnull(t.Response_Code,'') as Response_Code ,
                                            isnull(t.Resolution,'') as Resolution, isnull(t.PacResponse,'') as PacResponse, isnull(t.Auth_Protocol,'') as Auth_Protocol, t.AmountWithTax
                                             from MDL11.tblElectronicInvoice t where t.Cufe='{0}'
                                    """.format(cufe)

                    findCufeALudra = self.instance_sql.queryToPandas(commadfindCufeALudra)

                    bar.next()

                    if findCufeALudra.shape[0] > 0:
                        for indexcufe, rowCufe in findCufeALudra.iterrows():

                            dgi, qr, aludra = self.funtionValidateCufeDGI(rowCufe['Cufe'], rowCufe['QRcode'])

                            if rowCufe['PacResponse'] != '' and rowCufe['PacResponse'] is not None:
                                doc = xmltodict.parse(rowCufe['PacResponse'])
                                docjson = payload.dumps(doc)

                            commandUpdate = """
                                                        update MDL11.conteoFmottaJunio
                                                        set validate = 1,
                                                            urlQr = '{0}',
                                                            inDGI = {1},
                                                            qrAludra ={2},
                                                            Resolution = '{4}',
                                                            Response_Code = '{5}',
                                                            Auth_Protocol = '{6}',
                                                            aludra=1,
                                                            MontoAludra = {7}
                                                        where cufe = '{3}'                        
                                                    """.format(qr, 1 if dgi else 0, 1 if aludra else 0, row['cufe'],
                                                               rowCufe['Resolution'],
                                                               (rowCufe['Response_Code'] if (
                                                                           rowCufe['Response_Code'] != None and rowCufe[
                                                                       'Response_Code'] != '') else '')
                                                               , rowCufe['Auth_Protocol'] if (
                                        rowCufe['Response_Code'] is not None and rowCufe['Response_Code'] != ''
                                        and rowCufe['Response_Code'] == '0920')
                                                                                                           else (
                                    doc['rProtFe']['gInfFE']['dProtAut'] if dgi else '') if (
                                        rowCufe['PacResponse'] != '' and rowCufe['PacResponse'] is not None) else '',
                                                               rowCufe['AmountWithTax'])

                            self.instance_sql.executeCommand(commandUpdate)

                        print('Updated')
                    else:
                        dgi, qr, aludra = self.funtionValidateCufeDGI(cufe, None)
                        commandUpdate = """
                                                                            update MDL11.conteoFmottaJunio
                                                                            set validate = 1,
                                                                                urlQr = '{0}',
                                                                                inDGI = {1},
                                                                                qrAludra ={2},                                                            
                                                                                aludra=0
                                                                            where cufe = '{3}'                        
                                                                        """.format(qr, 1 if dgi else 0,
                                                                                   1 if aludra else 0, row['cufe'])

                        self.instance_sql.executeCommand(commandUpdate)

                        print('Updated')
                    time.sleep(5)
                    dataCufes = self.instance_sql.queryToPandas(commadSelectCufes)



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

        if 'CUFE, digestValue o Ambiente inexistente.' in response.text and qr is not None and qr != '':

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
