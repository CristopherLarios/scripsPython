import logging
import configparser
from db.psqlserver import SqlServerConnection
from progress.bar import Bar
import xmltodict
import json as payload
import requests


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

        self.domain_api = CONFIG[self.enviroment]['URL_API_feRecepFEDgi']
        self.apikey = CONFIG[self.enviroment]['API_KEY']

        self.instance_sql.createPool()


    def validarCufesDGI(self):

        global doc
        try:
            database = self.instance_sql.database
            LOGGER.info('TENANT {0} - Proccess {1}'.format(database.replace('DB_',''),"Validar CUFE"))

            commadSelectCufes = """
                 SELECT t.ElectronicInvoiceId,  t.Cufe as Cufe, isnull(t.QRcode,'') as QRcode, isnull(t.Response_Code,'') as Response_Code ,
                        isnull(t.Resolution,'') as Resolution, isnull(t.PacResponse,'') as PacResponse, isnull(t.Auth_Protocol,'') as Auth_Protocol,
                        Payload, Response
                FROM MDL11.tblElectronicInvoice t
                WHERE t.Status = 1 and t.Response_Code ='0260'-- and  year(createddate) = 2024
                --t.EInvoiceStatus in(2,3) 
            """

            dataCufes = self.instance_sql.queryToPandas(commadSelectCufes)

            LOGGER.info('Registros a validar {0}'.format(len(dataCufes)))

            bar = Bar('Processing', max=dataCufes.shape[0])

            for index, row in dataCufes.iterrows():
Z                try: 
                    LOGGER.info('------------------------------')
                    LOGGER.info('Cufe: {0}'.format(row['Cufe']))
                    
                    bar.next()
                    dgi,qr,aludra = self.funtionValidateCufeDGI(row['Cufe'], row['QRcode'])

                    LOGGER.info('Cufe: {0} - DGI {1} - Aludra {2}'.format(row['Cufe'],dgi,aludra))

                    if dgi:
                        if row['Auth_Protocol'] != '' and row['Auth_Protocol'] is not None:   
                            Auth_Protocol = str(row['Auth_Protocol']) 
                        elif row['PacResponse'] != '' and row['PacResponse'] is not None:
                            doc = xmltodict.parse(row['PacResponse'])
                            docjson = payload.dumps(doc)
                            Auth_Protocol = doc['rProtFe']['gInfFE']['dProtAut']
                        elif row['Response'] != '' and row['Response'] is not None:
                            response = str(row['Response'])
                            doc = payload.loads(response)
                            Auth_Protocol = doc['xProtFe'][0]['rProtFe']['gInfProt']['dProtAut']
                    

                        commandUpdate =  """
                            UPDATE MDL11.tblElectronicInvoice
                            set QRcode = '{1}',Resolution = 'Factura recibida con éxito',
                                Response_Code = '0920', InvoiceStatus ='Procesado',EInvoiceStatus = '0', Status = 1,
                                Auth_Protocol= '{2}'
                            WHERE ElectronicInvoiceId= '{0}'  
                        """.format(row['ElectronicInvoiceId'],qr,Auth_Protocol)
                        self.instance_sql.executeCommand(commandUpdate)    

                    # else:
                    #     payloadModificado = str(row['Payload'])
                    #     pay = payload.loads(payloadModificado)
                    #     headers = {
                    #     'api-key': self.apikey,
                    #     'Accept': 'application/json',
                    #     "Content-Type": "application/json; charset=utf-8"
                    #     }          

                        # pay.dGen.dFechaSalida = pay.dGen.dFechaEm
                        # pay['dGen']['dFechaSalida'] = pay['dGen']['dFechaEm'] 

                        # resp = requests.post(
                        #     self.domain_api,
                        #     headers=headers, auth=None, verify=False, json=pay)

                        # res = payload.loads(resp.content)

                        # if 'Data' in res:
                        #     if res['Data'] != None:
                        #         if 'gResProc' in res['Data'][0]:
                        #             if res['Data'][0]['gResProc']['dCodRes'] == '0920':
                        #                 LOGGER.info(
                        #                 'Cufe: {0} PROCESADO, CODE: {1}, Mensaje: {2}'.format(res['Data'][0]['xProtFe'][0]['rProtFe']['dCufe'],
                        #                                                                   res['Data'][0]['gResProc'][
                        #                                                                       'dCodRes'],
                        #                                                                   res['Data'][0]['gResProc'][
                        #                                                                       'dMsgRes']))
                        #                 dgi,qr,aludra = self.funtionValidateCufeDGI(row['Cufe'], row['QRcode'])
                        #                 if row['PacResponse'] != '' and row['PacResponse'] is not None:
                        #                     doc = xmltodict.parse(row['PacResponse'])
                        #                     docjson = payload.dumps(doc)

                        #                 if dgi:
                        #                     Auth_Protocol = doc['rProtFe']['gInfFE']['dProtAut']
                        #                     commandUpdate =  """
                        #                         UPDATE MDL11.tblElectronicInvoice
                        #                         set QRcode = '{1}',Resolution = 'Factura recibida con éxito',
                        #                             Response_Code = '0920', InvoiceStatus ='Procesado',EInvoiceStatus = '0', Status = 1,
                        #                             Auth_Protocol= '{2}'
                        #                         WHERE ElectronicInvoiceId= '{0}'  
                        #                     """.format(row['ElectronicInvoiceId'],qr,Auth_Protocol)
                        #                     self.instance_sql.executeCommand(commandUpdate)  

                        #             elif res['Data'][0]['gResProc']['dCodRes'] == '0260':
                        #                 LOGGER.info(
                        #                     'Cufe: {0} ENCOLADO, CODE: {1}, Mensaje: {2}'.format(row['Cufe'],
                        #                                                                         res['Data'][0]['gResProc'][
                        #                                                                             'dCodRes'],
                        #                                                                         res['Data'][0]['gResProc'][
                        #                                                                             'dMsgRes']))

                        #                 dgi,qr,aludra = self.funtionValidateCufeDGI(row['Cufe'], row['QRcode'])
                        #                 if row['PacResponse'] != '' and row['PacResponse'] is not None:
                        #                     doc = xmltodict.parse(row['PacResponse'])
                        #                     docjson = payload.dumps(doc)

                        #                 if dgi:
                        #                     Auth_Protocol = doc['rProtFe']['gInfFE']['dProtAut']
                        #                     commandUpdate =  """
                        #                         UPDATE MDL11.tblElectronicInvoice
                        #                         set QRcode = '{1}',Resolution = 'Factura recibida con éxito',
                        #                             Response_Code = '0920', InvoiceStatus ='Procesado',EInvoiceStatus = '0', Status = 1,
                        #                             Auth_Protocol= '{2}'
                        #                         WHERE ElectronicInvoiceId= '{0}'  
                        #                     """.format(row['ElectronicInvoiceId'],qr,Auth_Protocol)
                        #                     self.instance_sql.executeCommand(commandUpdate)
                        #             else:
                        #                 LOGGER.error('Cufe: {0} CODIGO DESCONOCIDO, CODE: {1}, Mensaje: {2}'.format(row['Cufe'],
                        #                                                                                             res['Data'][0][
                        #                                                                                                 'gResProc'][
                        #                                                                                                 'dCodRes'],
                        #                                                                                             res['Data'][0][
                        #                                                                                                 'gResProc'][
                        #                                                                                                 'dMsgRes']))
                        #     else:
                        #         LOGGER.info(
                        #                 'Cufe: {0} ERROR, CODE: {1}, Mensaje: {2}'.format(row['Cufe'], res['Errors'][0]['dCodRes'],
                        #                 res['Errors'][0]['dMsgRes']))

                                                                                                                        
                        # else:
                        #     LOGGER.info(
                        #         'No existe objeto Data {0}'.format(res))
                except Exception as e:                    
                    continue

        except Exception as e:
            print(e)


    def funtionValidateCufeDGI(self,cufe,qr):
        try:
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
            elif ('FACTURA' in response.text or "NOTA" in response.text or "REEMBOLSO" in response.text or "OTRO" in response.text) and qr is not None and qr != '':
                return True,qr,qrAludra
            elif 'FACTURA' in response.text or "NOTA" in response.text or "REEMBOLSO" in response.text or "OTRO" in response.text:
                    return True,urlDGI,qrAludra
            else:
                    return False,'',qrAludra
        except Exception as e:
            LOGGER.info('Cufe: {0}, ERROR: {1}'.format(cufe,e))
            
            





if __name__ == "__main__":
    instance_update = validarcufes()
    instance_update.validarCufesDGI()
