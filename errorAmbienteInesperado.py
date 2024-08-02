import logging
import requests
import configparser
from db.psqlserver import SqlServerConnection
from progress.bar import Bar
import json as payload
import xmltodict
import time

# logging setting
LOG_SCRIPT = "logScript"
LOG_NAME = "ambienteErrado"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)


# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')


class ambienteErrado:

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

        self.instance_sql.createPool()

    def run(self):

        apikey = CONFIG[self.enviroment]['API_KEY']
        database = self.instance_sql.database
        
        
        database = CONFIG[self.enviroment]['DATABASE_SQL']
        databasename= database.partition('_');
        
        
        

        LOGGER.info('TENANT {0}'.format(database.replace('DB_','erpmagaya')))

        df_almacenamiento = self.instance_sql.queryToPandas("""
               SELECT ElectronicInvoiceId,cufe, Payload, ElectronicInvoiceId, Resolution, PacResponse
                FROM MDL11.tblElectronicInvoice
                where
                Status =1 
                and Response_Code in ('0141')
                and year(CreatedDate) = '2024' 
                and month(CreatedDate) = 7
                and CompanyId = 'DCCCB46D-3C48-4040-89D9-2EF74134EFE5'
                """)

        LOGGER.info('Records founds {0}'.format(df_almacenamiento.shape[0]))

        df_almacenamiento = df_almacenamiento.drop_duplicates(subset=['cufe'])

        LOGGER.info('Records founds {0}'.format(df_almacenamiento.shape[0]))

        bar = Bar('Processing', max=df_almacenamiento.shape[0])

        for index, row in df_almacenamiento.iterrows():
            # print(row)

            LOGGER.info('------------------------------')
            LOGGER.info('Cufe: {0}'.format(row['cufe']))

            payloadModificado = str(row['Payload'])

            pay = payload.loads(payloadModificado)

            #print(pay)

            #print(pay['dGen']['dFechaSalida'])
            #print(pay['dGen']['dFechaEm'])
            #pay['dGen']['dFechaSalida'] = pay['dGen']['dFechaEm']

            #print(pay)

            headers = {
                'api-key': apikey,
                'Accept': 'application/json',
                "Content-Type": "application/json; charset=utf-8"
            }
            # print(payloadModificado)
            resp = requests.post(
                self.domain_api,
                headers=headers, auth=None, verify=False, json=pay)

            # print(payload.loads(resp.content))
            res = payload.loads(resp.content)
            if 'Data' in res:
                if res['Data'] != None:
                    if 'gResProc' in res['Data'][0]:
                        if res['Data'][0]['gResProc']['dCodRes'] == '0920':
                            LOGGER.info(
                                'Cufe: {0} PROCESADO, CODE: {1}, Mensaje: {2}'.format(res['Data'][0]['xProtFe'][0]['rProtFe']['dCufe'],
                                                                                      res['Data'][0]['gResProc'][
                                                                                          'dCodRes'],
                                                                                      res['Data'][0]['gResProc'][
                                                                                          'dMsgRes']))
                            print(res['Data'][0]['gResProc']['dCodRes'])

                            # if ("FE{0}".format(row['cufe']) != res['Data'][0]['xProtFe'][0]['rProtFe']['dCufe']) :
                            #     commandDelete0920 = """
                            #                                 update mdl11.tblElectronicInvoice    
                            #                                 set status =0                                                                                                                
                            #                                         where cufe = '{0}'
                            #                                                 """.format(row['cufe'])
                            #     df_update_status0920 = self.instance_sql.executeCommand(commandDelete0920)
                            #     print('delete 0920: {0}'.format(df_update_status0920))

                        elif res['Data'][0]['gResProc']['dCodRes'] == '0260':
                            LOGGER.info(
                                'Cufe: {0} ENCOLADO, CODE: {1}, Mensaje: {2}'.format(res['Data'][0]['xProtFe'][0]['rProtFe']['dCufe'],
                                                                                     res['Data'][0]['gResProc'][
                                                                                         'dCodRes'],
                                                                                     res['Data'][0]['gResProc'][
                                                                                         'dMsgRes']))
                            print(res['Data'][0]['gResProc']['dCodRes'])

                            # if ("FE{0}".format(row['cufe'])  != res['Data'][0]['xProtFe'][0]['rProtFe']['dCufe']) :
                            #     commandDelete0920 = """ update mdl11.tblElectronicInvoice    
                            #                                 set status =0                                                                                                                
                            #                                         where cufe = '{0}'
                            #                                                 """.format(row['cufe'])
                            #     df_update_status0920 = self.instance_sql.executeCommand(commandDelete0920)
                            #     print('delete 0920: {0}'.format(df_update_status0920))
                        else:
                            LOGGER.error('Cufe: {0} CODIGO DESCONOCIDO, CODE: {1}, Mensaje: {2}'.format(row['cufe'],
                                                                                                        res['Data'][0][
                                                                                                            'gResProc'][
                                                                                                            'dCodRes'],
                                                                                                        res['Data'][0][
                                                                                                            'gResProc'][
                                                                                                            'dMsgRes']))
                            print(res['Data'][0]['gResProc']['dCodRes'])
                    else:
                        LOGGER.info(
                            'No existe objeto Data {0}'.format(res))

                elif 'Status' in res and res['Status'] != None and ('dCodRes' in res['Status'] or 'dCodRes' in res['Status']['Message']):
                    dataNone = res['Status']
                    if dataNone['Code'] == 'A400' and 'Message' in dataNone and dataNone['Message'] != None:
                        dataNoneMessage = dataNone['Message']
                        LOGGER.error('Cufe: {0} CODIGO A400, CODE: {1}, Mensaje: {2}'.format(row['cufe'],
                                                                                             dataNoneMessage[
                                                                                                 'dCodRes'],
                                                                                             dataNoneMessage[
                                                                                                 'dMsgRes']))
                        # commandDelete0920 = """ update mdl11.tblElectronicInvoice    
                        #                                     set status =0                                                                                                                
                        #                                             where cufe = '{0}'
                        #                                                     """.format(row['cufe'])
                        # df_update_status0920 = self.instance_sql.executeCommand(commandDelete0920)
                        # print('delete 0920: {0}'.format(df_update_status0920))
                       


                else:
                    dataNone = res['Status']
                    print(dataNone)
                    LOGGER.error('cufe: {0} ERROR, CODE: {1}, Mensaje: {2}'.format(row['cufe'],
                                                                                   dataNone['dCodRes'] if 'dCodRes' in res['Status'] else dataNone['Code'],
                                                                                   dataNone['Message']))

            bar.next()
            ##time.sleep(10)
        bar.finish()


if __name__ == "__main__":
    instance_ambienteErrado = ambienteErrado()
    instance_ambienteErrado.run()
