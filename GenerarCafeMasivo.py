import logging
import configparser
from db.psqlserver import SqlServerConnection
from progress.bar import Bar
import xmltodict
import json as payload
import requests
from urllib3.exceptions import InsecureRequestWarning
import warnings


# logging setting
LOG_SCRIPT = "logScript"
LOG_NAME = "GenerarCafe"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')

class GenerarCafe:

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

        self.domain_api = 'https://apim.aludra.cloud/mdl18/generateFeCafe'
        self.apikey = 'RudyfIcJoI5kL7QYHJoDUbbntudwuWtlgvsl4nMno7bNoaRXeIOYVtRyYEIWIKKjpEOTm5bG9lD2K8RaVPJX05X0jK7OBox9wPpNjRULJRrSbqFQkia3JnPWb0U4WyG5MemD2jQ8Jo3gbcR1FlSufnmnDerDA6q1ASKJf6n/eS1Gw2t3lDlNAPQTDEmpVQ0mfzJOUF019++fpitKOFQ7uxUnz0LZr3+xQ8gNNS9aUlrvpctlgVIavGm2Bdg7v8vVlcsikso8asgE5Rcf1X0g2Dyu6KDI6KQfJjIHNerOodga8QSj1BvPu5o/fwuuEANtpTbJpGNITyknnf+LKVHTlu9LI3JiCagAtqGz9nhWekQ='

        self.instance_sql.createPool()

    def generar(self):
        try:
            database = self.instance_sql.database
            LOGGER.info('TENANT {0} - Proccess {1}'.format(database.replace('DB_',''),"Validar CUFE"))

            commadSelectCufes = """
                 select Cufe,c.CompanyId,c.Code from  MDL11.tblElectronicInvoice e
                        inner join CORE.tblCompany c on e.CompanyId = c.CompanyId
                        where e.cafe is null and e.NotificationOnBlob is null and e.Status = 1 and Response_Code = '0920'
            """

            dataCufes = self.instance_sql.queryToPandas(commadSelectCufes)

            LOGGER.info('Cafes A Generar: {0}'.format(len(dataCufes)))

            bar = Bar('Processing', max=dataCufes.shape[0])

            for index, row in dataCufes.iterrows():

                LOGGER.info('------------------------------')
                LOGGER.info('Cufe: {0}'.format(row['Cufe']))

                bar.next()

                
                payload = {
                    'gCompanyCode':row['Code'],
                    'dCufe': 'FE{0}'.format(row['Cufe'])
                }

                headers = {
                'api-key': self.apikey,
                'Accept': 'application/json',
                "Content-Type": "application/json; charset=utf-8"
                }
                requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
                resp = requests.post(
                self.domain_api,
                headers=headers, auth=None, verify=False,json=payload)
                warnings.resetwarnings()

                if resp.status_code != 200:
                    LOGGER.error('Cufe:{0}, CODE: {1}, Error: {2}'.format(row['Cufe'],resp.status_code, resp.reason))
                else:
                    LOGGER.info('Cufe:{0}, CODE: {1}, Info: {2}'.format(row['Cufe'],resp.status_code, resp.reason))

            bar.finish()
        except Exception as e :
            print(e)


if __name__ == "__main__":
    instance = GenerarCafe()
    instance.generar()
