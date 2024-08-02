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


class moverData:
    def __init__(self):
        super().__init__()
        self.tenantId = 141
        self.enviroment = 'prod'
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = CONFIG[self.enviroment]['SERVER_SQL']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']

        self.instance_sql.createPool()

        

    def run(self):

        dfQa= tenantDataSource(self)
        dfQa.createintance()
        database = self.instance_sql.database

        LOGGER.info('TENANT {0}'.format(database.replace('DB_','')))

        df_almacenamiento = self.instance_sql.queryToPandas("""
        SELECT LinkId, ProductId, ExternalCode, ExternalSystem, Description,
           CreatedDate, CreatedBy, LastModifiedDate, LastModifiedBy, Status,
            IntegratedSystemId FROM MDL04.pltblProductExternalCode
        """)
        
        LOGGER.info('Records founds {0}'.format(df_almacenamiento.shape[0]))

        bar = Bar('Processing', max=df_almacenamiento.shape[0])

        conteo = 0

        insert = """"""

        for index, row in df_almacenamiento.iterrows():
            print(row)
            conteo += 1
            LOGGER.info('------------------------------')
            LOGGER.info('LogId: {0}'.format(row['LogId']))

            insert += """
                    INSERT INTO MDL04.pltblProductExternalCode (LinkId, ProductId, ExternalCode,
                    ExternalSystem, Description, CreatedDate, CreatedBy, LastModifiedDate,
                    LastModifiedBy, Status, IntegratedSystemId)
                    VALUES ('{0}'}, -- LinkId - uniqueidentifier
                    '{1}', -- ProductId - uniqueidentifier
                    '{2}', -- ExternalCode - nvarchar(50)
                    '{3}', -- ExternalSystem - nvarchar(100)
                    '{4}', -- Description - nvarchar(150)
                    '{5}', -- CreatedDate - datetime
                    '{6}', -- CreatedBy - uniqueidentifier
                    '{7}', -- LastModifiedDate - datetime
                    '{8}', -- LastModifiedBy - uniqueidentifier
                    '{9}', -- Status - bit
                    '{10}' -- IntegratedSystemId - uniqueidentifier
                        );

                    """.format(row['LinkId'],row['ProductId'],row['ExternalCode'],row['ExternalSystem'],row['Description'],row['CreatedDate'],row['CreatedBy']
                               ,row['LastModifiedDate'],row['LastModifiedBy'],row['Status'],row['IntegratedSystemId'])
            
            if(conteo == 1000):
                dfQa.instance_sql.executeCommand(insert)
                print ("Insertado: {0}".format(conteo))


            



class tenantDataSource :
    def __init__(self):        
        self.instance_sql = SqlServerConnection()

    def createintance(self):
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = CONFIG[self.enviroment]['sql-aludra-qa.database.windows.net']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']       
        #Creando instancia
        self.instance_sql.createPool()
