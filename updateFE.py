import logging
import requests
import configparser
import time
from db.psqlserver import SqlServerConnection
from progress.bar import Bar

# logging setting
LOG_SCRIPT = "logScript"
LOG_NAME = "updateFE"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')

class updateFE:

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

        self.instance_sql.createPool()

    def run(self):
        corrida = 0
        while True:

            database = self.instance_sql.database

            LOGGER.info('TENANT {0}'.format(database.replace('DB_','')))

            df_almacenamiento = self.instance_sql.queryToPandas("""     
            select top 10000 ElectronicInvoiceId,cufe from MDL11.tblElectronicInvoice WHERE DateIssue IS NOT NULL AND (DateIssuedDateTime IS NULL OR IssuerRuc IS NULL OR Environment IS NULL)        
            """)


            if len(df_almacenamiento) > 0:
                corrida = corrida + 1  
                LOGGER.info('Corrida #{0}'.format(corrida))
                LOGGER.info('Records founds {0}'.format(df_almacenamiento.shape[0]))
                bar = Bar('Processing', max=df_almacenamiento.shape[0])

                # in_clause = "IN("
                query_Update = ""
                for index, fe in df_almacenamiento.iterrows():
                    query_Update += """ 
                    UPDATE MDL11.tblElectronicInvoice
                        SET DateIssuedDateTime =
                            CASE
                                WHEN DateIssue IS NOT NULL AND TRY_CONVERT(DATETIME2(0), DateIssue, 126) IS NOT NULL THEN CONVERT(DATETIME2(0), DateIssue)
                                WHEN ISDATE(CONVERT(VARCHAR(50), CreatedDate, 126)) = 1 OR CreatedDate IS NULL THEN CONVERT(datetime2(0), SUBSTRING(CUFE, 31, 4) + '-' + SUBSTRING(CUFE, 35, 2) + '-' + SUBSTRING(CUFE, 37, 2), 126)
                                ELSE CreatedDate
                            END,
                            IssuerRuc = STUFF(SUBSTRING(Cufe, 4, 20), 1, PATINDEX('%[^0]%', SUBSTRING(Cufe, 4, 20)) - 1, ''),
                            Environment = CAST(CONVERT(VARCHAR(1), SUBSTRING(Cufe, 54,1)) AS tinyint),
                            IssuingType = CAST(CONVERT(VARCHAR(2), SUBSTRING(Cufe, 52,2)) AS NVARCHAR(2))
                    WHERE ElectronicInvoiceId = '{0}'; 

                    """.format(fe['ElectronicInvoiceId'])
                   # in_clause += "'{0}',".format(fe['ElectronicInvoiceId'])
                    
                   # LOGGER.info('{1}- Actualizado {0}'.format(fe['cufe'], index))

                    bar.next()
                bar.finish()

                # in_clause= in_clause[:-1]
                # in_clause +=")"
                
                self.instance_sql.executeCommand(query=query_Update)
                
            else :
                break; 
        
if __name__ == "__main__":
    instance_update = updateFE()
    instance_update.run()
