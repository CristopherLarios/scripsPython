import configparser
import json
import logging
from tabulate import tabulate
import requests
from db.psqlserver import SqlServerConnection
from progress.bar import Bar

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

class reporte_documentos:

    def __init__(self):
        super().__init__()
        #init variables
        self.enviroment = 'prod'
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = CONFIG[self.enviroment]['SERVER_SQL']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']
        self.apiCore = 'https://apim.aludra.cloud/core/validateApiKey'
        
        #Creando instancia
        self.instance_sql.createPool()

    def run(self):
        
        listTenantConte = []
        listCompanyConteo = []
         #Obteniendo tenant   
        listTenant = self.listadoTenant()

        for  tenant in listTenant:
            apikey= self.findApiKey(tenant=tenant)
            if apikey != None:
                dataSourse = self.getDataSource(apikey) 
                if dataSourse != None:
                    dataSourse.createintance()
                    listCompany = self.getCompanyCertificate(datasource=dataSourse)

                    listTenantCompanyConteo = []
                    for company in listCompany:
                        conteo = self.geneareConteo(dataSourse, company)
                        listTenantCompanyConteo.append((tenant, company, conteo))

                    listTenantConte.extend(listTenantCompanyConteo)
            
            datos = []
            columnas = [ 'Tenant','Company', 'Conteo', 'Resolution', 'Response_Code']
            
            for tenant, company, conteo in listTenantConte:
                for cont in conteo:
                    datos.append([tenant.CompanyName, company.Name,  cont.conteo, cont.Resolution, cont.Response_Code])

            datos.sort(key=lambda x: x[0])  # Ordenar por Company

            html_content = """
            <html>
                <head>
                    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.25/css/jquery.dataTables.min.css">
                    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
                    <script src="https://cdn.datatables.net/1.10.25/js/jquery.dataTables.min.js"></script>
                    <style>{css}</style>
                </head>
                <body>
                    <table id="data-table">
                        <thead>
                            <tr>{headers}</tr>
                        </thead>
                        <tbody>{body}</tbody>
                    </table>

                    <script>
                        $(document).ready(function() {{
                            $('#data-table').DataTable({{
                                "order": [[0, "asc"]]  // Ordenar por Company en orden ascendente
                            }});
                        }});
                    </script>
                </body>
            </html>
            """.format(
                css="""
                    table {{
                        border-collapse: collapse;
                    }}
                    th, td {{
                        border: 1px solid black;
                        padding: 8px;
                        text-align: left;
                    }}
                """,
                headers="".join(["<th>{}</th>".format(col) for col in columnas]),
                body="".join(["<tr>{}</tr>".format("".join(["<td>{}</td>".format(cell) for cell in row])) for row in datos])
            )

            with open("output.html", "w") as file:
                file.write(html_content)
            # for respuesta in listTenantConte:
                # print(respuesta.tenant.CompanyName)
                # for compa in respuesta.companyConteo:
                #     print(compa.companies.Name)
                #     for cont in compa.conteos:
                #         print("{0}  | {1} | {2} ".format(cont.conteo,cont.Resolution,cont.Response_Code))
                        
        
    
    def listadoTenant(self):
        LOGGER.info("Obteniendo tenant")
        queryTenant = "SELECT  SaaSId, CompanyName, InitialCatalog FROM dbo.tblTenant WHERE InitialCatalog NOT LIKE '%BK%' ORDER BY SaaSId asc"

        LOGGER.info("Ejecutando query teant: {0}".format(queryTenant))

        tenantData= self.instance_sql.queryToPandas(query=queryTenant)

        LOGGER.info('Tenant Encontrados {0}'.format(tenantData.shape[0]))

        bar = Bar('Processing', max=tenantData.shape[0])

        LOGGER.info("Recorriendo tenants")

        listTenat = []

        for index, row in tenantData.iterrows():
            LOGGER.info('------------------------------')
            LOGGER.info("SaaSId: {0}".format(row['SaaSId']))
            LOGGER.info("CompanyName: {0}".format(row['CompanyName']))
            LOGGER.info("InitialCatalog: {0}".format(row['InitialCatalog']))

            listTenat.append(tenant(row['SaaSId'],row['CompanyName'],row['InitialCatalog']))

        return listTenat

    def findApiKey(self,tenant):
        LOGGER.info("Obteniendo apikey tenant {0} tenantId {1} ".format(tenant.CompanyName,tenant.SaaSId))

        queryApiKey = "SELECT CompanyId,ApiKey  FROM dbo.tblApiKey	WHERE CompanyId = {0} AND Status = 1".format(tenant.SaaSId)

        apiKeys = self.instance_sql.queryToPandas(query=queryApiKey)

    #if  apiKeys.count(axis=0) > 0:
        for index, apikey in apiKeys.iterrows():
            LOGGER.info("Validando ApiKey")

            headers = {
            'Accept': 'application/json',
            "Content-Type": "application/json; charset=utf-8"
            }

            apikeyJson = {'apiKey': apikey['ApiKey'] }
            #apikeyJsonToPython  = json.loads(apikeyJson)

            resp = requests.post(
            self.apiCore,
            headers=headers, auth=None, verify=False, json=apikeyJson)

            if resp.status_code != 200:
                LOGGER.error("Error al validar apikey")
            elif resp.status_code == 200:
                if 'Data' in resp.json() and 'Status' in resp.json():
                    if resp.json()['Status']['Code'] == 200:
                        return apiKey(apikey['CompanyId'], apikey['ApiKey'])

    #else:
    #    LOGGER.error("El cliente {0} no tiene apikey".format(tenant.CompanyName))
        return None


    def getDataSource(self, apikey):
        headers = {
                'Accept': 'application/json',
                "Content-Type": "application/json; charset=utf-8"
                }
        
        apikeyJson = {'apiKey': apikey.ApiKey }
        #apikeyJsonToPython  = json.loads(apikeyJson)

        resp = requests.post(
            self.apiCore,
            headers=headers, auth=None, verify=False, json=apikeyJson)

        if resp.status_code != 200:
            LOGGER.error("Error al validar apikey")
        elif resp.status_code == 200:
            if 'Data' in resp.json() and 'Status' in resp.json():
                if resp.json()['Status']['Code'] == 200:
                    return tenantDataSource(str(resp.json()['Data']['DataSource']), CONFIG[self.enviroment]['USER_NAME_SQL'],
                                            CONFIG[self.enviroment]['PASSWORD_SQL'],str(resp.json()['Data']['InitialCatalog']))
                else:
                    return None
                
    def getCompanyCertificate(self, datasource):

        listCompany = []
        queryCompanies ="SELECT a.CompanyId,a.Code, a.Name FROM core.tblCompany a LEFT JOIN [MDL11].[tblClientCertificates] b on a.[CompanyId] =  b.[CompanyId]"
        
        comapnies = datasource.instance_sql.queryToPandas(queryCompanies)

        for index, company in comapnies.iterrows():
            listCompany.append(companies(company['CompanyId'],company['Code'],company['Name']))
        return listCompany
    
    def geneareConteo(self,datasource, company):
        listConteo = []
        queryConteo = """
                    select count(*) as conteo, Resolution, Response_Code from mdl11.tblElectronicInvoice
                    where CreatedDate >= '2023-07-11 19:00:22.623' and Status = 1 and CompanyId ='{0}'
                    group by  Resolution, Response_Code
                    """.format(company.CompanyId)
        
        conteo = datasource.instance_sql.queryToPandas(queryConteo)

        for index, count in conteo.iterrows():
            listConteo.append(conteos(count['conteo'],count['Resolution'],count['Response_Code']))
        
        return listConteo

    




class tenant:
    
    def __init__(self,SaaSId,CompanyName,InitialCatalog):
         self.InitialCatalog = InitialCatalog
         self.SaaSId = SaaSId
         self.CompanyName = CompanyName


class apiKey:
    def __init__(self,CompanyId,ApiKey):
        self.ApiKey = ApiKey
        self.CompanyId = CompanyId

class tenantDataSource :
    def __init__(self,DataSource,Username,Password,InitialCatalog):        
        self.instance_sql = SqlServerConnection()
        self.DataSource=DataSource
        self.Username = Username
        self.Password = Password
        self.InitialCatalog = InitialCatalog

    def createintance(self):
        self.enviroment = 'prod'
        self.instance_sql.server = self.DataSource
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = self.Password
        self.instance_sql.username = self.Username
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = self.InitialCatalog        
        #Creando instancia
        self.instance_sql.createPool()

class companies:
    def __init__(self,CompanyId,Code,Name):
        self.CompanyId =CompanyId
        self.Code = Code
        self.Name = Name

class conteos:
    def __init__(self,conteo,Resolution,Response_Code):
        self.conteo =conteo
        self.Resolution= Resolution
        self.Response_Code =Response_Code
        
class CompanyConteo:
    def __init__(self,companies,conteos):
        self.companies = companies 
        self.conteos = conteos    

class tenantCompaniConteo:
    def __init__(self,tenant,companyConteo):
        self.tenant = tenant        
        self.CompanyConteo = companyConteo



if __name__ == "__main__":
    instance_ambienteErrado = reporte_documentos()
    instance_ambienteErrado.run()

        



