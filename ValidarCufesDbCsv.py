import csv
import logging
import configparser
import requests
from progress.bar import Bar
import os
import asyncio
import aiohttp
from db.psqlserver import SqlServerConnection
from tqdm import tqdm

LOG_SCRIPT = "logScript"
LOG_NAME = "validarCufeCSV"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# load credentials
CONFIG = configparser.ConfigParser()
CONFIG.read('credential.ini')

class virificacion():
    def __init__(self):
        super().__init__()
        #Declarand variables de clase
        self.enviroment = 'prod'
        self.instance_sql = SqlServerConnection()
        self.instance_sql.server = CONFIG[self.enviroment]['SERVER_SQL']
        self.instance_sql.driver = CONFIG[self.enviroment]['DRIVER']
        self.instance_sql.password = CONFIG[self.enviroment]['PASSWORD_SQL']
        self.instance_sql.username = CONFIG[self.enviroment]['USER_NAME_SQL']
        self.instance_sql.port = CONFIG[self.enviroment]['PORT_SQL']
        self.instance_sql.database = CONFIG[self.enviroment]['DATABASE_SQL']
        self.urlDGI = 'https://dgi-fep.mef.gob.pa/Consultas/FacturasPorCUFE?CUFE=FE'       

    async def run(self):
        self.instance_sql.createPool()
        database = self.instance_sql.database

        LOGGER.info('TENANT {0}'.format(database.replace('DB_','')))        

        if not os.path.exists('Cufe_DGI_{0}.csv'.format(database.replace('DB_',''))):
            with open('Cufe_DGI_{0}.csv'.format(database.replace('DB_','')), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Cufe', 'QR'])

        if not os.path.exists('Cufe_No_DGI_{0}.csv'.format(database.replace('DB_',''))):
            with open('Cufe_No_DGI_{0}.csv'.format(database.replace('DB_','')), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Cufe'])


    #pagina = 0
    #while True:
        try:
            
           # pagina += 1
            df_almacenamiento = self.instance_sql.queryToPandas("""
                    select  Cufe from MDL11.tblElectronicInvoice
                    where CAST(CreatedDate AS DATE) BETWEEN '2024-01-31' AND '2024-02-4' 
                    and status = 1
                    order by Cufe
                    --OFFSET ({0} - 1) * 1000 ROWS
                    --FETCH NEXT 1000 ROWS ONLY;
                    """
                    #.format(pagina)
                    )
            
            LOGGER.info('Records founds {0}'.format(df_almacenamiento.shape[0]))

            if df_almacenamiento.shape[0] > 0:
                cufelist = [list(row) for row in df_almacenamiento.iterrows()]
                cufe_batches = [cufelist[i:i + 1000] for i in range(0, len(cufelist), 1000)]

                async with aiohttp.ClientSession() as session:
                    for cufe_batch in tqdm(cufe_batches, desc="Progreso", unit="paquete"):
                        for _ in range(3):  # Intenta hasta 3 veces mantener las peticiones antes de cancelar instrucción
                            try:
                                results = await self.async_validar_paquete_cufes(session, cufe_batch)
                                # Procesar los resultados y guardar en los archivos aquí
                                for result, cufe in zip(results, cufe_batch):
                                    if result[0]:  # Si es válido
                                        with open('Cufe_DGI_{0}.csv'.format(database.replace('DB_','')), 'a', newline='') as file:
                                            write = csv.writer(file, delimiter=';')
                                            write.writerow([cufe[1]['Cufe'], result[1]])
                                    else:
                                        with open('Cufe_No_DGI_{0}.csv'.format(database.replace('DB_','')), 'a', newline='') as file:
                                            write = csv.writer(file, delimiter=';')
                                            write.writerow([cufe[1]['Cufe']])
                                break  # Sale del bucle de reintento si la petición está completa con éxito
                            except Exception as e:
                                print(f"Error en el paquete: {e}")
                                await asyncio.sleep(1)  # Esperar antes del siguiente reintento       
            # else:
            #    break

        except Exception as e:
            print(e)

    async def async_funcion_validar_cufe(self, session, cufe):
        qr = self.urlDGI + cufe
        async with session.get(qr) as response:
            try:
                if 'FACTURA' in await response.text() or "NOTA" in await response.text() or 'REEMBOLSO' in await response.text() or 'OTRO' in await response.text():
                    return True, qr
                else:
                    return False, 'No se encuentra en DGI'
            except Exception as e:
                print(f"Error al consultar en DGI: {e}")
                await asyncio.sleep(1)  
            
    async def async_validar_paquete_cufes(self, session, cufe_batch):
        try:
            tasks = []
            for cufe in cufe_batch:
               # print(str(cufe[1]['Cufe']).replace('Cufe','').replace(" ","", ))
                tasks.append(self.async_funcion_validar_cufe(session, str(cufe[1]['Cufe']).replace('Cufe','').replace(" ","", )))
            return await asyncio.gather(*tasks)
        except Exception as e:
             print(f"Error: {e}")
    


if __name__ == "__main__":
    instance_update = virificacion()
    asyncio.run(instance_update.run())
