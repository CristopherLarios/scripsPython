import csv
import logging
import configparser
import requests
from progress.bar import Bar
import os
import asyncio
import aiohttp
from tqdm import tqdm

# Configuración de logging
LOG_SCRIPT = "logScript"
LOG_NAME = "validarCufeCSV"
LOGGER = logging.getLogger('{0}'.format(LOG_NAME))
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-s - %(message)s')
HANDLER = logging.FileHandler(filename='{0}.log'.format(LOG_SCRIPT), mode='a')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

class validarCufeCSV():
    def __init__(self):
        super().__init__()
        self.csvName = 'validacion.csv'
        self.urlDGI = 'https://dgi-fep.mef.gob.pa/Consultas/FacturasPorCUFE?CUFE='

    async def async_validar_cufes_continuacion(self):
        try:
            cufes_validados = set()

            with open('Cufe_No_DGI.csv', 'r', newline='') as cufeNoDGI:
                reader = csv.DictReader(cufeNoDGI)
                for row in reader:
                    cufes_validados.add(row['cufe'])

            with open('Cufe_DGI.csv', 'r', newline='') as cufeDGI:
                reader = csv.DictReader(cufeDGI, delimiter=';')
                for row in reader:
                    cufes_validados.add(row['cufe'])

            with open(self.csvName, 'r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file, delimiter=';')
                cufe_validate = list(reader)

            cufe_to_validate = [cufe for cufe in cufe_validate if cufe['cufe'] not in cufes_validados]

            cufe_batches = [cufe_to_validate[i:i + 50] for i in range(0, len(cufe_to_validate), 50)]

            async with aiohttp.ClientSession() as session:
                for cufe_batch in tqdm(cufe_batches, desc="Progreso", unit="paquete"):
                    for _ in range(3):  # Intenta hasta 3 veces mantener las peticiones antes de cancelar instrucción
                        try:
                            results = await self.async_validar_paquete_cufes(session, cufe_batch)
                            # Procesar los resultados y guardar en los archivos aquí
                            for result, cufe in zip(results, cufe_batch):
                                if result[0]:  # Si es válido
                                    with open('Cufe_DGI.csv', 'a', newline='') as file:
                                        write = csv.writer(file, delimiter=';')
                                        write.writerow([cufe['cufe'], result[1]])
                                else:
                                    with open('Cufe_No_DGI.csv', 'a', newline='') as file:
                                        write = csv.writer(file, delimiter=';')
                                        write.writerow([cufe['cufe']])
                            break  # Sale del bucle de reintento si la petición está completa con éxito
                        except Exception as e:
                            print(f"Error en el paquete {cufe_batch}: {e}")
                            await asyncio.sleep(1)  # Esperar antes del siguiente reintento             
        except Exception as e:
            print(e)

    async def async_funcion_validar_cufe(self, session, cufe):

        if not str(cufe).startswith("FE"):
            cufe = "FE"+str(cufe)
        qr = self.urlDGI + cufe
        async with session.get(qr) as response:
            if 'FECHA AUTORIZACIÓN'  in await response.text():
                return True, qr
            else:
                return False, 'No se encuentra en DGI'

    async def async_validar_paquete_cufes(self, session, cufe_batch):
        tasks = []
        for cufe in cufe_batch:
            tasks.append(self.async_funcion_validar_cufe(session, cufe['cufe']))
        return await asyncio.gather(*tasks)

    def validarCufeDGI(self):
        try:
            with open('Cufe_No_DGI.csv', 'w', newline='') as FileSaveNo:
                fieldnamesNo = ['cufe']
                writeNo = csv.DictWriter(FileSaveNo, delimiter=';', fieldnames=fieldnamesNo)
                writeNo.writeheader()
                with open('Cufe_DGI.csv', 'w', newline='') as FileSave:
                    fieldnames = ['cufe', 'qr']
                    write = csv.DictWriter(FileSave, delimiter=';', fieldnames=fieldnames)
                    write.writeheader()
                    with open(self.csvName) as File:
                        reader = csv.reader(File, delimiter=';')
                        next(File,None)
                        ##bar = Bar('Processing', max=len(list(reader)))
                        for row in reader:
                           ##print(row)
                            dgi,qr = self.funcionValidarCufe(row[0])

                            if dgi:
                                write.writerow({
                                    'cufe': row[0],
                                    'qr': qr
                                })
                            else:
                                writeNo.writerow({'cufe': row[0]})
                            ##bar.next()
                        ##bar.finish()
        except Exception as e:
            print (e)

    def funcionValidarCufe(self, cufe):
        if not str(cufe).startswith("FE"):
            cufe = "FE"+str(cufe)

        print(cufe)
        qr = self.urlDGI+cufe
        response = requests.get(qr)
        if 'FECHA AUTORIZACIÓN' in response.text :
            return True,qr
        else:
            return False,'No se encuentra en DGI' 

    def selectorDePaso(self):
        print("Elija el paso a realizar: ")
        print("1. Comenzar desde 0")
        print("2. Continuar corrida anterior")
        seleccion = input("Ingrese su selección: ")

        if seleccion == '1':
            print("¿Está seguro de iniciar desde 0? Al confirmar perderá todo el avance.")
            sn = input("S/N ")
            if sn.upper() == "S":
                self.validarCufeDGI()
            else:
                print('FIN')
        elif seleccion == '2':
            asyncio.run(self.async_validar_cufes_continuacion())  # Usar asyncio.run para ejecutar el método asincrónico
        else:
            print("Selección inválida")

if __name__ == "__main__":
    instance_update = validarCufeCSV()
    instance_update.selectorDePaso()