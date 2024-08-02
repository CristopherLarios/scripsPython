import csv
import json
import os
import requests
from tqdm import tqdm
from requests.exceptions import ConnectionError

# URL del servicio de validación de RUC por lotes
url = 'https://apim.aludra.cloud/mdl18/feConsLoteRucDV'

# Headers de la petición
headers = {
    'api-key': "veDZldz4o1xNGSW7qznWIb7bg8HobT0REozQy3A23xGiS5ScHep1rlOrgksl6BH+fbhN+7it6dfPFtSnC+VCTv597N1xLiJcqwOzzjZHVeot/GdDeOAJijPjaXh/+VLEUOKpomrDa3IVrguukp6VbhNrjmu0vmUDm4MUIJlxyEFBtgFlqdjcmgpdHfnPa49scH9iccrhu31gMBxFlJLRG5I2+H9w8u8TlQV7Atu5LPNdD51eFMtuANyBgozXlo4VBJSXOUQiqLVUw3X0vgs89/CJfSAvrbVQ1j02eRHtOd+ZXEPwaPsHcJ0gbB957HkM3wgBKYMJ80Fr92UC4JFNvQY4ZjJY+My0uXyns6Pp1w4=",
    'content-type': 'application/json'
}

# Estructura del payload de la petición
payload = {
    "gCompanyCode": "HN",
    "xRucDV": {
        "rRucDV": {
            "gRucDV": [],
        }
    }
}

# Archivo de entrada CSV con los códigos RUC a validar
input_file = 'RUCSAValidar.csv'

# Archivos de salida CSV para RUCs afiliados y no afiliados
output_file_afiliados = 'RUCSAfiliados.csv'
output_file_no_afiliados = 'RUCSNoAfiliados.csv'

# Verificar si los archivos de salida existen, si no se crean
if not os.path.exists(input_file):
    with open(input_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['dRuc', 'dTipoRuc'])

if not os.path.exists(output_file_afiliados):
    with open(output_file_afiliados, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['dRuc', 'dTipoRuc', 'dNomb', 'dDV', 'iAfilFE', 'dTipoRec', 'dCodRes', 'dMsgRes'])

if not os.path.exists(output_file_no_afiliados):
    with open(output_file_no_afiliados, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['dRuc', 'dTipoRuc', 'dNomb', 'dDV', 'iAfilFE', 'dTipoRec', 'dCodRes', 'dMsgRes'])

# Leer los RUCs ya validados del archivo de afiliados existente
afiliados_validados = set()

if os.path.exists(output_file_afiliados):
    with open(output_file_afiliados, 'r') as afiliados_file:
        reader = csv.DictReader(afiliados_file)
        for row in reader:
            afiliados_validados.add(row['dRuc'])

if os.path.exists(output_file_no_afiliados):
    with open(output_file_no_afiliados, 'r') as afiliados_file:
        reader = csv.DictReader(afiliados_file)
        for row in reader:
            afiliados_validados.add(row['dRuc'])

# Leer los códigos RUC a validar del archivo CSV de entrada
with open(input_file, 'r') as file:
    reader = csv.DictReader(file)
    rucs_to_validate = list(reader)

# Filtrar los RUCs a validar para omitir los ya validados
rucs_to_validate = [ruc for ruc in rucs_to_validate if ruc['dRuc'] not in afiliados_validados]

# Validar los códigos RUC en lotes de 50
batch_size = 1
num_batches = (len(rucs_to_validate) - 1) // batch_size + 1

for i in tqdm(range(num_batches), desc='Validando RUCs'):
    start = i * batch_size
    end = min(start + batch_size, len(rucs_to_validate))
    batch_rucs = rucs_to_validate[start:end]

    # Actualizar el payload con los códigos RUC del lote actual
    payload['xRucDV']['rRucDV']['gRucDV'] = batch_rucs

    try:
        # Realizar la petición POST
        response = requests.post(url, headers=headers, json=payload)
        response_data = json.loads(response.text)

        if response.status_code == 200:
            data = response_data['Data'][0]['xResRucDV']['rResRucDV']

            # Almacenar los resultados en los archivos CSV correspondientes
            with open(output_file_afiliados, 'a', newline='') as afiliados_file, \
                 open(output_file_no_afiliados, 'a', newline='') as no_afiliados_file:

                afiliados_writer = csv.writer(afiliados_file)
                no_afiliados_writer = csv.writer(no_afiliados_file)

                for result in data:
                    res_ruc_dv = result['gResRucDV']

                    if 'dCodRes' in res_ruc_dv['gResProc']:
                        cod_res = res_ruc_dv['gResProc']['dCodRes']
                        if cod_res == '0724':
                            afiliados_writer.writerow([
                                res_ruc_dv['dRuc'],
                                res_ruc_dv['dTipoRuc'],
                                res_ruc_dv['dNomb'],
                                res_ruc_dv['dDV'],
                                res_ruc_dv['iAfilFE'],
                                res_ruc_dv['dTipoRec'],
                                res_ruc_dv['gResProc']['dCodRes'],
                                res_ruc_dv['gResProc']['dMsgRes']
                            ])
                            afiliados_validados.add(res_ruc_dv['dRuc'])
                        elif cod_res == '0723':
                            no_afiliados_writer.writerow([
                                res_ruc_dv['dRuc'],
                                '',
                                '',
                                '',
                                res_ruc_dv['iAfilFE'],
                                '',
                                res_ruc_dv['gResProc']['dCodRes'],
                                res_ruc_dv['gResProc']['dMsgRes']
                            ])
        else:
            print(f'Error en la petición: {response.status_code} - {response.text}')

    except ConnectionError as e:
        print(f'Error de conexión: {str(e)}. Reintentando la petición...')
