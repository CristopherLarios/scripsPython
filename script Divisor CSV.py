import os
import csv
from math import ceil

def dividir_csv(archivo_entrada, directorio_salida, lineas_por_archivo):
    # Verificar si el directorio de salida existe, si no se crea
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida)

    # Leer el archivo CSV original y extraer la cabecera
    with open(archivo_entrada, 'r', newline='') as archivo_origen:
        lector_csv = csv.reader(archivo_origen)
        cabecera = next(lector_csv)

    # Calcular la cantidad de archivos resultantes
    with open(archivo_entrada, 'r', newline='') as archivo_origen:
        total_lineas = sum(1 for linea in archivo_origen) - 1  # Restar 1 para excluir la cabecera
        num_archivos = ceil(total_lineas / lineas_por_archivo)

    # Dividir el archivo original en varios archivos con la misma cabecera
    with open(archivo_entrada, 'r', newline='') as archivo_origen:
        lector_csv = csv.reader(archivo_origen)
        next(lector_csv)  # Saltar la cabecera

        for i in range(num_archivos):
            archivo_salida = os.path.join(directorio_salida, f'archivo_{i + 1}.csv')

            with open(archivo_salida, 'w', newline='') as archivo_destino:
                escritor_csv = csv.writer(archivo_destino)
                escritor_csv.writerow(cabecera)

                for _ in range(lineas_por_archivo):
                    try:
                        linea = next(lector_csv)
                        escritor_csv.writerow(linea)
                    except StopIteration:
                        break

if __name__ == "__main__":
    # Especifica el archivo CSV de entrada, el directorio de salida y la cantidad de líneas por archivo
    archivo_entrada = '/home/kgb/Descargas/PRECIO-04.csv'
    directorio_salida = 'archivos_divididos'
    lineas_por_archivo = 499  # Cambiar esta cantidad por las lineas que se desean dividir

    dividir_csv(archivo_entrada, directorio_salida, lineas_por_archivo)
