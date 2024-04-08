#Uso de la API del DENUE de INEGI

#Librerías
import requests

import pandas as pd
pd.set_option('display.max_columns', None)
import io
#import folium
#from folium import plugins

TOKEN = '2bef2a37-7672-81b7-1d79-90827e4a4b02'

import requests
import pandas as pd

url = 'https://www.inegi.org.mx/sistemas/olap/exporta/exporta.aspx'

# Datos de la solicitud
data = {
    'nomdimfila': 'Año de registro|Ent y mun de registro',
    'to_display': '',
    'cube': 'Mortalidad por homicidios',
    'cubeName': 'Mortalidad por homicidios',
    'nomdimColumna': 'Mes de registro',
    'Lc_tituloFiltro': 'Consulta de: Defunciones por homicidio | Por: Año de registro y Ent y mun de registro | Según: Mes de registro',
    'Lc_encabeza': '\\Total\\Enero\\Febrero\\Marzo\\Abril\\Mayo\\Junio\\Julio\\Agosto\\Septiembre\\Octubre\\Noviembre\\Diciembre\\',
    'Lc_unidadmedida': '',
    'Lc_sql': 'select NON EMPTY ToggleDrillState({[Mes de registro].[Mes de registro].[Total]},{[Mes de registro].[Mes de registro].[Total]}) on columns, NON EMPTY ToggleDrillState(crossjoin({[Año de registro].levels(0).allmembers}, {[Ent y mun de registro].[Ent y mun de registro].[Total]}), {[Ent y mun de registro].[Ent y mun de registro].[Total]}) on rows from [Mortalidad por homicidios] where ([Measures].[Defunciones por homicidio])',
    'Lc_conexion': 'provider=MSOLAP.5;MDX Compatibility=2;data source=W-OLAPCLPRO12;Connect timeout=120;Initial catalog=MORTALIDAD_GENERAL',
    'Lc_titulo': 'Defunciones por homicidios|',
    'Lc_piepagina': 'FUENTE: INEGI. Estadísticas de mortalidad.',
    'Lc_salida': '0',
    'Lc_StrConexion': '1',
    'Lc_formato': 'Texto separado por comas(.csv)',
    'Lc_sql_allmembers': 'select NON EMPTY ToggleDrillState({[Mes de registro].[Mes de registro].[Total]},{[Mes de registro].[Mes de registro].[Total]}) on columns, NON EMPTY ToggleDrillState(crossjoin({[Año de registro].levels(0).allmembers}, {[Ent y mun de registro].[Ent y mun de registro].[Total]}), {[Ent y mun de registro].[Ent y mun de registro].[Total]}) on rows from [Mortalidad por homicidios] where ([Measures].[Defunciones por homicidio])',
    'Lc_ValidaDimGeo': '0',
    'completo': 'completo',
    'Cant_Col': '13',
    'Cant_Fil': '1056'
}

headers = {
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'ASPSESSIONIDAQSCARAS=JEIDJNMAEEPAIIHJBDKKNCJH; NSC_MC_tjtufnbt_2=ffffffff09911c9a45525d5f4f58455e445a4a423660; NSC_MC_OvfwpQpsubm=ffffffff0991142a45525d5f4f58455e445a4a423660',
    'Origin': 'https://www.inegi.org.mx',
    'Pragma': 'no-cache',
    'Referer': 'https://www.inegi.org.mx/sistemas/olap/consulta/general_ver4/MDXQueryDatos.asp?',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1'
}

try:
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    
    # Leer los datos en un DataFrame de pandas
    df = pd.read_csv(io.StringIO(response.text))
    
    # Guardar el DataFrame en un archivo CSV
    df.to_csv('INEGI_exporta.csv', index=False)
    
    print("Consulta exitosa. Datos guardados en 'INEGI_exporta.csv'.")
except requests.exceptions.RequestException as e:
    print("Error al hacer la solicitud:", e)

