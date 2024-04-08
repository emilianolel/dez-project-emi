#!/bin/bash

WORKDIR='./'
INEGI_RAW='inegi_crimenes.csv'
INEGI_CLEAN='inegi_crimenes_clean.csv'

# Definir la URL de la solicitud
url='https://www.inegi.org.mx/sistemas/olap/exporta/exporta.aspx'

# Definir los datos de la solicitud en formato URL-encoded
data='nomdimfila=A%F1o+de+registro%7CEnt+y+mun+de+registro&to_display=&cube=Mortalidad+por+homicidios&cubeName=Mortalidad+por+homicidios&nomdimColumna=Mes+de+registro&Lc_tituloFiltro=Consulta+de%3A+Defunciones+por+homicidio+%A0+Por%3A+A%F1o+de+registro+y+Ent+y+mun+de+registro+%A0+Seg%FAn%3A+Mes+de+registro&Lc_encabeza=%5C%5CTotal%5CEnero%5CFebrero%5CMarzo%5CAbril%5CMayo%5CJunio%5CJulio%5CAgosto%5CSeptiembre%5COctubre%5CNoviembre%5CDiciembre%5C&Lc_unidadmedida=&Lc_sql=select+NON+EMPTY+ToggleDrillState%28%7B%5BMes+de+registro%5D.%5BMes+de+registro%5D.%5BTotal%5D%7D%2C%7B%5BMes+de+registro%5D.%5BMes+de+registro%5D.%5BTotal%5D%7D%29+on+columns%2C+NON+EMPTY+ToggleDrillState%28crossjoin%28%7B%5BA%F1o+de+registro%5D.levels%280%29.allmembers%7D%2C+%7B%5BEnt+y+mun+de+registro%5D.%5BEnt+y+mun+de+registro%5D.%5BTotal%5D%7D%29%2C%7B%5BEnt+y+mun+de+registro%5D.%5BEnt+y+mun+de+registro%5D.%5BTotal%5D%7D%29+on+rows+from+%5BMortalidad+por+homicidios%5D+where+%28%5BMeasures%5D.%5BDefunciones+por+homicidio%5D%29&Lc_conexion=provider%3DMSOLAP.5%3BMDX+Compatibility%3D2%3Bdata+source%3DW-OLAPCLPRO12%3BConnect+timeout%3D120%3BInitial+catalog%3DMORTALIDAD_GENERAL&Lc_titulo=Defunciones+por+homicidios%7C&Lc_piepagina=FUENTE%3A+INEGI.+Estad%EDsticas+de+mortalidad.&Lc_salida=0&Lc_StrConexion=1&Lc_formato=Texto+separado+por+comas%28.csv%29&Lc_sql_allmembers=select+NON+EMPTY+ToggleDrillState%28%7B%5BMes+de+registro%5D.%5BMes+de+registro%5D.%5BTotal%5D%7D%2C%7B%5BMes+de+registro%5D.%5BMes+de+registro%5D.%5BTotal%5D%7D%29+on+columns%2C+NON+EMPTY+ToggleDrillState%28crossjoin%28%7B%5BA%F1o+de+registro%5D.levels%280%29.allmembers%7D%2C+%7B%5BEnt+y+mun+de+registro%5D.%5BEnt+y+mun+de+registro%5D.%5BTotal%5D%7D%29%2C%7B%5BEnt+y+mun+de+registro%5D.%5BEnt+y+mun+de+registro%5D.%5BTotal%5D%7D%29+on+rows+from+%5BMortalidad+por+homicidios%5D+where+%28%5BMeasures%5D.%5BDefunciones+por+homicidio%5D%29&Lc_ValidaDimGeo=0&completo=completo&Cant_Col=13&Cant_Fil=1056'

# Definir los encabezados de la solicitud
headers=(
    'Cache-Control: no-cache'
    'Connection: keep-alive'
    'Content-Type: application/x-www-form-urlencoded'
    'Cookie: ASPSESSIONIDAQSCARAS=JEIDJNMAEEPAIIHJBDKKNCJH; NSC_MC_tjtufnbt_2=ffffffff09911c9a45525d5f4f58455e445a4a423660; NSC_MC_OvfwpQpsubm=ffffffff0991142a45525d5f4f58455e445a4a423660'
    'Origin: https://www.inegi.org.mx'
    'Pragma: no-cache'
    'Referer: https://www.inegi.org.mx/sistemas/olap/consulta/general_ver4/MDXQueryDatos.asp?'
    'Sec-Fetch-Dest: document'
    'Sec-Fetch-Mode: navigate'
    'Sec-Fetch-Site: same-origin'
    'Sec-Fetch-User: ?1'
    'Upgrade-Insecure-Requests: 1'
)

# Hacer la solicitud con curl y guardar la respuesta en un archivo CSV
curl --retry 20 "$url" -H "${headers[@]}" --data-raw "$data" --compressed > ${WORKDIR}${INEGI_RAW} 2>&-


gawk -F ',' '!/(Defunciones|Total|FUENTE|^$)/ && length($2) > 0 { print }' ${WORKDIR}${INEGI_RAW} | gsed -e "s/\r//g" > ${INEGI_CLEAN}
