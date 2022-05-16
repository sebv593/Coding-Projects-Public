# -*- coding: utf-8 -*-
"""
Created on Sat May 14 13:16:39 2022

@author: sebvas
"""

########################################################################
############################# REFERENCES ###############################
########################################################################
### TAXDOWN TUTORIAL FOR ETORO ###
# https://help.taxdown.es/es/articles/6108952-como-pasar-de-etoro-a-csv-taxdown
# eToro account statement spanish version to be used due to tutorial being in this language

########################################################################
############################# LIBRARIES ################################
########################################################################

import pandas as pd
import numpy as np
import re 
# Script with private paths to documents and outputs
import eToroPrivatePathsLib


# API with geography data - https://datahub.io/core/country-codes#data-cli
# Uncomment to install if not available already in your libraries list
# !pip install datapackage
import datapackage

########################################################################
####################### EXTERNAL DATA SOURCES ##########################
########################################################################
data_url = 'https://datahub.io/core/country-codes/datapackage.json'

# To load Data Package into storage
package = datapackage.Package(data_url)

# To load only tabular data
resources = package.resources
for resource in resources:
    if resource.tabular:
        geography_complete_data = pd.read_csv(resource.descriptor['path'])

# Extracting just the columns of interest
geography_data_subset = geography_complete_data.loc[:, ['CLDR display name', 'ISO3166-1-Alpha-2', 'Region Name']] \
                                               .rename(columns = {'CLDR display name': 'Conuntry Name', 'ISO3166-1-Alpha-2': 'ISO Code 2'})

# Creating a column to define EU and Non-EU countries
geography_data_subset['EU Flag'] = geography_data_subset['Region Name'].apply(lambda region: 'EU' if region == 'Europe' else 'non-EU')

geography_data_mapping_iso2_EU_flag = dict(geography_data_subset[['ISO Code 2', 'EU Flag']].values)

########################################################################
############################# VARIABLES ################################
########################################################################
tax_period = '2021'

# Path of the excel document downloaded from eToro - reference to TAXDOWN TUTORIAL FOR ETORO at the beginning of this script for instructions
etoro_excel_file = eToroPrivatePathsLib.etoro_excel_file
closed_positions_sheet_name = 'Posiciones cerradas'
dividends_sheet_name = 'Dividendos'
etoro_account_statement_currency = 'USD'
etoro_date_columns = ['Fecha de apertura', 'Fecha de cierre']
etoro_date_columns_format = '%d/%m/%Y %H:%M:%S'

# Path of the empty .csv downloaded from Taxdown - reference to TAXDOWN TUTORIAL FOR ETORO at the beginning of this script for instructions
taxdown_template = eToroPrivatePathsLib.taxdown_template
taxdown_date_format = '%d/%m/%Y'
# Path where the processed file will be stored - adapt per your convenience
taxdown_output_path = eToroPrivatePathsLib.taxdown_output_path + tax_period + '.csv'

########################################################################
############################## FUNCTIONS ###############################
########################################################################

def extract_iso_2_from_ISIN(df, ISIN_column_name):
    """
    Function to extract iso code 2 from ISIN code

    Parameters
    ----------
    df : DATAFRAME
        DATA FRAME TO BE PROCESSED.
    ISIN_column_name : STRING
        NAME OF THE COLUMN WITH THE ISIN CODE.

    Returns
    -------
    OBJECT
        Object with the extraction of the iso code 2.

    """
    return df[ISIN_column_name].apply(lambda ISIN: np.nan if ISIN is np.nan else ISIN[:2])

def find_position_type (row):    
    """
    

    Parameters
    ----------
    row : ROW
        Row comming from a dataframe applied a lambda function at the row level to define the position type depending on a combination
        of factors within the row. The logic is defined as follow:

    ES -> type: según el tipo de operación y de activo (esto aparece en la columna “Tipo”) tendrás que poner:
      BUY_ETF: para compra de ETF 
      SELL_ETF: para la venta de ETF 
      BUY_SHARE_UE: para la compra de acciones europeas 
      SELL_SHARE_UE: para la venta de acciones europeas 
      BUY_SHARE_FOREIGNER: para la compra de acciones de fuera de la UE 
      SELL_SHARE_FOREIGNER: para la venta de acciones de fuera de la UE 
      BUY_FORWARD: para la compra de CFDs 
      SELL_FORWARD: para la venta de CFDs 
      BUY_CRYPTO: para la compra de crypto 
      SELL_CRYPTO: para la venta de crypto

    Returns
    -------
    temp_var : STRING
        String with the position type.

    """

    # Initializing an empty variable which will take a label to then change based on it being a BUY or SELL operation.    
    temp_var = np.nan
    
    # Conditional analysis to define the position type based on the logic above
    if  row['Tipo'] == 'ETF':
        temp_var = 'BUY_ETF'
    elif row['Tipo'] == 'Fondo de inversión':
        temp_var = 'BUY_FUND'
    elif row['Tipo'] == 'CFD':
        temp_var = 'BUY_FORWARD'
    elif row['Tipo'] == 'Cryptos':
        temp_var = 'BUY_CRYPTO'
    elif row['Tipo'] == 'Derechos de suscripción':
        temp_var = 'BUY_SUSCRIPTION_RIGHT'
    elif row['Tipo'] == 'Acciones':
        if row['origen_transaccion'] == 'EU':
            temp_var = 'BUY_SHARE_UE'
        elif row['origen_transaccion'] == 'non-EU':
            temp_var = 'BUY_SHARE_FOREIGNER'
        else:
            temp_var = 'BUY_SHARE_NO_QUOTED'
    else:
        temp_var= 'not_defined'
    
    # Evaluate the type of transaction to change the text to SELL for this kind of operations
    if row['tipo_transaccion'] == 'Sell':
        temp_var = temp_var.replace('BUY', 'SELL')
    
    return temp_var            
    ''

########################################################################
################################ CODE ##################################
########################################################################

######################### UNPROCESSED FILES ############################
# Reding Excel file directly downloaded from eToro
df_closed_positions = pd.read_excel(etoro_excel_file, sheet_name = closed_positions_sheet_name)
df_dividends = pd.read_excel(etoro_excel_file, sheet_name = dividends_sheet_name)
# Reading TaxDown csv template
df_taxdown = pd.read_csv(taxdown_template, sep = None)

######################## CLOSED POSITIONS ETL ###########################
# Copy of the template for closed positions
df_taxdown_closed_positions = df_taxdown.copy()

### CLOSED POSITIONS PRE-PROCESSING ###
# Extracting action type (buy, sell) from its text
df_closed_positions['tipo_transaccion'] = df_closed_positions['Acción'].apply(lambda action_text: 'Buy' if re.search('Buy', action_text) else 'Sell')
# Iterate over date columns and ransforming dates type format to 'DD/MM/YYYY'
for column in etoro_date_columns:
    df_closed_positions[column] = pd.to_datetime(df_closed_positions[column], format = etoro_date_columns_format).dt.strftime(taxdown_date_format)
# Extracting ISO Code 2 from ISIN code
df_closed_positions['ISO Code 2'] = extract_iso_2_from_ISIN(df_closed_positions, 'ISIN')
# Defining the origin of the asset
df_closed_positions['origen_transaccion'] = df_closed_positions['ISO Code 2'].map(geography_data_mapping_iso2_EU_flag)

### COMMON PROCESSING FOR ANY ASSET TYPE ###
# ES-> description: copia lo que pone en la columna “Acción” (pon lo mismo en las filas que dupliques)
df_taxdown_closed_positions.description = df_closed_positions['Acción']
# ES-> isin: copia lo que pone en la columna "ISIN" (pon lo mismo en las filas que dupliques)
df_taxdown_closed_positions['isin'] = df_closed_positions['ISIN']
# ES-> amount: copia lo que pone en la columna "unidades" (tanto para la fila de la venta como para la de la compra)
df_taxdown_closed_positions.amount = df_closed_positions['Unidades']
# ES -> retention: pon 0,00
df_taxdown_closed_positions.retention = 0
# ES -> retentionDetail: déjalo vacío
df_taxdown_closed_positions.retentionDetail = np.nan
# ES -> currency: Etoro habitualmente da el reporte en USD (puedes comprobarlo en la pestaña de “Resumen de la cuenta”)
df_taxdown_closed_positions.currency = 'USD'

### CUSTOMIZED PROCESSING BASED ON TRANSACTION TYPE ###
# ES -> operationDate*: 
#   Para las compras: copia la columna de “Fecha de apertura” 
#   Para las ventas: copia la columna de “Fecha de cierre”
#   El formato de la fecha tiene que ser DD/MM/AAAA, por lo que deberás 
#   eliminar la hora, minutos y segundos. 
df_taxdown_closed_positions.operationDate = df_closed_positions.apply(lambda row: row['Fecha de apertura'] if row['tipo_transaccion'] == 'Buy' else row['Fecha de cierre'], axis = 1)

### CUSTOMIZED PROCESSING BASED ON ASSET TYPE AND TRANSACTION TYPE ###
# Find the position type based on a customized function
df_taxdown_closed_positions['type'] = df_closed_positions.apply(lambda row: find_position_type(row), axis = 1)

# ES -> unitPrice: 
#   Para las compras: copia el importe de la columna “Tasa de apertura”
#   Para las ventas: copia el importe de la “Tasa de cierre”. Excepto para los CFDs de tipo SELL: en este caso deberás copiar la "Tasa de cierre" en la compra y la "Tasa de apertura" en la venta.
df_taxdown_closed_positions.unitPrice = df_closed_positions.apply(lambda row: row['Tasa de apertura'] if row['tipo_transaccion'] == 'Buy' else row['Tasa de cierre'], axis = 1)

### CALCULATIONS ###
# ES -> netCash: multiplica las columnas del CSV TaxDown amount x unitprice y divídelo entre la columna “Apalancamiento” del archivo de Etoro.
df_taxdown_closed_positions.netCash = (df_taxdown_closed_positions['amount'] * df_taxdown_closed_positions['unitPrice']) / df_closed_positions['Apalancamiento']
# ES -> commission: es la suma de lo que aparece en la columna “diferencial” y la columna ”comisiones por renovación de posiciones”. (Inclúyelo únicamente en la fila de la venta)
df_taxdown_closed_positions.commission = df_closed_positions['Diferencial'] + df_closed_positions['Comisiones por renovación de posiciones y dividendos']

######################## DIVIDENDS ###########################
# Copy of the template for dividends
df_taxdown_closed_dividends = df_taxdown.copy()
# ES -> description: copia lo que pone en la columna “Nombre del instrumento”. 
df_taxdown_closed_dividends.description = df_dividends['Nombre del instrumento']
# ES -> isin: copia lo que pone en la columna "ISIN". 
df_taxdown_closed_dividends['isin'] = df_dividends['ISIN']
# ES -> type: DIVIDENDS (en todas) 
df_taxdown_closed_dividends.type = 'DIVIDENDS'
# ES -> amount: déjalo vacío 
df_taxdown_closed_dividends.amount = 0
# ES -> unitPrice: déjalo vacío 
df_taxdown_closed_dividends.unitPrice = 0
# ES -> netCash: copia lo que pone en la columna “Dividendo neto recibido” 
df_taxdown_closed_dividends.netCash = df_dividends['Dividendo neto recibido (USD)']
# ES -> commission: déjalo vacío
df_taxdown_closed_dividends.commission = 0
# ES -> retention: importe de la columna “Importe de la retención fiscal”
df_taxdown_closed_dividends.retention = df_dividends['Importe de la retención fiscal (USD)']
# ES -> retentionDetail: las dos primeras letras del ISIN o lo que es lo mismo, el país en el que hemos sufrido esa retención mediante código ISO 3166-1
df_taxdown_closed_dividends.retentionDetail = extract_iso_2_from_ISIN(df_dividends, 'ISIN')
# ES -> currency: Etoro habitualmente da el reporte en USD (puedes comprobarlo en la pestaña de “Resumen de la cuenta”)
df_taxdown_closed_dividends.currency = 'USD'
# ES -> operationDate: copia lo que pone en la columna “Fecha del pago”. El formato de la fecha tiene que ser DD/MM/AAAA, por lo que deberás eliminar la hora, minutos y segundos.
df_taxdown_closed_dividends.operationDate = pd.to_datetime(df_dividends['Fecha de pago'], format = etoro_date_columns_format).dt.strftime(taxdown_date_format)

######################## CONCATENATION OF CLOSED POSITIONS AND DIVIDENDS ###########################
# ES -> IMPORTANTE: Para terminar, debes ordenar el documento por la columna fecha en orden descendiente. Para ello selecciona la primera fila del csv de TaxDown, pincha en datos > filtro > selecciona la columna fecha > ordenar > descendiente.
# Concatenate both dataframes
df_taxdown_final = pd.concat([df_taxdown_closed_positions, df_taxdown_closed_dividends])
# Transformation opeationDate to time for descending oredering
df_taxdown_final.operationDate = pd.to_datetime(df_taxdown_final.operationDate, format = taxdown_date_format)
# opeationDate descending ordering
df_taxdown_final.sort_values(by = ['operationDate'], ascending = False, inplace = True)
# returning opeationDate format to taxdown required one
df_taxdown_final.operationDate = df_taxdown_final.operationDate.dt.strftime(taxdown_date_format)
# Add broker information
df_taxdown_final['broker'] = 'eToro'
# ES -> Excel a CSV 
#   Haz clic en Archivo> Guardar como> luego cambia el formato de archivo de Libro de Excel (.xlsx) a CSV codificado en UTF-8 (delimitado por comas) (.csv) y guárdalo.
df_taxdown_final.to_csv(taxdown_output_path, encoding = 'utf-8', decimal = ',', index = False, sep = ';')


