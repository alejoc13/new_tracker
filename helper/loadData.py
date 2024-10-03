import pandas as pd
import warnings
import helper.procesing as pr
import asyncio
import smartsheet
import configparser
config = configparser.ConfigParser()
config.read('tracker.config')
import os

warnings.filterwarnings('ignore')

TOKEN = config.get('DEFAULT','TOKEN')

paths = pd.read_excel(r"documents/country_paths.xlsx")
def getSheets(sheet_id,SheetName,token):
    print(f'Downloading {SheetName}')
    current_dir = os.getcwd()
    path = f'{current_dir}\Documents/'
    smart = smartsheet.Smartsheet(token)
    smart.Sheets.get_sheet_as_excel(sheet_id,path,SheetName)
    print(f'{SheetName} was correctly Downloaded')
    

def getReport(report_id,reportName,token):
    print(f'Downloading {reportName}')
    current_dir = os.getcwd()
    path = f'{current_dir}\Documents/'
    smart = smartsheet.Smartsheet(token)
    smart.Reports.get_report_as_excel(report_id,path,reportName)
    print(f'{reportName} was correctly Downloaded')


async def load_country_data(country, paths):
    usecols=['REGISTRATION NUMBER', 'REGISTRATION NAME', 'STATUS', 
                 'APPROVAL DATE', 'EXPIRATION DATE', 'CFN', 
                 'CFN DESCRIPTION', 'OU', 'MANUFACTURING SITE', 'LICENSE HOLDER',
                 "RISK CLASSIFICATION"]
    db_path = list(paths.loc[paths["COUNTRY"] == country, "PATH"])[0]
    print(f"Loading {country} placed on {db_path}")

    # Aquí estamos utilizando run_in_executor para evitar bloquear el loop
    loop = asyncio.get_event_loop()

    # Llamada correcta usando una tupla para argumentos posicionales
    temporal = await loop.run_in_executor(
        None, 
        pd.read_excel, 
        db_path,              # Primer argumento: ruta del archivo
        'ACTIVE CODES',      # Segundo argumento: nombre de la hoja
    )

    # También necesitas definir los argumentos que faltan
    # Llama a pd.read_excel con sus argumentos de palabra clave
    temporal = await loop.run_in_executor(
        None, 
        pd.read_excel, 
        db_path, 
        'ACTIVE CODES',
    )
    # Manipulación del DataFrame
    temporal = temporal[usecols]
    temporal[["CFN","REGISTRATION NUMBER"]] =  temporal[["CFN","REGISTRATION NUMBER"]].astype(str)
    temporal[["APPROVAL DATE","EXPIRATION DATE"]] = temporal[["APPROVAL DATE","EXPIRATION DATE"]].apply(pd.to_datetime)
    temporal['Country'] = country.capitalize()
    print(f"{country} complete")
    return temporal 


async def load_data(countries, paths):
    tasks = [load_country_data(country, paths) for country in countries]
    results = await asyncio.gather(*tasks)
    df = pd.concat(results, ignore_index=True)
    return df


def uploadDatabases():

    countries = {
        'BOLIVIA',
        'COLOMBIA',
        'COSTA RICA',
        'ECUADOR',
        'EL SALVADOR',
        'MEXICO',
        'PERU',
        'PARAGUAY',
    }
    df = asyncio.run(load_data(countries, paths))
    countries = {
        'GUATEMALA',
        'VENEZUELA'
    }
    for country in countries:
        db_path = list(paths.loc[paths["COUNTRY"] == country,"PATH"])[0]
        print(f"Loading {country} placed on {db_path}")
        temporal =pd.read_excel(db_path,sheet_name = 'ACTIVE CODES',usecols= ['REGISTRATION NUMBER','REGISTRATION NAME','STATUS','APPROVAL DATE','EXPIRATION DATE','CFN','CFN DESCRIPTION','OU','MANUFACTURING SITE','LICENSE HOLDER'],converters={'CFN':str,'REGISTRATION NUMBER':str},
                                date_parser = ['EXPIRATION DATE','APPROVAL DATE'])
        
        temporal['Country'] = country.capitalize()
        temporal['RISK CLASSIFICATION'] = 'No disponible en BD'
        df = pd.concat([df,temporal],ignore_index=True)
        print("-----------")
   
    db_path = list(paths.loc[paths["COUNTRY"] == "URUGUAY","PATH"])[0]
    print(f"Loading Uruguay placed on {db_path}")
    temporal =pd.read_excel(db_path,sheet_name = 'ACTIVE CODES',usecols= ['REGISTRATION NUMBER','REGISTRATION NAME','STATUS','RISK CLASSIFICATION','APPROVAL DATE','EXPIRATION DATE','CFN','OU','MANUFACTURING SITE','LICENSE HOLDER'],converters={'CFN':str,'REGISTRATION NUMBER':str},
                                date_parser = ['EXPIRATION DATE','APPROVAL DATE'])
    temporal['Country'] = 'Uruguay'
    temporal['CFN DESCRIPTION'] = 'No disponible en BD'
    df = pd.concat([df,temporal],ignore_index=True)
    print("-----------")

    brand_code = {
        "BRAZIL COVIDIAN",
        "BRAZIL MDT"
        }
    df_brasil = pd.DataFrame(columns= ['Country','Registro ANVISA','Nome do Registro','Status do Registro','Classe de Risco ','Data de Aprovação Inicial','Data de Vencimento do Registro ','Código','Descrição do Código','BU','Fabricante Físico (Real)','Detentor do Registro'])
    for bc in brand_code:
        db_path = list(paths.loc[paths["COUNTRY"] == bc,"PATH"])[0]
        print(f"Loading {bc} placed on {db_path}")
        temporal =pd.read_excel(db_path,sheet_name = 'Banco de Dados',
                                usecols= ['Registro ANVISA','Nome do Registro','Status do Registro','Classe de Risco ','Data de Aprovação Inicial','Data de Vencimento do Registro ','Código','Descrição do Código','BU','Fabricante Físico (Real)','Detentor do Registro'],converters={'Código':str,'Registro ANVISA':str},
                                date_parser = ['Data de Vencimento do Registro ','Data de Aprovação Inicial'])
        temporal['Country'] = "Brazil"
        df_brasil = pd.concat([df_brasil,temporal],ignore_index=True)
        print("-----------")

    df_brasil = df_brasil.rename(columns={'Código':'CFN','BU':'OU','Registro ANVISA':'REGISTRATION NUMBER','Data de Vencimento do Registro ':'EXPIRATION DATE',
                                'Nome do Registro':'REGISTRATION NAME', 'Descrição do Código':'CFN DESCRIPTION','Status do Registro':'STATUS','Fabricante Físico (Real)':'MANUFACTURING SITE',
                                'Detentor do Registro':'LICENSE HOLDER','Data de Aprovação Inicial':'APPROVAL DATE','Classe de Risco ':'RISK CLASSIFICATION'})
    df_brasil = df_brasil[~df_brasil['STATUS'].isin(['Cancelado','OBSOLETO','obsoleto','Obsoleto','\\','Vencido'])]
    df = pd.concat([df,df_brasil],ignore_index=True)
    brand_code = {
    "ARGENTINA COVIDIAN",
    "ARGENTINA MDT"
        }
    df_AR = pd.DataFrame(columns = ['Country','REGISTRATION NUMBER','REGISTRATION NAME','STATUS','RISK CLASSIFICATION','APPROVAL DATE','EXPIRATION DATE','CFN','CFN DESCRIPTION','OU','MANUFACTURING NAME','MANUFACTURING ADDRESS','LICENSE HOLDER'] )
    for bc in brand_code:
        db_path = list(paths.loc[paths["COUNTRY"] == bc,"PATH"])[0]
        print(f"Loading {bc} placed on {db_path}")
        temporal =pd.read_excel(db_path,sheet_name = 'ACTIVE CODES',usecols= ['REGISTRATION NUMBER','REGISTRATION NAME','STATUS','RISK CLASSIFICATION','APPROVAL DATE','EXPIRATION DATE','CFN','CFN DESCRIPTION','OU','MANUFACTURING NAME','MANUFACTURING ADDRESS','LICENSE HOLDER'],
                                date_parser = ['EXPIRATION DATE','APPROVAL DATE'],converters = {'REGISTRTION NUMBER':str,'CFN':str,} )
        temporal['Country'] = 'Argentina'
        df_AR = pd.concat([df_AR,temporal],ignore_index=True)
        print("-----------")
    df_AR['CUT ADDRESS'] = df_AR.apply(pr.cut_values,axis = 1)
    df_AR['CUT NAME'] = df_AR.apply(pr.cut_values,axis = 1,column = 'MANUFACTURING NAME')
    df_AR['MANUFACTURING SITE'] = df_AR.apply(pr.paste_problem,axis=1)
    temporal = pd.DataFrame(columns=['REGISTRATION NUMBER','REGISTRATION NAME','STATUS','RISK CLASSIFICATION','APPROVAL DATE','EXPIRATION DATE','CFN','CFN DESCRIPTION','OU','Country','MANUFACTURING SITE','LICENSE HOLDER'])
    for column in temporal.columns:
        temporal[column] = df_AR[column]
    df = pd.concat([df,temporal],ignore_index=True)

    db_path = list(paths.loc[paths["COUNTRY"] == "HONDURAS","PATH"])[0]
    print(f"Loading honduras placed on {db_path}")
    honduras = pd.read_excel(db_path,sheet_name='Base de datos',converters = {'Registration number':str,'CFN':str})
    honduras = honduras.rename(columns={"Registration number":"REGISTRATION NUMBER","Product name":"REGISTRATION NAME","Approval date \n(dia-Mes-YY)":"APPROVAL DATE",
                                        "Descripción":"CFN DESCRIPTION","BU":"OU","Manufacturing site 1":"MANUFACTURING SITE","Expire date \n(dia-Mes-YY)":"EXPIRATION DATE"}
                            )
    honduras['Country'] = 'Honduras'

    honduras['STATUS'] = 'No disponible en BD'
    honduras['LICENSE HOLDER'] = 'No disponible en BD'
    honduras['RISK CLASSIFICATION'] = 'No disponible en BD'
    
    temporal = pd.DataFrame(columns=['REGISTRATION NUMBER','REGISTRATION NAME','STATUS','RISK CLASSIFICATION','APPROVAL DATE','EXPIRATION DATE','CFN','CFN DESCRIPTION','OU','Country','MANUFACTURING SITE','LICENSE HOLDER'])
    temporal[temporal.columns] = honduras[temporal.columns].copy()
    df = pd.concat([df,temporal],ignore_index=True)

    db_path = list(paths.loc[paths["COUNTRY"] == "NICARAGUA","PATH"])[0]
    print(f"Loading honduras placed on {db_path}")
    Nicaragua = pd.read_excel(db_path,sheet_name='Base de datos',converters = {'Registration number':str,'CFN':str})
    Nicaragua = Nicaragua.rename(columns={"MANUFACTURING PLANT/ADDRESS":"MANUFACTURING SITE"})
    Nicaragua['RISK CLASSIFICATION'] = 'No disponible en BD'
    Nicaragua['LICENSE HOLDER'] = 'No disponible en BD'
    Nicaragua['Country'] = 'Honduras'
    temporal[temporal.columns] = Nicaragua[temporal.columns].copy()
    df = pd.concat([df,temporal],ignore_index=True)

    db_path = list(paths.loc[paths["COUNTRY"] == "RDOM","PATH"])[0]
    print(f"Loading honduras placed on {db_path}")
    rdom = pd.read_excel(db_path,sheet_name='ACTIVE CODES',converters = {'Registration number':str,'CFN':str})
    rdom['RISK CLASSIFICATION'] = 'No disponible en BD'
    rdom['Country'] = 'Republica Dominicana'
    temporal[temporal.columns] = rdom[temporal.columns].copy()
    df = pd.concat([df,temporal],ignore_index=True)
    df = pr.normalizeInformation(df)
    return df


def load_SPlan():
    report_id = config.get('SHEET_IDS','S_PLAN')
    reportName = 'Submission Plan - Full Report.xlsx'
    getReport(report_id,reportName,TOKEN)
    print('Cargando Submission Plan...')
    df_plan = pd.read_excel('Documents\Submission Plan - Full Report.xlsx',usecols=['Id','RAS Name','Project/Product Name','Status','Submission Type','Expected Submission Date','Approval Date','Therapy Group',
                            'Expected Approval Date','Submission Date','Country','Cluster','License Number','RAC/RAN','SubOU','License Expiration Date'])
    df_plan = df_plan.rename(columns={'Project/Product Name':'PRODUCT NAME','License Number':'REGISTRATION NUMBER','License Expiration Date':'EXPIRATION DATE'})
    print('Submission Plan cargado')
    return df_plan


def load_vouchers():
    report_id = config.get('VOUCHERS','S_PLAN')
    reportName = 'Vouchers Report.xlsx'
    print('Cargando los datos de Vouchers...')
    getReport(report_id,reportName,TOKEN)
    df = pd.read_excel('Documents\Vouchers Report.xlsx',converters = {'Primary':str})
    df = df.rename(columns={'Project/Product Name':'PRODUCT NAME','Primary':'REGISTRATION NUMBER'})
    print('Vouchers Cargados')
    return df


def load_criticals():
    report_id = config.get('CRITICALS','S_PLAN')
    reportName = 'Expected Critical communications.xlsx'
    getReport(report_id,reportName,TOKEN)
    print('Cargando los datos de Expected Critical communications...')
    df = pd.read_excel('Documents\Expected Critical Communications Report.xlsx',date_parser=['License Expiration Date'],converters={'License Number':str})
    df = df.rename(columns={'PRODUCT NAME':'PRODUCT NAME','License Number':'REGISTRATION NUMBER'})
    print('Critical communications cargados')
    return df


def load_external():
    Name = input('Ingrese el nombre del documento a cruzar con el Submission plan: ')
    FileName = f'Documents/{Name}.xlsx'
    hoja = input('Ingrese el nombre de la hoja principal con la que se cruzará el Submission Plan: ')
    print(f'Cargando {FileName}')
    df = pd.read_excel(FileName,sheet_name = hoja )
    return df