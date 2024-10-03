import pandas as pd
import datetime
import asyncio
import rapidfuzz as fuzz
import time
import json

COLUMNS_TO_TRIM = ["REGISTRATION NUMBER","CFN","STATUS"] 
DATE_COLUMNS = ["APPROVAL DATE", "EXPIRATION DATE"]
EMPTY_dATE_MESSAGE = "No date on database" 
FILE_EXTENSION = ".xlsx"
FOLFER_DOCUMENTS = "documents/"
NO_DATE_MESSAGE = "no date in database"
COLUMNS_ORDER = ["Country","CFN","CFN DESCRIPTION","OU","REGISTRATION NUMBER","APPROVAL DATE",
                 "EXPIRATION DATE","STATUS","RISK CLASSIFICATION","REGISTRATION NAME","LICENSE HOLDER",
                 "MANUFACTURING SITE"]


def cut_values(row, column='MANUFACTURING ADDRESS', sep='\n'):
    """Extracts and trims separated values from a specified column in a DataFrame row."""
    # Convert the column value to a string and split it by the separator
    values = str(row[column]).split(sep)
    # Strip whitespace from each part and return the list if there's a split, else the original value
    return [value.strip() for value in values] if len(values) > 1 else row[column]


def paste_problem(row,name ='CUT ADDRESS',address =  'CUT ADDRESS'):
    a,b = row[name],row[address]
    if type(a) is list:
        junto = "\n\n".join([f"{nombre} {dir}" for nombre,dir in zip(a,b)])
    else:
        junto = f"{a} {b}"
    return junto


def normalizeInformation(df:pd.DataFrame) -> pd.DataFrame:
    data: pd.DataFrame = df.copy()
    data.dropna(subset=["CFN"],inplace=True)
    print("deleting whitespaces on registration number and cfn")
    data["STATUS"] = data["STATUS"].str.capitalize()
    data["OU"] = data["OU"].str.replace(" ","")
    data["OU"] = data["OU"].str.upper()
    for col in COLUMNS_TO_TRIM:
        data[col] = data[col].str.strip()
    data = data.fillna("No reported on databases")
    for col in DATE_COLUMNS:
        data[col] = pd.to_datetime(data[col],errors="coerce").fillna(NO_DATE_MESSAGE)
    return data


def manage_dates(date:str):
    try:
        separated_date = date.split("-")
        date_format = datetime.datetime(year = int(separated_date[2]),
                                        month = int(separated_date[1]),
                                        day = int(separated_date[0]))
        return date_format
    except:
        return "no date"


def filter_by_ou(df:pd.DataFrame):
    data = df.copy()
    valid_options = list(data["OU"].unique())
    print(f"kep in mind this list of valdi options {valid_options}")
    try:
        ous = input("liste las OUs a buscar en las bases de datos separadas por punto y coma (;): ").upper()
        ous = [ou.strip() for ou in ous.split(";") if ou.strip() in valid_options]
        print(f"las ous validas para la busqueda son {ous} otras opciones que no se encuentren en esta lsita no serán consideradas")
        filtered = data.loc[data["OU"].isin(ous)]
        filtered = filtered[COLUMNS_ORDER]
        print("exito filtrando por ou")
        not_cfn = filtered.copy()
        not_cfn = not_cfn.drop("CFN",axis = 1)
        not_cfn = not_cfn.drop_duplicates(subset=["REGISTRATION NUMBER"])
        return filtered,not_cfn
    except Exception as e:
        print("error en la funccion filter_by_ou")
        print(e)
        return


def filter_by_cfn(df:pd.DataFrame):
    doc_name = input("ingrese el nombre del archivo excel (sin extension): ")
    full_name = f"{FOLFER_DOCUMENTS}{doc_name}{FILE_EXTENSION}"
    try:
        data:pd.DataFrame = df.copy()
        cfn_dataframe = pd.read_excel(full_name)
        cfns = [str(cfn).strip() for cfn in cfn_dataframe["CFN"].unique()]
        filtered = data.loc[data["CFN"].isin(cfns)]
        filtered = filtered[COLUMNS_ORDER]
        print("exito filtrando por cfns")
        not_cfn = filtered.copy()
        not_cfn = not_cfn.drop("CFN",axis = 1)
        not_cfn = not_cfn.drop_duplicates(subset=["REGISTRATION NUMBER"])
        return filtered,not_cfn
    except Exception as e:
        print("error en la función filter by cfn")
        print(e)


def filtered_by_license(df:pd.DataFrame):
    doc_name = input("ingrese el nombre del archivo excel (sin extension): ")
    full_name = f"{FOLFER_DOCUMENTS}{doc_name}{FILE_EXTENSION}"
    try:
        data = df.copy()
        licenses_df = pd.read_excel(full_name)
        licenses = [str(license_number).strip() for license_number in licenses_df["REGISTRATION"].unique()]
        filtered = data.loc[data["REGISTRATION NUMBER"].isin(licenses)]
        filtered = filtered[COLUMNS_ORDER]
        not_cfn = filtered.copy()
        not_cfn = not_cfn.drop("CFN",axis = 1)
        not_cfn = not_cfn.drop_duplicates(subset=["REGISTRATION NUMBER"])
        print("exito filtrando por numero de registro")
        return filtered,not_cfn
        

    except Exception as e:
        print("error en la función filter by registration")
        print(e)


def filter_by_timelapse(df:pd.DataFrame):
    data = df.copy()
    print("in the following messages you will need to put dates in format DD-MM-YYY")
    initial_date = input("ingrese la fecha de inicio de la buqueda: ")
    end_Date = input("ingrese la fecha final de la busqueda: ")
    try:
        initial_date = manage_dates(initial_date)
        end_Date = manage_dates(end_Date)
        if "no date" in [initial_date]:
            print("error en las fechas proporcionadas por el ususario")
            return 
        no_info_date = data.loc[data["EXPIRATION DATE"] == NO_DATE_MESSAGE]
        data_first_filter = data.loc[data["EXPIRATION DATE"] != NO_DATE_MESSAGE]
        filter_timelapse = data_first_filter.loc[(data_first_filter["EXPIRATION DATE"]>=initial_date) &
                                                 (data_first_filter["EXPIRATION DATE"]<=end_Date)]
        filter_timelapse = pd.concat([filter_timelapse,no_info_date])
        filter_timelapse = filter_timelapse[COLUMNS_ORDER]
        not_cfn = filter_timelapse.copy()
        not_cfn = not_cfn.drop("CFN",axis = 1)
        not_cfn = not_cfn.drop_duplicates(subset=["REGISTRATION NUMBER"])
        return filter_timelapse,not_cfn
    
    except Exception as e:
        print("error en la función filter by timelapse")
        print(e)
        return
        

def filter_by_expired(df:pd.DataFrame):
    data = df.copy()
    today = datetime.datetime.today()
    no_info_date = data.loc[data["EXPIRATION DATE"] == NO_DATE_MESSAGE]
    data_first_filter = data.loc[data["EXPIRATION DATE"] != NO_DATE_MESSAGE]
    filter_expired = data_first_filter.loc[data_first_filter["EXPIRATION DATE"]<= today]
    filtered = pd.concat([filter_expired,no_info_date])
    filtered = filtered[COLUMNS_ORDER]
    not_cfn = filtered.copy()
    not_cfn = not_cfn.drop("CFN",axis = 1)
    not_cfn = not_cfn.drop_duplicates(subset=["REGISTRATION NUMBER"])
    return filtered,not_cfn


async def search_sufix(data:pd.DataFrame, sufix:str):
    print(f"working on {sufix}")
    filtered = data.loc[data["CFN"].str.startswith(sufix)]
    filtered["FOUND USING"] = sufix
    return filtered


async def filter_by_sufix(df:pd.DataFrame):
    doc_name = input("ingrese el nombre del archivo excel (sin extension): ")
    full_name = f"{FOLFER_DOCUMENTS}{doc_name}{FILE_EXTENSION}"
    try:
        data:pd.DataFrame = df.copy()
        cfn_dataframe = pd.read_excel(full_name)
        
        sufixes = [str(cfn).strip() for cfn in cfn_dataframe["CFN"].unique()]
        
        tasks = [search_sufix(data,sufix) for sufix in sufixes]
        responses = await asyncio.gather(*tasks)
        use_cols = COLUMNS_ORDER.copy()
        use_cols.append("FOUND USING")
        filtered = pd.DataFrame(columns = use_cols)
        for response in responses:
            filtered = pd.concat([filtered,response])
        not_cfn = filtered.copy()
        not_cfn = not_cfn.drop("CFN",axis = 1)
        not_cfn = not_cfn.drop_duplicates(subset=["REGISTRATION NUMBER"])
        return filtered,not_cfn
    except Exception as e:
        print("failed on filter by sufix")
        print(e)


def create_basic_excel(cfn_sheet:pd.DataFrame,registration_sheet:pd.DataFrame,coparation_sheet:pd.DataFrame):
    try:
        path = f"results/traker_{input('enter a name for the tracker: ')}.xlsx"
        with pd.ExcelWriter(path) as writer1:
            cfn_sheet.to_excel(writer1, sheet_name = 'CFNs', index = False)
            registration_sheet.to_excel(writer1, sheet_name = 'Registros', index = False)
            coparation_sheet.to_excel(writer1, sheet_name = 'Comparado con Submission Plan', index = False)
        print("excel file created")
        return
    except Exception as e:
        print("error creating excel file")
        print(e)
        return 
    

async def fuzz_matching(reference:str, list_to_search: list) -> dict:
    result = []
    for cfn in list_to_search:
        try:
            ratio = fuzz.fuzz.ratio(str(reference),str(cfn))
            if ratio < 80:
                continue
            result.append({
                "reference":reference,
                "found":cfn,
                "ratio": ratio
            })
        except:
            result.append({
                "reference":reference,
                "found":cfn,
                "ratio": "not possible to make the comparation"
            })


    return result


async def compare_documents():
    print("Working on comparate multiple documents")
    distribution = {"document1":{},"document2":{}}
    try:
        print("informacion para el primer documento")
        distribution["document1"] = {
            "doc_name": f'{FOLFER_DOCUMENTS}{input("nombre del primer documento: ")}{FILE_EXTENSION}',
            "column_reference": input("Ingrese el nombre de la columna a analizar: "),
            "search_doc":input("es el documento de referencia (Y/N):" )
        }
        distribution["document2"] = {
            "doc_name": f'{FOLFER_DOCUMENTS}{input("nombre del segundo documento: ")}{FILE_EXTENSION}',
            "column_reference": input("Ingrese el nombre de la columna a analizar: "),
            "search_doc": "Y" if distribution["document1"]["search_doc"]== "N" else "N"
        }
        print("Uploading documents")
        document_1 = pd.read_excel(distribution["document1"]["doc_name"])
        document_2 = pd.read_excel(distribution["document2"]["doc_name"])
        print("Documents uploaded")
        if distribution["document1"]["search_doc"] == "Y":
            cfn_to_search = list(set(document_1[distribution["document1"]["column_reference"]].dropna()))
            search_on_this_list = list(set(document_2[distribution["document2"]["column_reference"]].dropna()))
        else:
            print("tu marido es la referencia")
            cfn_to_search = list(set(document_1[distribution["document2"]["column_reference"]].dropna()))
            search_on_this_list = list(set(document_1[distribution["document2"]["column_reference"]].dropna()))
        tasks = [fuzz_matching(cfn,search_on_this_list) for cfn in cfn_to_search]
        results = await asyncio.gather(*tasks)
        final_result =[]
        for info in results:
            if len(info) == 0:
                continue
            final_result.extend(info)
        

        output = pd.DataFrame(final_result)
        output.to_excel("data_test.xlsx")
    except ValueError as e:
        print("error en busqueda especializada en dos docuemntos")
        print(e)    