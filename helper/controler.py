from helper.loadData import uploadDatabases,load_SPlan,load_criticals,load_vouchers,load_external
import pandas as pd
import asyncio
import os
import helper.procesing as pr
print("preparin databases info")
df = uploadDatabases()
submission_plan = load_SPlan()

def Option1():
    print("starting search by cfn")
    cfns_list,registrations_sheet = pr.filter_by_cfn(df)
    
    sub_plan_comparation = pd.merge(submission_plan,registrations_sheet,
                                    how="inner",on="REGISTRATION NUMBER",
                                    suffixes=["_db","_sp"])
    pr.create_basic_excel(cfns_list,registrations_sheet,sub_plan_comparation)
    print("tracker successfuly completed")
    return


def Option2():
    print("starting search by subOU")
    cfns_list,registrations_sheet = pr.filter_by_ou(df)
    sub_plan_comparation = pd.merge(submission_plan,registrations_sheet,
                                    how="inner",on="REGISTRATION NUMBER",
                                    suffixes=["_db","_sp"])
    pr.create_basic_excel(cfns_list,registrations_sheet,sub_plan_comparation)
    return


def Option3():
    print("starting search by Registration Number")
    cfns_list,registrations_sheet = pr.filtered_by_license(df)
    sub_plan_comparation = pd.merge(submission_plan,registrations_sheet,
                                    how="inner",on="REGISTRATION NUMBER",
                                    suffixes=["_db","_sp"])
    pr.create_basic_excel(cfns_list,registrations_sheet,sub_plan_comparation)
    print("tracker successfuly completed")
    return

def Option7():
    print("starting tracking by timelapse")
    cfns_list,registrations_sheet = pr.filter_by_timelapse(df)
    sub_plan_comparation = pd.merge(submission_plan,registrations_sheet,
                                    how="inner",on="REGISTRATION NUMBER",
                                    suffixes=["_db","_sp"])
    pr.create_basic_excel(cfns_list,registrations_sheet,sub_plan_comparation)
    print("tracker successfuly completed")
    return

def Option8():
    print("searching for expired licenses")
    cfns_list,registrations_sheet = pr.filter_by_expired(df)
    sub_plan_comparation = pd.merge(submission_plan,registrations_sheet,
                                    how="inner",on="REGISTRATION NUMBER",
                                    suffixes=["_db","_sp"])
    pr.create_basic_excel(cfns_list,registrations_sheet,sub_plan_comparation)
    print("tracker successfuly completed")
    return

def Option12():
    print("searching for sufixes")
    cfns_list,registrations_sheet = asyncio.run(pr.filter_by_sufix(df))
    sub_plan_comparation = pd.merge(submission_plan,registrations_sheet,
                                    how="inner",on="REGISTRATION NUMBER",
                                    suffixes=["_db","_sp"])
    pr.create_basic_excel(cfns_list,registrations_sheet,sub_plan_comparation)
    print("tracker successfuly completed")
    return

def Option13():
    print("funcion experimental")
    asyncio.run(pr.compare_documents())
    pass


