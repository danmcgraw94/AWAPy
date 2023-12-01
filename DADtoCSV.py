import sys
import csv
import arcpy
import pandas as pd
import numpy as np
import re
import datetime

# This code was developed to process all DAD tables in AWA PMP tools that had available
# DAD tables within a DAD

# NO Tail update removes trailing storm type on SPAS_IDs
# This provides checking and/or work around to writing to the shapefiles

##This will update the 1. Storm Listing - DAD_Values folders, nothing else
##Existing info saved in superseded folders (ss)

def process_dad_tables(workspace_path):
    # Set the workspace to your Geodatabase where the tables are located
    arcpy.env.workspace = workspace_path

    # Get all DAD tables in gdb
    dad_tables = arcpy.ListTables()

    df_dad = []
    df_h6sqmi1 = []
    dh_h6sqmi10 = []
    dh_h6sqmi100 = []

    df_h24sqmi1 = []
    df_h24sqmi10 = []
    df_h24sqmi100 = []

    for dad in dad_tables:
        areas = []
        h6s = []
        h24s = []
        # Create a full path to the table
        dad_path = arcpy.env.workspace + "\\" + dad
        
        # Check to see if table exists
        if arcpy.Exists(dad_path):
            # Use search cursor to extract 10 and 100 sq mi 24hr values from any of the existing tables
            rows = arcpy.da.SearchCursor(dad_path, ['AREASQMI', 'H_06', 'H_24'])
            for row in rows:
                try:
                    area = float(row[0])
                    h6 = float(row[1])
                    h24 = float(row[2])

                    areas.append(area)
                    h6s.append(h6)
                    h24s.append(h24)
                except (ValueError, TypeError):
                    # Handle the exception if conversion fails (e.g., non-numeric or NULL values)
                    areas.append(-9999)
                    h6s.append(-9999)
                    h24s.append(-9999)

        else:
            print("Table not found: " + dad_path)

        del row, rows

        # Get index for 1,10,and 100 sq mi in table, may be different for each DAD
        index1 = next((index for index, value in enumerate(areas) if value == 1), None)
        index10 = next((index for index, value in enumerate(areas) if value == 10), None)
        index100 = next((index for index, value in enumerate(areas) if value == 100), None)

        # 1 sq mi
        if index1 is not None:
            area1sq = areas[index1]
            h6sq1 = h6s[index1]
            h24sq1 = h24s[index1]
        else:
            area1sq = -9999
            h6sq1 = -9999
            h24sq1 = -9999

        # 10 sq mi
        if index10 is not None:
            area10sq = areas[index10]
            h6sq10 = h6s[index10]
            h24sq10 = h24s[index10]
        else:
            area10sq = -9999
            h6sq10 = -9999
            h24sq10 = -9999

        # 100 sq mi
        if index100 is not None:
            area100sq = areas[index100]
            h6sq100 = h6s[index100]
            h24sq100 = h24s[index100]
        else:
            area100sq = -9999
            h6sq100 = -9999
            h24sq100 = -9999
        
        df_dad.append(dad)
        
        df_h6sqmi1.append(h6sq1)
        dh_h6sqmi10.append(h6sq10)
        dh_h6sqmi100.append(h6sq100)

        df_h24sqmi1.append(h24sq1)
        df_h24sqmi10.append(h24sq10)
        df_h24sqmi100.append(h24sq100)

    data = []
    for i in range(len(df_dad)):
        data.append({
            "SPAS_ID": df_dad[i],
            "H6SqMi1":df_h6sqmi1[i],
            "H6SqMi10":dh_h6sqmi10[i],
            "H6SqMi100":dh_h6sqmi100[i],
            "H24SqMi1": df_h24sqmi1[i],
            "H24SqMi10": df_h24sqmi10[i],
            "H24SqMi100": df_h24sqmi100[i]
        })

    df = pd.DataFrame(data)
    return df
    # Export DF to a CSV file
    #df.to_csv(output_csv_path, index=False)
    #print "Exported CSV to",output_csv_path
    
def clean_spas_id(spas_id):
    match = re.search(r'SPAS_(\d+_\d+)_', spas_id)
    if match:
        return "SPAS_" + match.group(1)
    else:
        return spas_id

def uniqueSPAS(df):
    newdf = df.copy(deep=True)
    # Find duplicates based on 'SPAS_ID' and keep the row with the higher 'H6SqMi10' value
    newdf = newdf.sort_values(by='H6SqMi10', ascending=False).drop_duplicates(subset='SPAS_ID', keep='first')
    return newdf

# Set DAD directories
spasdir = r"D:\0.RMC\NAS PMP\SPAS"
azdad_dir = spasdir + r"\AZ-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"
conmdad_dir = spasdir + r"\CO-NM-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"
nddad_dir = spasdir + r"\ND-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"
okdad_dir = spasdir + r"\OK-AR-LA-MS-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"
padad_dir = spasdir + r"\PA-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"
txdad_dir = spasdir + r"\TX-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"
vadad_dir = spasdir + r"\VA-PMP\pmp-eval-tool\Input\DAD_Tables.gdb"
wydad_dir = spasdir + r"\WY-PMP\WY_VII_PMP-EvalTool\PMP_Evaluation_Tool\Input\DAD_Tables.gdb"

az_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\AZ\AZ_SPAS_DADvalues.csv'
conm_csv =  r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\CONM\CONM_SPAS_DADvalues.csv'
nd_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\ND\ND_SPAS_DADvalues.csv'
okarlams_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\OK-AR-LA-MS\OK_SPAS_DADvalues.csv'
pa_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\PA\PA_SPAS_DADvalues.csv'
tx_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\TX\TX_SPAS_DADvalues.csv'
va_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\VA\VA_SPAS_DADvalues.csv'
wy_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\WY\WY_SPAS_DADvalues.csv'

# Process DADs
dadtime1 = datetime.datetime.now()
print "AZ"
azdads = process_dad_tables(azdad_dir)
print "CO-NM"
conmdads = process_dad_tables(conmdad_dir)
print "ND"
nddads = process_dad_tables(nddad_dir)
print "OK-AR-LA-MS"
okdads = process_dad_tables(okdad_dir)
print "PA"
padads = process_dad_tables(padad_dir)
print "TX"
txdads = process_dad_tables(txdad_dir)
print "VA"
vadads = process_dad_tables(vadad_dir)
print "WY"
wydads = process_dad_tables(wydad_dir)
dadtime2 = datetime.datetime.now()

processtime = dadtime2 - dadtime1
print 'All DAD processing = ',processtime.total_seconds(),'seconds'

# Remove trailing storm types
azdads['SPAS_ID'] = azdads['SPAS_ID'].apply(clean_spas_id)
conmdads['SPAS_ID'] = conmdads['SPAS_ID'].apply(clean_spas_id)
nddads['SPAS_ID'] = nddads['SPAS_ID'].apply(clean_spas_id)
okdads['SPAS_ID'] = okdads['SPAS_ID'].apply(clean_spas_id)
padads['SPAS_ID'] = padads['SPAS_ID'].apply(clean_spas_id)
txdads['SPAS_ID'] = txdads['SPAS_ID'].apply(clean_spas_id)
vadads['SPAS_ID'] = vadads['SPAS_ID'].apply(clean_spas_id)
wydads['SPAS_ID'] = wydads['SPAS_ID'].apply(clean_spas_id)

# Now filter down to unique storms (this returns a new df made from a deep copy)
fazdads = uniqueSPAS(azdads)
fconmdads = uniqueSPAS(conmdads)
fnddads = uniqueSPAS(nddads)
fokdads = uniqueSPAS(okdads)
fpadads = uniqueSPAS(padads)
ftxdads = uniqueSPAS(txdads)
fvadads = uniqueSPAS(vadads)
fwydads = uniqueSPAS(wydads)

fazdads.to_csv(az_csv, index=False)
fconmdads.to_csv(conm_csv, index=False)
fnddads.to_csv(nd_csv, index=False)
fokdads.to_csv(okarlams_csv, index=False)
fpadads.to_csv(pa_csv, index=False)
ftxdads.to_csv(tx_csv, index=False)
fvadads.to_csv(va_csv, index=False)
fwydads.to_csv(wy_csv, index=False)
