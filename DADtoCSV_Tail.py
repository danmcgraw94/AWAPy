import sys
import arcpy
import pandas as pd
import numpy as np

# This code was developed to process all DAD tables in AWA PMP tools that had available
# DAD tables within a DAD

# This code is now outdated - the tailing storm types (ex: _loc or _gen) cause issues in the code that was used to add seleced
# DAD values to corresponding storm shape files

# Some expiramentation exists at the bottom of this code for Nebraska and Ohio
# Developed Sept-2023 by Dan McGraw during RMC developmental detail

def process_dad_tables(workspace_path, output_csv_path):
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

    # Export DF to a CSV file
    df.to_csv(output_csv_path, index=False)
    print "Exported CSV to",output_csv_path


# Specify the workspace path and output CSV path
az_workspace = r'D:\0.RMC\NAS PMP\SPAS\AZ-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'
conm_workspace = r'D:\0.RMC\NAS PMP\SPAS\CO-NM-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'
nd_workspace = r'D:\0.RMC\NAS PMP\SPAS\ND-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'
#ne_workspace
#oh_workspace
okarlams_workspace = r'D:\0.RMC\NAS PMP\SPAS\OK-AR-LA-MS-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'
pa_workspace = r'D:\0.RMC\NAS PMP\SPAS\PA-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'
tx_workspace = r'D:\0.RMC\NAS PMP\SPAS\TX-PMP\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'
va_workspace = r'D:\0.RMC\NAS PMP\SPAS\VA-PMP\pmp-eval-tool\Input\DAD_Tables.gdb'
wy_workspace = r'D:\0.RMC\NAS PMP\SPAS\WY-PMP\WY_VII_PMP-EvalTool\PMP_Evaluation_Tool\Input\DAD_Tables.gdb'

az_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\AZ\AZ_SPAS_DADvalues.csv'
conm_csv =  r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\CONM\CONM_SPAS_DADvalues.csv'
nd_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\ND\ND_SPAS_DADvalues.csv'


ne_csv1 = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\NE\NE_SPAS_DADvalues1.csv'
ne_csv2 = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\NE\NE_SPAS_DADvalues2.csv'
ne_csv3 = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\NE\NE_SPAS_DADvalues3.csv'

oh_csv1 = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\OH\OH_SPAS_DADvalues1.csv'
oh_csv2 = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\OH\OH_SPAS_DADvalues2.csv'
oh_csv3 = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\OH\OH_SPAS_DADvalues3.csv'

okarlams_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\OK-AR-LA-MS\OK_SPAS_DADvalues.csv'
pa_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\PA\PA_SPAS_DADvalues.csv'
tx_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\TX\TX_SPAS_DADvalues.csv'
va_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\VA\VA_SPAS_DADvalues.csv'
wy_csv = r'D:\0.RMC\NAS PMP\SPAS\1.StormListing\DAD_Values\WY\WY_SPAS_DADvalues.csv'


# Call the function to process DAD tables
# Find which OH and NE storms can be obtained from other GBD - best option may be to output several CSVs
##process_dad_tables(az_workspace, az_csv)
##process_dad_tables(conm_workspace, conm_csv)
##process_dad_tables(nd_workspace, nd_csv)
##process_dad_tables(okarlams_workspace, okarlams_csv)
##process_dad_tables(pa_workspace, pa_csv)
##process_dad_tables(tx_workspace, tx_csv)
##process_dad_tables(va_workspace, va_csv)
##process_dad_tables(wy_workspace, wy_csv)

