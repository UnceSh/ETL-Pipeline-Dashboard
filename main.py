# Imports
import pandas as pd
import numpy as np
from ftplib import FTP # For file transfer
import os # To obtain file directory and paths 
import zipfile # To read the zipped data
import json # To parse file data
from sqlalchemy import create_engine, text, MetaData, Table # SQL engine
from sqlalchemy.dialects.mysql import MEDIUMTEXT, insert # Long text storage

# Pipeline Part 1: Data ingestion (identify sources of data and create processes to collect it)

def ftp_connect(hostname, username, password):
    """
    Establishes a connection to FTP account using account information.
    Args:
        hostname (str): hostname for FTP account
        username (str): username for FTP account
        password (str): password for FTP account
    Returns:
        ftp (ftp): ftp account
    """
    ftp = FTP(hostname)
    ftp.login(username, password)
    ftp.set_pasv(True)
    return(ftp)

def download_file(ftp, remote_file_path, local_file_path): 
    """
    Downloads / writes file in raw binary from specified remote path to specified local path.
    Args:
        ftp (ftp): FTP variable for signed into FTP account
        remote_file_path (str): File path for the ftp server.
        local_file_path (str): File path for the local folder to write file to.    
    Returns:
        None
    """
    with open(local_file_path, "wb") as file:
        ftp.retrbinary("RETR " + remote_file_path, file.write)
    print("File: " + local_file_path + " downloaded successfully.")

def update_files(hostname, username, password, folder_path):
    """
    Connects to FTP account, then checks for any new files and downloads them, and checks for any updated files 
    and downloads them.
    Args:
        hostname (str): hostname of the FTP account
        username (str): username used to login to the FTP account
        password (str): password used to login to the FTP account
        folder_path (str): string literal defining the path of where the downloaded files should go
    Returns:
        None
    """
    # Establish FTP connection
    ftp = ftp_connect(hostname, username, password)

    # Obtain file lists
    fileListr = ftp.nlst() # Remote server files
    fileListl = os.listdir(path=folder_path) # Local server files
    print(fileListl)

    # Checks if file is already present locally, and if not then downloads (downloads new files)
    for file in fileListr:
        remote_file_path = file
        local_file_path = os.path.join(folder_path, file)
        # Downloads daily update files
        if file.startswith("newpets") or file.startswith("updatedpets"):
            download_file(ftp, remote_file_path, local_file_path)
        # Checks for new files    
        elif file in fileListl:
            print("File: " + file + " already exists.")
        else:
            download_file(ftp, remote_file_path, local_file_path)

    # Logs out of ftp account when done
    ftp.quit()


# Pipline Part 2: Convert data into usable format (zipped .json --> dataframe) and establish updating capabilities

def zipReader(folder_path):
    """
    Takes a folder_path as input, where this path leads to a zip folder. The function accesses the files inside
    of this folder and writes it to a list and returns the list. The files are assumed to be .json.
    Args:
        folder_path (str): string literal for folder path of zip folder
    Returns:
        list: list of entries stored within the .json files in the zip folder
    """
    
    data = []  
    # Opens zip folder
    with zipfile.ZipFile(folder_path, 'r') as zip:
        # Loops for each file in the folder (only one file expected per folder for personal use case)
        for filename in zip.namelist():
            # Opens file, expected to be .json, stores each line of file into list
            with zip.open(filename) as file:
                for line in file:
                    row = json.loads(line.decode('utf-8'))
                    data.append(row)
            print(filename, "has been extracted.")
    return(data)

def initialData(folder_path):
    """
    Creates a df of the entire intiial data represented in the pets_1 - pets_7 files.
    Args: 
        folder_path (str): string literal representing the path of a folder holding all the zipped files
    Returns:
        DataFrame: dataframe of the entire data stored within these .json files
    """

    fileList = os.listdir(path=folder_path) 
    data = []
    # Loops over all files
    for file in fileList:
        # Checks if its pets_1-7
        if file.startswith("pets_"):
            # Appends data from file to our list
            data.extend(zipReader(os.path.join(folder_path, file)))
    return(pd.DataFrame(data))

def updateData(folder_path):
    """
    Creates a df of the data that needs to be updated, represented by the updatedpets file.
    Args: 
        folder_path (str): string literal representing the path of a folder holding the zipped files
    Returns:
        DataFrame: dataframe of the data needing updates stored within these .json files
    """

    fileList = os.listdir(path=folder_path) 
    data = []
    # Loops over all files
    for file in fileList:
        # Checks if its the updated data
        if file.startswith("updated") or file.startswith("new"):
            # Appends data from file to our list
            data.extend(zipReader(os.path.join(folder_path, file)))
    return(pd.DataFrame(data))

def newData(folder_path):
    """
    Creates a df of the new data, represented by the newpets file.
    Args: 
        folder_path (str): string literal representing the path of a folder holding the zipped files
    Returns:
        DataFrame: dataframe of the new data stored within these .json files
    """

    fileList = os.listdir(path=folder_path) 
    data = []
    # Loops over all files
    for file in fileList:
        # Checks if its the new data
        if file.startswith("new"):
            # Appends data from file to our list
            data.extend(zipReader(os.path.join(folder_path, file)))
    return(pd.DataFrame(data))

# Pipeline Part 3: Prepare and clean the data

# Drop invalid entrees (images stored as entrees)
def dropInvalid(df):
    '''
    Drops invalid entries from dataframe. Specifically uses the animalID column for this task.
    Args:
        df (DataFrame): df where NaN values are to be dropped
    Returns:
        df (DataFrame): df where NaN values have been dropped
    '''
    df.iloc[:, 1] = pd.to_numeric(df.iloc[:, 1], errors='coerce') # converts incorrect entries into NaN
    df = df.dropna(subset=['animalID']) # drops the NaN values
    print("NaN values have been successfully dropped.")
    return(df)

def dtypeConv(df):
    """
    Converts the data types of inputted dataframe from object to proper fields using manually determined dtypes.
    Args:
        df (DataFrame): dataframe whose types are being converted
    Returns:
        DataFrame: dataframe wtih correct data types
    """
    # Mapping scheme for booleans
    boolMap = {'Yes': True, 'No': False, '': pd.NA, None: pd.NA}

    # Manual column datatype conversion
    df[df.columns[0:2]] = df.iloc[:, 0:2].astype(int)
    df[df.columns[2]] = df.iloc[:, 2].astype('category')
    df[df.columns[3]] = df.iloc[:, 3].astype(int) # epoch
    df[df.columns[4:7]] = df.iloc[:, 4:7].astype('string')
    df[df.columns[7:12]] = df.iloc[:, 7:12].astype('category')
    df[df.columns[12:18]] = df.iloc[:, 12:18].apply(lambda col: col.map(boolMap).astype('boolean'))
    df[df.columns[18]] = df.iloc[:, 18].astype('category')
    df[df.columns[19]] = pd.to_datetime(df.iloc[:, 19]).dt.normalize() # date column
    df[df.columns[20:22]] = df.iloc[:, 20:22].apply(lambda col: col.map(boolMap).astype('boolean'))
    df[df.columns[22]] = df.iloc[:, 22].astype('category')
    df[df.columns[23:25]] = df.iloc[:, 23:25].replace('', np.nan).astype(float) 
    df[df.columns[25]] = df.iloc[:, 25].astype('category')
    df[df.columns[26]] = df.iloc[:, 26].map(boolMap).astype('boolean')
    df[df.columns[27:30]] = df.iloc[:, 27:30].astype('category')
    df[df.columns[30:32]] = df.iloc[:, 30:32].apply(lambda col: col.map(boolMap).astype('boolean'))
    df[df.columns[32]] = pd.to_datetime(df.iloc[:, 32]).dt.normalize()
    df[df.columns[33:34]] = df.iloc[:, 33:34].astype('category')
    df[df.columns[34]] = df.iloc[:, 34].astype('string')
    df[df.columns[35]] = pd.to_numeric(df.iloc[:, 35], errors='coerce')  # convert to float/int, then datetime
    df[df.columns[35]] = pd.to_datetime(df.iloc[:, 35], unit='s').dt.normalize()
    df[df.columns[36]] = df.iloc[:, 36].astype('category')
    df[df.columns[37:40]] = df.iloc[:, 37:40].astype('string') # 37 is html code, 39 is a link
    df[df.columns[40]] = df.iloc[:, 40].str.extract(r'(\d+\.?\d*)').astype(float) # extracts int/floats only
    df[df.columns[41:56]] = df.iloc[:, 41:56].astype('category')
    df[df.columns[56:94]] = df.iloc[:, 56:94].apply(lambda col: col.map(boolMap).astype('boolean'))
    df[df.columns[94:97]] = df.iloc[:, 94:97].astype('string')
    df[df.columns[97]] = df.iloc[:, 97].astype(int) # epoch
    df[df.columns[98:103]] = df.iloc[:, 98:103].astype('string')
    df[df.columns[103:105]] = df.iloc[:, 103:105].apply(lambda col: col.map(boolMap).astype('boolean'))
    
    df = df.replace({np.nan: None}) # turns NaNs to None
    for col in df.select_dtypes(include=['float']).columns: # turns None in floats into 0
        df[col] = df[col].replace(to_replace=[None, np.nan], value=0) 

    print("Data types have successfully been converted.")
    return df

def schemaConv(df):
    '''
    Converts the schema of category columns into foreign keys.
    Args:
        df (DataFrame): df with columns to be converted
    Returns:
        df (DataFrame): a dataframe with the converted columns
        LookupTables (dictionary): a dictionary of lookup tables for each column
    '''

    # Stores category columns
    catList = [col for col in df.select_dtypes(include='category')]
    
    # Create lookup table
    lookupTables = {}
    for col in catList:
        uniqueVals = sorted(df[col].cat.categories)
        # Filters for >5 values
        if (len(uniqueVals)>5):
            lookup_df = pd.DataFrame({
                'id': range(1, len(uniqueVals)+1),
                'value': uniqueVals
            })
            lookupTables[col] = lookup_df

    # Convert df columns into foreign key 
    for col, lookup_df in lookupTables.items():
        # Create mapping and replace
        mapping = dict(zip(lookup_df['value'], lookup_df['id']))
        df[col] = df[col].map(mapping)
        df[col] = df[col].astype("Int32")
    
    
    print("Dataframe Schema successfully converted.")
    # Save as .json
    serializable_lookup = {col: lookupTables[col].to_dict(orient='records') for col in lookupTables}
    with open("lookupTables.json", "w") as f:
        json.dump(serializable_lookup, f)

    # Return df and lookuptable
    return(df, lookupTables)

def schemaConv2(df):
    '''
    Converts the schema of category columns into foreign keys.
    Args:
        df (DataFrame): df with columns to be converted
    Returns:
        df (DataFrame): a dataframe with the converted columns
    '''

    # Opens .json with lookuptables
    with open("lookupTables.json", "r") as f:
        loaded = json.load(f)

        # Convert back to DataFrame
        lookupTables = {col: pd.DataFrame(loaded[col]) for col in loaded}
    
    # Convert schema
    catList = [col for col in df.select_dtypes(include='category')]
    for col in catList:
        if col in lookupTables:
            mapping = dict(zip(lookupTables[col]['value'], lookupTables[col]['id']))
            df[col] = df[col].map(mapping)
    df = df.replace({np.nan: None})
    print("Schema successfully converted.")

    # Return df
    return(df)



# Pipeline Part 4: Store the processed data into SQL database

def inConnection(host, username, password, database, df, table):
    '''
    Establishes a connection to SQL DB using credentials and turns df into sql table. Replaces original
    table. Only use if implementing table from scratch.
    Args:
        host (str): host key for connection
        username (str): username to sign in for connection
        password (str): password to sign in for connection
        database (str): the database to create the table in
        df (DataFrame): dataframe that gets exported into sql table
        table (str): name of the table
    Returns:
        None 
    '''

    # Creates engine
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")

    # Detects and converts long text column dtype
    dtype_map = {}
    if "description" in df.columns:
        dtype_map["description"] = MEDIUMTEXT()
    if "pictures" in df.columns:
        dtype_map["pictures"] = MEDIUMTEXT()

    df.to_sql(
        name=table.lower(),
        con=engine,
        if_exists='replace',
        index=False,
        dtype=dtype_map
    )

    # Adds primary key to pets table
    if table == ('pets'):
        with engine.connect() as conn:
            conn.execute(text("""
                              ALTER TABLE pets
                              ADD PRIMARY KEY (animalID);
                              """))
    if table == ('new_pets'):
        with engine.connect() as conn:
            conn.execute(text("""
                              ALTER TABLE new_pets
                              ADD PRIMARY KEY (animalID);
                              """))
    

    print(f"{table} has been successfully loaded into SQL DB.")


def uploadLookupTables(host, username, password, database, lookupDict):
    '''
    Uploads every key value pair (name : df) inside of a dictionary
    Args:
        host (str): host key for connection
        username (str): username to sign in for connection
        password (str): password to sign in for connection
        database (str): the database to create the table in
        lookupDict (dictionary): dictionary holding all the tables to be uploaded
    Returns:
        None 
    '''
    for col, lookup_df in lookupDict.items():
        inConnection(host, username, password, database, lookup_df, col)

def upConnection(host, username, password, database, df, tableName):
    '''
    Establishes a connection to SQL DB and then upserts data into the specified table.
    Args:
        host (str): host key for connection
        username (str): username to sign in for connection
        password (str): password to sign in for connection
        database (str): the database to create the table in
        df (DataFrame): dataframe that gets exported into sql table
        table (str): name of the table
    Returns:
        None 
    '''

    # Creates engine
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
    metadata = MetaData()
    
    # Reflect table
    table = Table(tableName, metadata, autoload_with=engine)

    # Upsert statements 
    records = df.to_dict("records")
    '''stmt = insert(table).values(records)
    update_dict = {
        col.name: stmt.inserted[col.name]
        for col in table.columns
        if col.name not in [pk.name for pk in table.primary_key.columns]
    }
    upsert_stmt = stmt.on_duplicate_key_update(**update_dict)

    # Execute
    with engine.begin() as conn:
        conn.execute(upsert_stmt)'''
    
    from sqlalchemy.dialects.mysql import insert

    pk_cols = [pk.name for pk in table.primary_key.columns]

    with engine.begin() as conn:
        for record in records:  # single-row insert
            stmt = insert(table).values(record)
            update_dict = {
                col.name: stmt.inserted[col.name] 
                for col in table.columns if col.name not in pk_cols
            }
            conn.execute(stmt.on_duplicate_key_update(**update_dict))

    
    print(f"{tableName} has been successfully updated.")




# Personal Data
# FTP Account Data
ftpHostname = ""
ftpUsername = ""
ftpPassword = ""
ftpFolder_path = r'' # Folder we want to download files into

# Database Account Data
dbHost = ""
dbUsername = ""
dbPassword = ""
database = "petadoption"

def singleUseProcesses():
    '''
    Runs all of the single-time processes.
    Args:
        None
    Returns:
        None
    '''

    # Single-time processes
    update_files(ftpHostname, ftpUsername, ftpPassword, ftpFolder_path) # downloads files
    inData = initialData(ftpFolder_path) # processes initial files into a single dataframe
    inData = dropInvalid(inData) # drops invalid entries
    inData = dtypeConv(inData) # converts dtypes
    inData, lookupTables = schemaConv(inData) # converts schema
    inConnection(dbHost, dbUsername, dbPassword, database, inData, 'pets') # uploads initial pet df
    uploadLookupTables(dbHost, dbUsername, dbPassword, database, lookupTables) # uploads lookup tables

    nData = newData(ftpFolder_path) # processes the new files into a single dataframe
    nData = dropInvalid(nData) # drops invalid entries
    nData = dtypeConv(nData) # converts dtypes
    nData = schemaConv2(nData) # converts schema
    inConnection(dbHost, dbUsername, dbPassword, database, nData, 'new_pets') # uploads new pet df

def automaticProcesses():
    '''
    Runs all of the updating processes.
    Args:
        None
    Returns:
        None
    '''

    # Automatic processes
    update_files(ftpHostname, ftpUsername, ftpPassword, ftpFolder_path) # downloads files
    upData = updateData(ftpFolder_path) # processes the update files into a single dataframe
    upData = dropInvalid(upData) # drops invalid entries
    upData = dtypeConv(upData) # converts dtypes
    upData = schemaConv2(upData) # converts schema
    upConnection(dbHost, dbUsername, dbPassword, database, upData, 'pets') # uploads updated pet df

    nData = newData(ftpFolder_path) # processes the new files into a single dataframe
    nData = dropInvalid(nData) # drops invalid entries
    nData = dtypeConv(nData) # converts dtypes
    nData = schemaConv2(nData) # converts schema
    upConnection(dbHost, dbUsername, dbPassword, database, nData, 'new_pets') # uploads new pet df

# Runs the single time processes
# singleUseProcesses()

# Runs the update processes
automaticProcesses()
