# Automatic Extract, Transform, Load Pipeline and Dashboard

## Abstract

## Processes: All of the following processes are automatically performed on a daily basis 
Data Ingestion
- Utilized RescueGroups' Pet Adoption Data via API Call
- Downloads files from RescueGroups server via File Transfer Protocol
- Automatically searches for file updates on a daily basis
- Stores .json files as zipped files on personal server to save storage

<br>Data Cleaning
- Processes zipped .json files into Pandas Dataframe
- Performs row-wise removal of invalid entries
- Converts every feature into proper data types, optimizing storage space 
- Generates data schema to store as metadata within database
- Creates lookup tables to optimize storage via foreign keys
  
<br>Initial Data Load
- Connects to sql server via engine and creates database
- Uploads database alongside database schema and lookup tables
  
<br>Continual Data Load (Automatic)
- Connects to sql server via engine and retrieves database metadata
- Ensures format of new data matches database metadata
- Loads data into database via Upsert statement

## Dashboard
The interactive pet dashboard showcases data on pets such as:
-  Total amount of pets
-  Daily pets listed
-  Weekly pets listed
-  Most common Pets
-  Most common pet breeds
-  Age of pets
-  Location of pets
<br> Additionally, the interactive pet dashboard allows for data filtering based on pet species.
