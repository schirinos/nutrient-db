nutrient-db
===========

Nutrient-db is a program to convert the USDA National Nutrient Database for Standard Reference (http://www.ars.usda.gov/ba/bhnrc/ndl) from the flat files they provide into a relational database and optional a collection of json documents.

Usage
-----------------

To generate an SQLite database file from the flat files included in the repo run: 

<pre><code>python nutrientdb.py</code></pre>

By default it will look in the **data/sr25** directory for the required flat files and parse them into an SQLite database file named *nutrients.db* that will be stored in the current working directory. 

If the *nutrients.db* file already exists and is a valid SQLite database with partial nutrient data in it the script will think you have already completed the parsing of the flat files and not create a new database file. To re-parse the flat files you need to pass the *-f* option to force recreation of the database file.

Command line options are available to help export the information into json format and directly to a mongo database.

### Command line options

#### Path to flat files
##### -p, --path [default: data/sr25]

The path with the flat files to be parsed are located.

<pre><code>python nutrientdb.py -p data/sr25</code></pre>


#### Force re-parse 
##### -f, --force

Force recreation of SQLite database from flat files. Use this option to re-parse the data from the flat files and create a new database file. Useful if the database gets corrupted, a previous parse failed to complete or there are changes to the flat files you want to capture in the database.

<pre><code>python nutrientdb.py -f</code></pre>

#### Export data as json
##### -e, --export 

Export the data as json by printing out each document to the console. The format of the json is a custom schema where each json document represents a unqiue food item from the food descriptions table. All other information is attached to these individual documents.

Since the program prints to standard out by defautl you can redirect the output to a file, for example:

<pre><code>python nutrientdb.py -e > nutrients.json</code></pre>

#### Export to mongo

To export the data to a mongodb you must provide the following options. Any missing options (except -mport which defaults to 27017) will result in the program not trying to export to mongo.

The program will always try an upsert based on the NDB_No of the food item. That means you can safely run the script multiple times to refresh existing info.

<pre><code>python nutrientdb.py --mhost localhost --mport 27017 --mdb mydatabase --mcoll mycollection</code></pre>

##### --mhost [default: localhost]

The hostname of the mongo instance.

##### --mport [default: 27017]

The port of the mongo instance.

##### --mdb

Name of the mongo database to connect to.

##### --mcoll

Name of the collection to insert the documents into.


Notes on Data
-----------------

The **data** directory stores the flat files to be parsed in subfolders for each full release of the USDA data. If you want to parse a different data set you can add it under a subfolder in this directory and specify the path to the files as a command line option. The program looks for a specific set of files as defined by the USDA schema. If any of these files are incorrectly named or missing parsing will fail. 

The schema between releases may change. The program is designed for sr25. Modifications may be needed to the program for reading previous release schemas or ones in the future.

USDA National Nutrient Database for Standard Reference (http://www.ars.usda.gov/ba/bhnrc/ndl)

nutrients.db
-----------------

This file in the repo is a fully parsed SQLite database of USDA sr25 data in the file *nutrients.db*. The file is about 50MB.
