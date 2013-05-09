nutrient-db
===========

Nutrient-db is a program to convert the USDA National Nutrient Database for Standard Reference from the flat files they provide into a relational database and optional a collection of json documents.

Usage
-----------------

To generate an SQLite database file from the flat files included in the repo run: 

<pre><code>python nutrient.py</code></pre>

By default it will look in the **data/sr25** directory for all the required flat files and parse them into an SQLite database file named *nutrients.db* in the current working directory. 

If the *nutrients.db* file already exists and is a valid SQLite database with some info in it the script will think you have already completed the parsing of the flat files and not create a new file. You will need to pass the *-f* option to force recreation of the database file.

Command line options are available to help export the information into json format and directly to a mongo database.

### Command line options

#### -f

Force recreation of SQLite database from flat files. Use this option to re-parse the data from the flat files and create a new database file. Useful if the database gets corrupted, a previous parse failed to complete or there are changes to the flat files you want to capture in the database.

#### -e

Export the data as json by printing out each document to the console. The format of the json is a custom schema where each json document represents a unqiue food item from the food descriptions table. All other information is attached to these individual documents.

You may pass additional arguements along with this one to make the program export each document to a mongodb collection instead.
