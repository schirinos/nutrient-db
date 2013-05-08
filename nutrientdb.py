#!/usr/bin/python
"""Parses USDA flat files and converts them into an sqlite database"""

import sqlite3
import argparse

class NutrientDB:
	"""Parses USDA flat files and converts them into an sqlite database"""

	def __init__(self, database=None):
		"""Initializes connection to database"""

		# Set default database file if not set
		if database is None:
			self.database = sqlite3.connect('nutrients.db')
		else :
			self.database = database

		# Create table statements
		self.create_table_stmt = {}
		self.create_table_stmt["food_des"] = '''DROP TABLE IF EXISTS food_des; CREATE TABLE food_des 
									(NDB_No text, FdGrp_Cd, Long_Desc, Shrt_Desc, ComName, ManufacName, Survey, 
									Ref_desc, Refuse, SciName, N_Factor, Pro_Factor, Fat_Factor, CHO_Factor);
									CREATE UNIQUE INDEX food_des_ndb_no_idx ON food_des (NDB_No)'''

		self.create_table_stmt["fd_group"] = '''DROP TABLE IF EXISTS fd_group; CREATE TABLE fd_group (FdGrp_Cd, FdGrp_Desc);
									CREATE UNIQUE INDEX fd_group_FdGrp_Cd_idx ON fd_group (FdGrp_Cd)'''

		self.create_table_stmt["langual"] = '''DROP TABLE IF EXISTS langual; CREATE TABLE langual (NDB_No, Factor_Code);
									CREATE INDEX langual_ndb_no_idx ON langual (NDB_No)'''

		self.create_table_stmt["langdesc"] = '''DROP TABLE IF EXISTS langdesc; CREATE TABLE langdesc (Factor_Code, Description);
									CREATE INDEX langdesc_Factor_Code_idx ON langdesc (Factor_Code)'''

		self.create_table_stmt["nut_data"] = '''DROP TABLE IF EXISTS nut_data; CREATE TABLE nut_data 
									(NDB_No text, Nutr_No, Nutr_Val, Num_Data_Pts, Std_Error, Src_Cd, Deriv_Cd, Ref_NDB_No, Add_Nutr_Mark, Num_Studies,
										Min, Max, DF, Low_EB, Up_EB, Stat_cmt, AddMod_Date, CC);
									CREATE INDEX nut_data_NDB_No_idx ON nut_data (NDB_No)'''

		self.create_table_stmt["nutr_def"] = '''DROP TABLE IF EXISTS nutr_def; CREATE TABLE nutr_def 
									(Nutr_No, Units, Tagname, NutrDesc, Num_Dec, SR_Order);
									CREATE UNIQUE INDEX nutr_def_Nutr_No_idx ON nutr_def (Nutr_No)'''
									

	def insert_row(self, cursor, datatype, fields):
		"""Inserts a row of data into a specific table based on passed datatype"""

		# Generate insert parameters string 
		insert_params = "(" + ",".join(['?' for x in fields]) + ")"

		# Execute insert
		cursor.execute("insert into " + datatype + " values " + insert_params, fields)

	def refresh(self, filename, datatype):
		"""Converts the passed file into database table. Drops the table and recreats it if it already exists."""

		# Init database cursor
		cursor = self.database.cursor()

		# Refresh the table definition
		self.create_table(cursor, datatype)

		# Iterate through each line of the file
		with open(filename, 'rU') as f:
			for line in f:
				# Break up fields using carets, remove whitespace and tilda text field surrounders
				# We also need to decode the text from the Windows cp1252 encoding used by the USDA files
				fields = [unicode(field.strip().strip('~'), "cp1252") for field in line.split('^')]
				
				# Insert row into database
				self.insert_row(cursor, datatype, fields)

		# Commit changes to file
		self.database.commit()

	def create_table(self, cursor, datatype):
		"""Creates a new table in the database based on the datatype. Drops existing table if there is one."""
		
		# Create new table
		cursor.executescript(self.create_table_stmt[datatype])

def main():
	"""Parses USDA flat files and converts them into an sqlite database"""

	# Setup command line parsing
	#parser = argparse.ArgumentParser(description='Process some integers.')
	#parser.add_argument('--sum', dest='accumulate', help='sum the integers (default: find the max)')
	#args = parser.parse_args()

	# Initialize nutrient database
	nutrients = NutrientDB()

	# Parse files
	nutrients.refresh('FOOD_DES.txt', 'food_des')
	nutrients.refresh('FD_GROUP.txt', 'fd_group')
	nutrients.refresh('LANGUAL.txt', 'langual')
	nutrients.refresh('LANGDESC.txt', 'langdesc')
	nutrients.refresh('LANGDESC.txt', 'langdesc')
	nutrients.refresh('NUT_DATA.txt', 'nut_data')
	nutrients.refresh('NUTR_DEF.txt', 'nutr_def')

# Only execute if calling file directly
if __name__=="__main__":
    main()