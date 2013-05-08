#!/usr/bin/python
"""Parses USDA flat files and converts them into an sqlite database"""

import os
import sys
import sqlite3
import argparse
import pymongo

class NutrientDB:
	"""Parses USDA flat files and converts them into an sqlite database"""

	def __init__(self, database_name='nutrients.db'):
		"""Initializes connection to database"""

		# Connect to sqlite database
		self.database = sqlite3.connect(database_name)

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

		self.create_table_stmt["src_cd"] = '''DROP TABLE IF EXISTS src_cd; CREATE TABLE src_cd 
									(Src_Cd, SrcCd_Desc);
									CREATE UNIQUE INDEX src_cd_Src_Cd_idx ON src_cd (Src_Cd)'''

		self.create_table_stmt["deriv_cd"] = '''DROP TABLE IF EXISTS deriv_cd; CREATE TABLE deriv_cd 
									(Deriv_Cd, Deriv_Desc);
									CREATE UNIQUE INDEX deriv_cd_Deriv_Cd_idx ON deriv_cd (Deriv_Cd)'''

		self.create_table_stmt["weight"] = '''DROP TABLE IF EXISTS weight; CREATE TABLE weight 
									(NDB_No, Seq, Amount, Msre_Desc, Gm_Wgt, Num_Data_Pts, Std_Dev);
									CREATE INDEX weight_NDB_No_idx ON weight (NDB_No)'''

		self.create_table_stmt["footnote"] = '''DROP TABLE IF EXISTS footnote; CREATE TABLE footnote 
									(NDB_No, Footnt_No, Footnt_Typ, Nutr_No, Footnt_Txt);
									CREATE INDEX footnote_NDB_No_idx ON footnote (NDB_No)'''

		self.create_table_stmt["data_src"] = '''DROP TABLE IF EXISTS data_src; CREATE TABLE data_src 
									(DataSrc_ID, Authors, Title, Year, Journal, Vol_City, Issue_State, Start_Page, End_Page);
									CREATE UNIQUE INDEX data_src_DataSrc_ID_idx ON data_src (DataSrc_ID)'''

		self.create_table_stmt["datsrcln"] = '''DROP TABLE IF EXISTS datsrcln; CREATE TABLE datsrcln 
									(NDB_No, Nutr_No, DataSrc_ID);
									CREATE INDEX datsrcln_NDB_No_idx ON datsrcln (NDB_No)'''
		
	def export_mongo(self, client):		
		"""Export nutrient data as json into mongodb"""	
		print "trying mongo export"	

		# Iterate through each food item and build a full nutrient json document
		for food in self.database.execute('''
				select * from food_des, fd_group where food_des.FdGrp_Cd = fd_group.FdGrp_Cd'''):
			# Store unique identifier for the food
			ndb_no = food[0]

			# Get all the nutrients in the food
			for nutrient in self.database.execute('''
				select * from nut_data, nutr_def 
				left join src_cd on nut_data.Src_Cd = src_cd.Src_Cd
				left join deriv_cd on nut_data.Deriv_Cd = deriv_cd.Deriv_Cd
				where nut_data.Nutr_No = nutr_def.Nutr_No and nut_data.NDB_No = ?''', [ndb_no]):
				#print nutrient
				pass
				
				# Get the sources of nutrient data
				source_ids = []
				for source in self.database.execute(''' 
					select * from datsrcln where NDB_No = ? and Nutr_No = ?''', [ndb_no, nutrient[1]]):
					source_ids.append(source[2])

			# Get all footnotes for the food
			for footnote in self.database.execute('''select * from footnote where footnote.NDB_No = ?''', [ndb_no]):
				#print footnote
				pass

			# Get gram weight for the food
			for gramweight in self.database.execute('''select * from weight where weight.NDB_No = ?''', [ndb_no]):
				#print gramweight
				pass

			# Get language variants for the food
			for langual in self.database.execute('''
				select * from langual, langdesc where langual.Factor_Code = langdesc.Factor_Code and langual.NDB_No = ?''', [ndb_no]):
				#print langual
				pass

			# Store info in a dictionry that we will insert into mongo
			document = { "description": food['Long_Desc']}

			print document
			# Insert into mongo collection
			break		

	def has_data(self):
		"""Queries the database to see if there is any data in it."""

		# Init database cursor
		cursor = self.database.cursor()
		 
		# Try getting one row of food descriptions table
		try:
			if (cursor.execute("select * from food_des limit 1").fetchone() is None):
				return False
			else:
				return True
		except sqlite3.OperationalError, e:
			return False
		except Exception, e:
			return False
		

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

		# Print out which file we are working on
		sys.stdout.write("Parsing " + filename + '...')
		sys.stdout.flush()

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

		# Done message
		print "Done"

	def create_table(self, cursor, datatype):
		"""Creates a new table in the database based on the datatype. Drops existing table if there is one."""
		
		# Create new table
		cursor.executescript(self.create_table_stmt[datatype])

def main():
	"""Parses USDA flat files and converts them into an sqlite database"""

	# Setup command line parsing
	parser = argparse.ArgumentParser(description='''Parses USDA nutrient database flat files and coverts it into SQLite database. 
		Also provides options for exporting the nutrient data from the SQLite database into other formats.''')
	
	# Add arguments
	parser.add_argument('-p', '--path', dest='path', help='The path to the nutrient data files. (default: data/sr25/)', default='data/sr25/')
	parser.add_argument('-db', '--database', dest='database', help='The name of the SQLite file to read/write nutrient info. (default: nutrients.db)', default='nutrients.db')
	parser.add_argument('-f', '--force', dest='force', action='store_true', help='Whether to force refresh of database file from flat file. If database file already exits and has some data we skip flat file parsing. (default: False)')
	parser.add_argument('-m', '--mongo', dest='mongo', action='store_true', help='Flag that tells whether to try and export data into a mongo db collection.')

	# Parse the arguments
	args = vars(parser.parse_args())

	# Path to flat files
	path = args['path']

	# Check if we need to blow away original db
	if (args['force'] and os.path.exists(args['database'])):
		# Remove existing nutrients database
		os.remove(args['database'])

	# Initialize nutrient database
	nutrients = NutrientDB(args['database'])

	# Parse files
	if (not nutrients.has_data()):
		print "Refreshing database from flat files..."
		nutrients.refresh(path + 'FOOD_DES.txt', 'food_des')
		nutrients.refresh(path + 'FD_GROUP.txt', 'fd_group')
		nutrients.refresh(path + 'LANGUAL.txt', 'langual')
		nutrients.refresh(path + 'LANGDESC.txt', 'langdesc')
		nutrients.refresh(path + 'LANGDESC.txt', 'langdesc')
		nutrients.refresh(path + 'NUT_DATA.txt', 'nut_data')
		nutrients.refresh(path + 'NUTR_DEF.txt', 'nutr_def')
		nutrients.refresh(path + 'SRC_CD.txt', 'src_cd')
		nutrients.refresh(path + 'DERIV_CD.txt', 'deriv_cd')
		nutrients.refresh(path + 'WEIGHT.txt', 'weight')
		nutrients.refresh(path + 'FOOTNOTE.txt', 'footnote')
		nutrients.refresh(path + 'DATA_SRC.txt', 'data_src')
		nutrients.refresh(path + 'DATSRCLN.txt', 'datsrcln')

	# Export each food item as json document into a mongodb
	if args['mongo']:
		nutrients.export_mongo(pymongo.MongoClient('localhost', 27017))

# Only execute if calling file directly
if __name__=="__main__":
    main()