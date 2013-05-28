#!/usr/bin/python
"""Parses USDA flat files and converts them into an sqlite database"""

import os
import sys
import json
import sqlite3
import argparse
import pymongo

class NutrientDB:
	"""Parses USDA flat files and converts them into an sqlite database"""

	def __init__(self, database_name='nutrients.db'):
		"""Initializes connection to database"""

		# Connect to sqlite database
		self.database = sqlite3.connect(database_name)

		# Add enhanced rows to connection
		self.database.row_factory = sqlite3.Row

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
	
	def convert_to_documents(self, mongo_client=None, mongo_db=None, mongo_collection=None):		
		"""Converts the nutrient database into a json document. Optionally inserts into a mongo collection"""	

		# Iterate through each food item and build a full nutrient json document
		for food in self.database.execute('''
				select * from food_des, fd_group where food_des.FdGrp_Cd = fd_group.FdGrp_Cd'''):

			# Store unique identifier for the food
			ndb_no = food['NDB_No']

			# Store base food info as a dictionary (we will later insert this document into mongo)
			document = { 
				'group': food['FdGrp_Desc'],
				'manufacturer': food['ManufacName'],
				"name": {
					"long": food['Long_Desc'],
					'common': [],
					'sci': food['SciName']
				}
			}

			# Split common names by comma to get an array
			comm_names = [com_name for com_name in food['ComName'].split(',') if com_name != '']
			document['name']['common'] = document['name']['common'] + comm_names

			# We also append the langual food source description as other common names of the food
			document['name']['common'] = document['name']['common'] + self.query_langual_foodsource(ndb_no)

			# Add nutrient info
			document['nutrients'] = self.query_nutrients(ndb_no)

			# Add portion gram converstion weights for common measures
			document['portions'] = self.query_gramweight(ndb_no)

			# Put all other data into a meta field
			document['meta']  = {
				'ndb_no': int(ndb_no),
				'nitrogen_factor': food['N_Factor'],
				'protein_factor': food['Pro_Factor'],
				'fat_factor': food['Fat_Factor'],
				'carb_factor': food['CHO_Factor'],
				'fndds_survey': food['Survey'],
				'ref_desc': food['Ref_desc'],
				'ref_per': food['Refuse'],
				'footnotes': self.query_footnote(ndb_no),
				'langual': self.query_langual(ndb_no)
			}

			# Has user passed info to insert into mongo collection
			if (mongo_client and mongo_db and mongo_collection):
				print "Adding to mongo food#: " +  str(document['meta']['ndb_no'])

				# Get refrence to colleciton we want to add the documents to
				collection = mongo_client[mongo_db][mongo_collection]

				# Upsert document into collection 
				collection.update({'meta.ndb_no': document['meta']['ndb_no']}, document, upsert=True)
			else:
				print json.dumps(document)

	def query_gramweight(self, ndb_no):	
		'''Query the nutrient db for gram weight info based on the food's unique ndb number'''

		# Get gram weight for the food
		return [{
			'amt': gramweight['Amount'],
			'unit': gramweight['Msre_Desc'],
			'g': gramweight['Gm_Wgt']
		} for gramweight in self.database.execute('''select * from weight where weight.NDB_No = ?''', [ndb_no])]

	def query_footnote(self, ndb_no):	
		'''Query the nutrient db for footnote info based on the food's unique ndb number'''

		# Get all footnotes for the food
		return [{
			'n_code': footnote['Nutr_No'],
			'type': footnote['Footnt_Typ'],
			'text': footnote['Footnt_Txt']
		} for footnote in self.database.execute('''select * from footnote where footnote.NDB_No = ?''', [ndb_no])]

	def query_langual(self, ndb_no):	
		'''Query the nutrient db for langual description info based on the food's unique ndb number'''

		# Init empty list to store the langual
		thesaurus = []

		# Get language variants for the food
		for langual in self.database.execute('''
			select * from langual, langdesc where langual.Factor_Code = langdesc.Factor_Code
			and langual.Factor_Code not like 'B%'
			and langual.NDB_No = ?''', [ndb_no]):
			thesaurus.append({'code': langual['Factor_Code'], 'description': langual['Description']})

		# Return the langual description info
		return thesaurus

	def query_langual_foodsource(self, ndb_no):	
		'''Query the nutrient db for the "food source" langual info based on the food's unique ndb number and convert into array.'''

		# Init empty list to store the langual
		thesaurus = []

		# Get language variants for the food, we only get the languals starting with A,B,C
		for langual in self.database.execute('''
			select * from langual, langdesc where langual.Factor_Code = langdesc.Factor_Code 
			and langual.Factor_Code like 'B%'
			and langual.NDB_No = ?''', [ndb_no]):
			thesaurus.append(langual['Description'])

		# Return the langual description info
		return thesaurus

	def query_nutrients(self, ndb_no):
		'''Query the nutrient db for nutrients info based on the food's unique ndb number'''

		# Init empty list to store nutrients
		nutrients = []

		# Get all the nutrients in the food
		for nutrient in self.database.execute('''
			select * from nut_data, nutr_def 
			left join src_cd on nut_data.Src_Cd = src_cd.Src_Cd
			left join deriv_cd on nut_data.Deriv_Cd = deriv_cd.Deriv_Cd
			where nut_data.Nutr_No = nutr_def.Nutr_No and nut_data.NDB_No = ?''', [ndb_no]): 
			
			# Get the sources of nutrient data
			source_ids = [source['DataSrc_ID'] for source in self.database.execute(''' 
				select * from datsrcln where NDB_No = ? and Nutr_No = ?''', [ndb_no, nutrient['Nutr_No']])]

			# Filter out the extra id numbers
			nutrient_filtered = {
				'code': nutrient['Nutr_No'],
				'name': nutrient['NutrDesc'],
				'abbr': nutrient['Tagname'],
				'value': nutrient['Nutr_Val'],
				'units': nutrient['Units'],
				'meta': {
					'imputed': nutrient['Ref_NDB_No'],
					'is_add': nutrient['Add_Nutr_Mark'],
					'rounded': nutrient['Num_Dec'],
					'conf': nutrient['CC'],
					'mod_month': nutrient['AddMod_Date'][0:2],
					'mod_year': nutrient['AddMod_Date'][3:],
					'lower_error': nutrient['Low_EB'],
					'upper_error': nutrient['Up_EB'],
					'std_error': nutrient['Std_Error'],
					'data_points': nutrient['Num_Data_Pts'],
					'minval': nutrient['Min'],
					'maxval': nutrient['Max'],
					'degrees_of_freedom': nutrient['DF'],
					'stat_comments': nutrient['Stat_cmt'],
					'sources': source_ids,
					'source_type': nutrient['SrcCd_Desc'],
					'derivation': nutrient['Deriv_Desc'],
					'studies': nutrient['Num_Studies']
				}
			}

			# Add filtered nutrient info to list of nutrients
			nutrients.append(nutrient_filtered)

		# Return all nutrients
		return nutrients

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
	parser.add_argument('-f', '--force', dest='force', action='store_true', help='Whether to force refresh of database file from flat file. If database file already exits and has some data in it we skip flat file parsing.')
	parser.add_argument('-e', '--export', dest='export', action='store_true', help='Converts nutrient data into json documents and outputs to standard out, each document is seperated by a newline.')
	parser.add_argument('--mhost', dest='mhost', help='Mongo hostname. Defaults to localhost.', default='localhost')
	parser.add_argument('--mport', dest='mport', help='Mongo port. Defaults to 27017.', default=27017)
	parser.add_argument('--mdb', dest='mdb', help='Mongo database to connect to.')
	parser.add_argument('--mcoll', dest='mcoll', help='Mongo collection to export data to.')

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
	if args['export']:
		nutrients.convert_to_documents()
	elif (args['mhost'] and args['mport'] and args['mdb'] and args['mcoll']):
		# Export documents to mongo instance
		nutrients.convert_to_documents(mongo_client=pymongo.MongoClient(args['mhost'], args['mport']), mongo_db=args['mdb'], mongo_collection=args['mcoll'])

# Only execute if calling file directly
if __name__=="__main__":
    main()