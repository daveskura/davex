"""
  Dave Skura

	Connect to Postgres
	Connect to local sqlite_db
	read tables and rowcounts by schema/table
	load into sqlite_db

"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 
from schemawizard_package.schemawizard import schemawiz

import logging
import sys

from enum import Enum

class dbtype(Enum):
	nodb		= 0
	Postgres= 1
	MySQL		= 2
	SQLite	= 3

class runner():
	def __init__(self,databasetype=dbtype.nodb):
		self.sqlite = sqlite_db()
		self.db = None # postgres_db() or mysql_db()

		if databasetype == dbtype.nodb:
			print('Which database do you want to analyze ?')
			print('1. Postgres')
			print('2. MySQL')
			selectchar = input('select (1,2): ') or 'x'
			if selectchar.upper() == '1':
				databasetype = dbtype.Postgres
			elif selectchar.upper() == '2':
				databasetype = dbtype.MySQL
			else:
				sys.exit(0)

		if databasetype == dbtype.Postgres:
			self.db = postgres_db()
		elif databasetype == dbtype.MySQL:
			self.db = mysql_db()
		else:
			sys.exit(0)

		self.connect()
		query_tablecounts = """
			select table_schema, 
						 table_name, 
						 (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
			from (
				select table_name, table_schema, 
							 query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
				from information_schema.tables
				where table_schema not in ('pg_catalog','information_schema')
			) t;
		"""
		query_schemacounts = """

			select table_schema, count(*) as table_count
			from information_schema.tables
			where table_schema not in ('pg_catalog','information_schema')
			group by table_schema


		"""
    
		tblcountsname = databasetype.name.lower() + '_table_counts'
		schemacountsname = databasetype.name.lower() + '_schemas'


		csvtablefilename = 'tables.tsv'
		csvschemafilename = 'schemas.tsv'
		logging.info("Querying " + databasetype.name + " for schema counts using query_to_xml/xpath against (information_schema.tables) ") # 
		self.db.export_query_to_csv(query_schemacounts,csvschemafilename,'\t')

		logging.info("Querying " + databasetype.name + " for table counts using query_to_xml/xpath against (information_schema.tables) ") # 
		self.db.export_query_to_csv(query_tablecounts,csvtablefilename,'\t')

		logging.info("Loading " + csvschemafilename + ' to local_sqlite_db') # 

		if self.sqlite.does_table_exist(schemacountsname):
			logging.info('table ' + schemacountsname + ' exists.')
			logging.info('tuncate/load table ' + schemacountsname)
			self.sqlite.load_csv_to_table(csvschemafilename,schemacountsname,True,'\t')
		else:
			obj = schemawiz(csvschemafilename)
			sqlite_ddl = obj.guess_sqlite_ddl(schemacountsname)

			logging.info('\nCreating ' + schemacountsname)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvschemafilename,schemacountsname,False,obj.delimiter)

		logging.info(schemacountsname + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + schemacountsname)) + ' rows.\n') 


		logging.info("Loading " + csvtablefilename + ' to local_sqlite_db') # 

		if self.sqlite.does_table_exist(tblcountsname):
			logging.info('table ' + tblcountsname + ' exists.')
			logging.info('tuncate/load table ' + tblcountsname)
			self.sqlite.load_csv_to_table(csvtablefilename,tblcountsname,True,'\t')
		else:
			obj = schemawiz(csvtablefilename)
			sqlite_ddl = obj.guess_sqlite_ddl(tblcountsname)

			logging.info('\nCreating ' + tblcountsname)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvtablefilename,tblcountsname,False,obj.delimiter)

		logging.info(tblcountsname + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + tblcountsname)) + ' rows.\n') 



		self.disconnect()
	def connect(self):
		self.db.connect()
		logging.info('Connected to ' + self.db.db_conn_dets.dbconnectionstr())
		self.sqlite.connect()
		logging.info('Connected to ' + self.sqlite.db_conn_dets.dbconnectionstr())

	def disconnect(self):
		self.sqlite.close()
		self.db.close()

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info(" Starting Simple Analysis") # 

	runner()
