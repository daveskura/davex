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
	def __init__(self,databasetype=dbtype.nodb,pcache_prefix='',schemaname='',):
		self.sqlite = sqlite_db()
		self.db = None # postgres_db() or mysql_db()
		cache_prefix = ''

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

			if pcache_prefix !='':
				print('Give this database a name in the cache ?')
				cache_prefix = input('cache table prefix: ') or ''
		else:
			if pcache_prefix !='':
				cache_prefix = pcache_prefix 


		cache_schemas_tablename = cache_prefix.lower() + databasetype.name.lower() + "_schemas"
		cache_tblcounts_tablename = cache_prefix.lower() + databasetype.name.lower() + "_table_counts"

		if databasetype == dbtype.Postgres:
			self.db = postgres_db()
		elif databasetype == dbtype.MySQL:
			self.db = mysql_db()
		else:
			sys.exit(0)

		self.connect()
		if databasetype == dbtype.Postgres:
			query_tablecounts = """
				select table_schema, 
							 table_name, 
							 (xpath('/row/cnt/text()', xml_count))[1]::text::int as counts
				from (
					select table_name, table_schema, 
								 query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
					from information_schema.tables
					"""
			if schemaname == '': 
				query_tablecounts += "where table_schema not in ('pg_catalog','information_schema')	) t;"
			else:
				query_tablecounts += "where table_schema = '" + schemaname + "' ) t; "

		elif databasetype == dbtype.MySQL:
			query_tablecounts = """
				SELECT table_schema,table_name,table_rows as counts
				FROM information_Schema.tables
			"""
			if schemaname == '': 
				query_tablecounts += " WHERE table_schema not in ('performance_schema','sys','information_schema');"
			else:
				query_tablecounts += " WHERE table_schema = '" + schemaname + "'; "

		if databasetype == dbtype.Postgres:
			query_schemacounts = """
				select table_schema, count(*) as counts
				from information_schema.tables
				WHERE table_schema not in ('pg_catalog','information_schema') 
				group by table_schema 
			"""
		elif databasetype == dbtype.MySQL:
			query_schemacounts = """
				SELECT table_schema,count(*) as counts
				FROM information_Schema.tables 
				WHERE table_schema not in ('performance_schema','sys','information_schema') 
				group by table_schema 
				"""
    
		csvtablefilename = cache_tblcounts_tablename + '.tsv'
		csvschemafilename = cache_schemas_tablename + '.tsv'
		logging.info("Querying " + databasetype.name + " for schema counts ") # 
		self.db.export_query_to_csv(query_schemacounts,csvschemafilename,'\t')

		logging.info("Querying " + databasetype.name + " for table counts ") # 
		self.db.export_query_to_csv(query_tablecounts,csvtablefilename,'\t')

		logging.info("Loading " + csvschemafilename + ' to local sqlite cache') # 

		if self.sqlite.does_table_exist(cache_schemas_tablename):
			logging.info('table ' + cache_schemas_tablename + ' exists.')
			logging.info('tuncate/load table ' + cache_schemas_tablename)
			self.sqlite.load_csv_to_table(csvschemafilename,cache_schemas_tablename,True,'\t')
		else:
			obj = schemawiz(csvschemafilename)
			sqlite_ddl = obj.guess_sqlite_ddl(cache_schemas_tablename)

			logging.info('\nCreating ' + cache_schemas_tablename)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvschemafilename,cache_schemas_tablename,True,obj.delimiter)

		logging.info(cache_schemas_tablename + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + cache_schemas_tablename)) + ' rows.\n') 


		logging.info("Loading " + csvtablefilename + ' to sqlite cache') # 

		if self.sqlite.does_table_exist(cache_tblcounts_tablename):
			logging.info('table ' + cache_tblcounts_tablename + ' exists.')
			logging.info('tuncate/load table ' + cache_tblcounts_tablename)
			self.sqlite.load_csv_to_table(csvtablefilename,cache_tblcounts_tablename,True,'\t')
		else:
			obj = schemawiz(csvtablefilename)
			sqlite_ddl = obj.guess_sqlite_ddl(cache_tblcounts_tablename)

			logging.info('\nCreating ' + cache_tblcounts_tablename)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvtablefilename,cache_tblcounts_tablename,True,obj.delimiter)

		logging.info(cache_tblcounts_tablename + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + cache_tblcounts_tablename)) + ' rows.\n') 

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
