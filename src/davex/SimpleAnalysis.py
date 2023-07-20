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
from davex import supported_types

import logging
import sys

class runner():
	def __init__(self,databasetype=supported_types.dbtype.nodb,pcache_prefix='',schemaname=''):
		logging.info('here')
		self.sqlite = sqlite_db()
		self.db = None # postgres_db() or mysql_db()
		cache_prefix = ''

		if databasetype == supported_types.dbtype.nodb:
			print('Which database do you want to analyze ?')
			print('1. Postgres')
			print('2. MySQL')
			selectchar = input('select (1,2): ') or 'x'
			if selectchar.upper() == '1':
				databasetype = supported_types.dbtype.Postgres
			elif selectchar.upper() == '2':
				databasetype = supported_types.dbtype.MySQL
			else:
				sys.exit(0)

			if pcache_prefix !='':
				print('Give this database a name in the cache ?')
				cache_prefix = input('cache table prefix: ') or ''
		else:
			if pcache_prefix !='':
				cache_prefix = pcache_prefix 
		logging.info('here1')

		cache_schemas_tablename = cache_prefix.lower() + "schemas"
		cache_tblcounts_tablename = cache_prefix.lower() + "table_counts"
		logging.info('here2')
		if self.sqlite.does_table_exist(cache_schemas_tablename): 
			if schemaname != '':
				self.sqlite.execute("DELETE FROM " + cache_schemas_tablename + " WHERE upper(table_schema) = upper('" + schemaname + "')")
			else:
				self.sqlite.execute("DELETE FROM " + cache_schemas_tablename )

		logging.info('here3')
		if self.sqlite.does_table_exist(cache_tblcounts_tablename): 
			if schemaname != '':
				self.sqlite.execute("DELETE FROM " + cache_tblcounts_tablename + " WHERE upper(table_schema) = upper('" + schemaname + "')")
			else:
				self.sqlite.execute("DELETE FROM " + cache_tblcounts_tablename )

		logging.info('here4')
		logging.info(databasetype)
		logging.info((databasetype == supported_types.dbtype.MySQL))

		if databasetype == supported_types.dbtype.Postgres:
			self.db = postgres_db(cache_prefix.lower().replace('_',''))
		elif databasetype == supported_types.dbtype.MySQL:
			logging.info('here4.1')
			logging.info(cache_prefix.lower().replace('_',''))
			logging.info('here4.2')
			self.db = mysql_db(cache_prefix.lower().replace('_',''))
			logging.info('here4.3')
		else:
			sys.exit(0)

		self.connect()
		logging.info('here5')
		if databasetype == supported_types.dbtype.Postgres:
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

		elif databasetype == supported_types.dbtype.MySQL:
			logging.info('there')
			query_tablecounts = """
				SELECT table_schema,table_name,table_rows as counts
				FROM information_Schema.tables
			"""
			if schemaname == '': 
				query_tablecounts += " WHERE table_schema not in ('performance_schema','sys','information_schema');"
			else:
				query_tablecounts += " WHERE table_schema = '" + schemaname + "'; "

		if databasetype == supported_types.dbtype.Postgres:
			query_schemacounts = """
				select table_schema, count(*) as counts
				from information_schema.tables
				WHERE table_schema not in ('pg_catalog','information_schema') 
				group by table_schema 
			"""
		elif databasetype == supported_types.dbtype.MySQL:
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
			logging.info('load table ' + cache_schemas_tablename)
			self.sqlite.load_csv_to_table(csvschemafilename,cache_schemas_tablename,False,'\t')
		else:
			obj = schemawiz(csvschemafilename)
			sqlite_ddl = obj.guess_sqlite_ddl(cache_schemas_tablename)

			logging.info('\nCreating ' + cache_schemas_tablename)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvschemafilename,cache_schemas_tablename,False,obj.delimiter)

		logging.info(cache_schemas_tablename + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + cache_schemas_tablename)) + ' rows.\n') 


		logging.info("Loading " + csvtablefilename + ' to sqlite cache') # 

		if self.sqlite.does_table_exist(cache_tblcounts_tablename):
			logging.info('table ' + cache_tblcounts_tablename + ' exists.')
			logging.info('tuncate/load table ' + cache_tblcounts_tablename)
			self.sqlite.load_csv_to_table(csvtablefilename,cache_tblcounts_tablename,False,'\t')
		else:
			obj = schemawiz(csvtablefilename)
			sqlite_ddl = obj.guess_sqlite_ddl(cache_tblcounts_tablename)

			logging.info('\nCreating ' + cache_tblcounts_tablename)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvtablefilename,cache_tblcounts_tablename,False,obj.delimiter)

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

	runner(supported_types.dbtype.nodb,'demo','dm_appraisal')
