"""
  Dave Skura, Dec,2022
"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 
from PostgresTableAnalysis import runner as Postgres_runner
from MySQLTableAnalysis import runner as MySQL_runner

import SimpleAnalysis
import readchar

import logging
from dbx import help
import sys
class cacheinstancemgr():
	def __init__(self):
		self.sqlite = sqlite_db()
		self.sqlite.connect()
		self.cache_prefix = ''
		self.instance_tablename = 'cache_instances'
	
	def checkinstancetable(self):
		if not self.sqlite.does_table_exist(self.instance_tablename):
			logging.info('table ' + self.instance_tablename + ' does not exist.  Creating it.')
			csql = "CREATE TABLE " + self.instance_tablename + """ (
				instance_name text
				)
			"""
			self.sqlite.execute(csql)

	def add_instance(self,instance_name):
		self.checkinstancetable()
		self.remove_instance(instance_name)
		self.sqlite.execute("INSERT INTO " + self.instance_tablename + "(instance_name) VALUES ('" + instance_name + "')")

	def remove_instance(self,instance_name):
		self.sqlite.execute("DELETE FROM " + self.instance_tablename + " WHERE instance_name ='" + instance_name + "'")

	def get_instance_by_ranknbr(self,instance_rankbnr):
		sql = """
				SELECT instance_name
				FROM (
					SELECT RANK() OVER (ORDER BY instance_name) as nbr ,instance_name FROM """ + self.instance_tablename + """ 
					) L
				WHERE nbr = """ + instance_rankbnr + """
				ORDER BY 1

		"""
		return str(self.sqlite.queryone(sql))

	def ask_for_instance(self):
		enteringnewinstance = False
		self.checkinstancetable()
		sql = """
				SELECT *
				FROM (
					SELECT 0 as nbr,'New Instance' as instance_name
					UNION ALL
					SELECT RANK() OVER (ORDER BY instance_name) as nbr ,instance_name  FROM """ + self.instance_tablename + """ 
					) L
				ORDER BY nbr
				"""
		try:
			data = self.sqlite.export_query_to_str(sql,'\t')
			datalines = data.split('\n')
			linecounter = len(datalines) - 2
			if linecounter > 0:
				print('Cache Instances:')
				print(data)
				linecounter = len(datalines) - 2
				if linecounter < 10:
					print('select (nbr):')
					rnk_nbr = readchar.readchar()
				else:
					rnk_nbr = input('select (nbr): ') or '\r'
				if (rnk_nbr =='\r' or rnk_nbr =='0'): 
					enteringnewinstance = True
				else:
					cache_instance = self.get_instance_by_ranknbr(rnk_nbr)

			else:
				enteringnewinstance = True
			
		except:
			enteringnewinstance = True

		if enteringnewinstance:
			print('Give this database a name in the cache')
			
			cache_instance = input('cache table prefix: ') or 'Demo'
			self.add_instance(cache_instance)

		cache_instance += '_'
		return cache_instance

def showmenu(dbname,pcache_prefix='',selected_schema='',selected_table=''):
	print('')
	title = '' 
	database_ref = dbname
	search_ref = 'Build cache.  Find all schemas and'
	if selected_schema != '':
		database_ref += '.' + selected_schema
		if selected_table != '':
			database_ref += '.' + selected_table
		search_ref = 'Build cache.  ' + database_ref 
	
	if pcache_prefix == '':
		cache_prefix = ''
		title = database_ref + ' :'
	else:
		cache_prefix = pcache_prefix
		title =  database_ref + ' on ' + cache_prefix[:-1] + ' :'

	print(title)
	print('0. Show local cache')
	print('1. List/Select schemas in ' + dbname)
	print('2. List/Select schemas counts ')
	print('3. List/Select tables in ' + database_ref)
	print('4. List/Select tables with row counts ' )
	print('5. ' + search_ref + ' count all tables') 
	if selected_table !='':
		print('6. Show table details for ' + database_ref)

	print('x. Empty local cache')

def main():
	selected_schema = ''
	selected_table = ''
	sqlite = sqlite_db()
	db = None # postgres_db() or mysql_db() 

	print('\nWhat database are we analyzing ?')
	print('1. Postgres ') 
	print('2. MySQL ') 
	selectchar = readchar.readchar()

	print('')
	if selectchar.upper() == '1':
		databasetype = SimpleAnalysis.dbtype.Postgres
		db = postgres_db() 
	elif selectchar.upper() == '2':
		databasetype = SimpleAnalysis.dbtype.MySQL
		db = mysql_db()
	else:
		sys.exit(0)

	cache_prefix = cacheinstancemgr().ask_for_instance()
	
	metrics_table_name = cache_prefix + 'table_metrics'
	metrics_tablehdr_name = cache_prefix + 'table_comments'
	cache_schemas_tablename = cache_prefix + databasetype.name + "_schemas"
	cache_tblcounts_tablename = cache_prefix + databasetype.name + "_table_counts"

	db.connect()

	print('Opening Local Cache...')
	sqlite.connect()

	selectchar = 'this'
	while selectchar != '\r':
		showmenu(databasetype.name,cache_prefix,selected_schema,selected_table)
		selectchar = readchar.readchar()
		print('')
		if selectchar.upper() == '0':
			selected_cache_table = ''
			innerselectchar = ''
		
			print('Local cache')
			sql = """
				SELECT 
						RANK() OVER (ORDER BY name) as nbr
						,name 
				FROM sqlite_master WHERE type = 'table'	AND name like '%""" + cache_prefix + """%'
			"""
			try:
				data = sqlite.export_query_to_str(sql,'\t')
			except:
				data = '\t\n'

			print(data)
			datalines = data.split('\n')
			linecounter = len(datalines) - 2
			if linecounter < 10:
				print('select (nbr):')
				innerselectchar = readchar.readchar()
			else:
				innerselectchar = input('select (nbr): ') or '\r'

			if innerselectchar != '\r':
				for row in datalines:
					flds = row.split('\t')
					if flds[0].lower() == innerselectchar.lower():
						selected_cache_table = flds[1]
						print(selected_cache_table + ':')

				sql = "SELECT DENSE_RANK() OVER (ORDER BY counts) as nbr,* FROM " + selected_cache_table + " ORDER BY counts "
				try:
					data = sqlite.export_query_to_str(sql,'\t')
					print(data)
					tableselecter = input('Select Table (nbr): ') or '\r'
					if tableselecter != '\r':
						datalines = data.split('\n')
						for row in datalines:
							flds = row.split('\t')
							if flds[0] == tableselecter:
								if datalines[0].split('\t')[1].lower() == 'table_schema':
									selected_schema = flds[1]
								if datalines[0].split('\t')[2].lower() == 'table_name':
									selected_table = flds[2]

					if selected_table != '':
						print(selected_schema + '.' + selected_table + ' selected.')
				except:
					data = '\t\n'

		elif selectchar.upper() == '1':
			if databasetype == SimpleAnalysis.dbtype.MySQL:
				sql = """
					SELECT
						DENSE_RANK() OVER (ORDER BY table_schema) as nbr
						,L.*
					FROM (
						SELECT DISTINCT table_schema FROM INFORMATION_SCHEMA.TABLES
						WHERE table_schema not in ('performance_schema','sys','information_schema')
					)L
					ORDER BY table_schema
				"""
			elif databasetype == SimpleAnalysis.dbtype.Postgres:
				sql = """
					SELECT
							DENSE_RANK() OVER (ORDER BY table_schema) as nbr
							,L.*
					FROM (
							SELECT DISTINCT table_schema FROM INFORMATION_SCHEMA.TABLES
							WHERE table_schema not in ('pg_catalog','information_schema')
					)L
					ORDER BY table_schema
				"""
			data = db.export_query_to_str(sql,'\t')
			print(data)
			datalines = data.split('\n')
			schemacounter = len(datalines) - 2

			if schemacounter < 10:
				print('select (nbr):')
				insideselectchar = readchar.readchar()
			else:
				insideselectchar = input('select (nbr): ') or '\r'
			
			if insideselectchar != '\r':
				for row in datalines:
					flds = row.split('\t')
					if flds[0].lower() == insideselectchar.lower():
						selected_schema = flds[1]
						print('You selected ' + selected_schema)
			print('')


		elif selectchar.upper() == '2':
			try:
				sql = "SELECT rowid,* FROM " + cache_schemas_tablename + " order by rowid"
				data = sqlite.export_query_to_str(sql,'\t')
				print(data)
				datalines = data.split('\n')
				schemacounter = len(datalines) - 2

				if schemacounter < 10:
					print('select (nbr):')
					insideselectchar = readchar.readchar()
				else:
					insideselectchar = input('select (nbr): ') or '\r'
				
				if insideselectchar != '\r':
					for row in datalines:
						flds = row.split('\t')
						if flds[0].lower() == insideselectchar.lower():
							selected_schema = flds[1]
							print('You selected ' + selected_schema)
				print('')
			except:
				print("Cache table " + cache_schemas_tablename + " may not exist yet.  Try build cache first")

		elif selectchar.upper() == '3':
			if databasetype == SimpleAnalysis.dbtype.MySQL:
				sql = """
					SELECT DENSE_RANK() OVER (ORDER BY table_schema,table_name) as nbr,table_schema,table_name FROM INFORMATION_SCHEMA.TABLES
					WHERE table_schema not in ('performance_schema','sys','information_schema')
					ORDER BY table_schema,table_name
				"""
			elif databasetype == SimpleAnalysis.dbtype.Postgres:
				sql = """
					SELECT DENSE_RANK() OVER (ORDER BY table_schema,table_name) as nbr,table_schema,table_name FROM INFORMATION_SCHEMA.TABLES
					WHERE table_schema not in ('pg_catalog','information_schema')
					ORDER BY table_schema,table_name
				"""
			if selected_schema != '':
				sql += " AND table_schema like '" + selected_schema + "' "

			sql += ' ORDER BY 1,2'
				
			try:
				data = db.export_query_to_str(sql,'\t')
				print(data)
				tableselecter = input('Select Table (nbr): ') or '\r'
				if tableselecter != '\r':
					datalines = data.split('\n')
					for row in datalines:
						flds = row.split('\t')
						if flds[0] == tableselecter:
							if datalines[0].split('\t')[1].lower() == 'table_schema':
								selected_schema = flds[1]
							if datalines[0].split('\t')[2].lower() == 'table_name':
								selected_table = flds[2]

				if selected_table != '':
					print(selected_schema + '.' + selected_table + ' selected.')
			except:
				data = '\t\n'

		elif selectchar.upper() == '4':
			try:
				sql = "SELECT DENSE_RANK() OVER (ORDER BY counts,table_name) as nbr,* FROM " + cache_tblcounts_tablename + " ORDER BY counts,table_name "
				data = sqlite.export_query_to_str(sql,'\t')
				print(data)
				tableselecter = input('Select Table (nbr): ') or '\r'
				if tableselecter != '\r':
					datalines = data.split('\n')
					for row in datalines:
						flds = row.split('\t')
						if flds[0] == tableselecter:
							if datalines[0].split('\t')[1].lower() == 'table_schema':
								selected_schema = flds[1]
							if datalines[0].split('\t')[2].lower() == 'table_name':
								selected_table = flds[2]

				if selected_table != '':
					print(selected_schema + '.' + selected_table + ' selected.')
			except:
				print('Cache table ' + cache_tblcounts_tablename + " may not exist yet.  Try build cache first")


		elif selectchar.upper() == '5':
			help.show_SimpleAnalysis()
			print('')
			SimpleAnalysis.runner(databasetype,cache_prefix,selected_schema)


		elif selectchar.upper() == '6':
			#help.show_TableAnalysis()

			sql = "SELECT COUNT(*) FROM " + metrics_table_name + " WHERE schema_name = '" + selected_schema + "' AND table_name = '" + selected_table + "'"

			rebuildmetrics = False
			if not sqlite.does_table_exist(metrics_table_name):
				rebuildmetrics = True
			elif sqlite.queryone(sql) == 0:
				rebuildmetrics = True

			if rebuildmetrics:
				if databasetype == SimpleAnalysis.dbtype.MySQL:
					actor = MySQL_runner(cache_prefix,selected_schema,selected_table)
				elif databasetype == SimpleAnalysis.dbtype.Postgres:
					actor = Postgres_runner(cache_prefix,selected_schema,selected_table)


			sql = """
			SELECT schema_name||'.'||table_name as tableref
				,field_name
				,sample_data
				,distinct_values
				,CASE WHEN indexes = 'None' THEN '' ELSE indexes END as indexes
				,field_comments
				,table_comments
				,row_counts
			FROM """ + metrics_table_name + """ A
				INNER JOIN """ + metrics_tablehdr_name + """ B USING (schema_name,table_name)
			WHERE schema_name='world' and table_name='city'
			"""
			sql = sql.replace('world',selected_schema)
			sql = sql.replace('city',selected_table)

			data = sqlite.query(sql)
			colcount = 0
			for row in data:
				colcount += 1

			

			hdron = True
			for row in data:
				if hdron:
					print('Table: ',row[0])
					print('  ',str(colcount),' Columns.')
					print('  ',str(row[7]),' Rows.')
					print('  ',row[6],'\n')
					hdron = False

				print('field name: ',row[1]) # field_name
				print('Sample data: ',row[2])
				print('Count of distinct values: ',row[3])
				print('Indexes: ',row[4])
				print('DDL/Field comments: ',row[5],'\n')

		elif selectchar.upper() == 'x':
			print('Emptying cache')
			tables = sqlite.query("	SELECT name FROM sqlite_master WHERE type = 'table'	")
			for row in tables:
				sqlite.execute('drop table ' + row[0])
		
	sqlite.close()

main()