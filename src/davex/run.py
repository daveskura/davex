"""
  Dave Skura, Dec,2022
"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 

from davex.PostgresTableAnalysis import runner as Postgres_runner
from davex.MySQLTableAnalysis import runner as MySQL_runner
import davex.SimpleAnalysis as SimpleAnalysis

from querychart_package.querychart import charter

import readchar

import logging
from davex import help,supported_types
import sys

logging.basicConfig(level=logging.INFO)

def main():
	selected_schema = ''
	selected_table = ''
	sqlite = sqlite_db()
	sqlite.connect()
	db = None # postgres_db() or mysql_db() 
	dbversion, db, dbtype, cache_prefix = cacheinstancemgr(sqlite).ask_for_instance()
	
	selectchar = 'this'
	while selectchar != '\r':
		showmenu(dbtype.name,cache_prefix,selected_schema,selected_table)
		selectchar = readchar.readchar()
		print('')
		if selectchar.upper() == '0':
			selected_schema, selected_table = Show_cache(sqlite,cache_prefix)

		elif selectchar.upper() == '1':
			selected_table = ''
			selected_schema = Show_ListSelectSchemas(db,sqlite,cache_prefix,dbtype,False)

		elif selectchar.upper() == '2':
			selected_table = ''
			selected_schema = Show_ListSelectSchemas(db,sqlite,cache_prefix,dbtype,True)

		elif selectchar.upper() == '3':
			selected_schema, selected_table = Show_ListSelectTables(db,sqlite,cache_prefix,dbtype,selected_schema,False)

		elif selectchar.upper() == '4':
			selected_schema, selected_table = Show_ListSelectTables(db,sqlite,cache_prefix,dbtype,selected_schema,True)

		elif selectchar.upper() == '5':
			SimpleAnalysis.runner(dbtype,cache_prefix,selected_schema)

		elif selectchar.upper() == '6':
			BuildShow_tableprofile(sqlite,cache_prefix,dbtype,selected_schema,selected_table)

		elif selectchar.upper() == '7':
			grapharoo(cache_prefix,selected_schema,selected_table)

		elif selectchar.upper() == '8':
			grapharooyou(cache_prefix,selected_schema,selected_table)

		elif selectchar.upper() == 'x':
			print('Emptying cache')
			tables = sqlite.query("	SELECT name FROM sqlite_master WHERE type = 'table'	")
			for row in tables:
				sqlite.execute('drop table ' + row[0])

		else:
			sys.exit(0)

	sqlite.close()
	db.close()

class cacheinstancemgr():
	def __init__(self,sqlite):
		self.sqlite = sqlite
		self.sqlite.connect()
		self.cache_prefix = ''
		self.instance_tablename = 'cache_instances'
	
	def checkinstancetable(self):
		if not self.sqlite.does_table_exist(self.instance_tablename):
			logging.info('table ' + self.instance_tablename + ' does not exist.  Creating it.')
			csql = "CREATE TABLE " + self.instance_tablename + """ (
				dbtype integer,
				databasetype text,
				instance_name text
				)
			"""
			self.sqlite.execute(csql)

	def add_instance(self,dbtype,instance_name):
		self.checkinstancetable()
		self.remove_instance(instance_name)
		sql = "INSERT INTO " + self.instance_tablename + "(dbtype,databasetype,instance_name) VALUES (" + str(dbtype.value) + ",'" + dbtype.name + "','" + instance_name + "')"
		self.sqlite.execute(sql)

	def remove_instance(self,instance_name):
		self.sqlite.execute("DELETE FROM " + self.instance_tablename + " WHERE instance_name ='" + instance_name + "'")

	def get_instance_by_ranknbr(self,instance_rankbnr):
		instance_name = ''
		dbtype = None
		databasetype = ''

		sql = """
				SELECT instance_name,dbtype,databasetype 
				FROM (
					SELECT RANK() OVER (ORDER BY instance_name) as nbr ,instance_name,dbtype,databasetype FROM """ + self.instance_tablename + """ 
					) L
				WHERE nbr = """ + instance_rankbnr + """
				ORDER BY 1

		"""		
		data = self.sqlite.query(sql)
		for row in data:
			instance_name = row[0]
			dbtype = supported_types.dbtype(int(row[1]))
			databasetype = row[2]
			break

		return instance_name,dbtype,databasetype

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
		#try:
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
				cache_instance,dbtype,databasetype = self.get_instance_by_ranknbr(rnk_nbr)
				if cache_instance == '':
					enteringnewinstance = True

		else:
			enteringnewinstance = True
			
		#except:
		#	enteringnewinstance = True

		if enteringnewinstance:
			print('Give this database a name in the cache')
			
			cache_instance = input('cache table prefix: ') or 'Demo'

			print('\nWhat database type are you analyzing:')
			print('1. Postgres ') 
			print('2. MySQL ') 
			selectchar = readchar.readchar()

			print('')
			db = None
			if selectchar.upper() == '1':
				dbtype = supported_types.dbtype.Postgres
			elif selectchar.upper() == '2':
				dbtype = supported_types.dbtype.MySQL
			else:
				sys.exit(0)

			self.add_instance(dbtype,cache_instance)

		if dbtype == supported_types.dbtype.Postgres:
			db = postgres_db(cache_instance) 
		elif dbtype == supported_types.dbtype.MySQL:
			db = mysql_db(cache_instance)
		else:
			sys.exit(0)

		db.connect()

		dbversion = db.queryone('SELECT VERSION()')
		# cache prefix has an underscore after it
		cache_prefix = cache_instance + '_'

		return dbversion, db, dbtype, cache_prefix 

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
	print('5. Count tables, store in cache.') 
	if selected_table !='':
		print('6. Show table details for ' + database_ref)
		print('7. Graph distinct value counts by field for ' + database_ref)
		print('8. Graph percent (%) distinct value counts/total rows by field for ' + database_ref)

	print('x. Empty local cache')

def BuildShow_tableprofile(cache_db,cache_prefix,databasetype,selected_schema,selected_table):
	metrics_table_name = cache_prefix + 'table_metrics'
	metrics_tablehdr_name = cache_prefix + 'table_comments'
	sql = "SELECT COUNT(*) FROM " + metrics_table_name + " WHERE schema_name = '" + selected_schema + "' AND table_name = '" + selected_table + "'"

	rebuildmetrics = False
	if not cache_db.does_table_exist(metrics_table_name):
		rebuildmetrics = True
	elif cache_db.queryone(sql) == 0:
		rebuildmetrics = True

	if rebuildmetrics:
		if databasetype == supported_types.dbtype.MySQL:
			actor = MySQL_runner(cache_prefix,selected_schema,selected_table)
		elif databasetype == supported_types.dbtype.Postgres:
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

	data = cache_db.query(sql)
	colcount = 0
	for row in data:
		colcount += 1

	header_on = True
	for row in data:
		if header_on:
			print('Table: ',row[0])
			print('  ',str(colcount),' Columns.')
			print('  ',str(row[7]),' Rows.')
			print('  ',row[6],'\n')
			header_on = False

		print('field name: ',row[1]) # field_name
		print('Sample data: ',row[2])
		print('Count of distinct values: ',row[3])
		print('Indexes: ',row[4])
		print('DDL/Field comments: ',row[5],'\n')



def Show_cache(cache_db,cache_prefix):
	selected_schema = ''
	selected_table = ''

	print('Local cache')
	sql = """
		SELECT 
				RANK() OVER (ORDER BY name) as nbr
				,name 
		FROM sqlite_master WHERE type = 'table'	AND name like '%""" + cache_prefix + """%'
	"""
	try:
		data = cache_db.export_query_to_str(sql,'\t')
	except:
		data = '\t\n'

	print(data)
	datalines = data.split('\n')
	linecounter = len(datalines) - 2
	innerselectchar = ''
	if linecounter < 10:
		print('select (nbr):')
		innerselectchar = readchar.readchar()
	else:
		innerselectchar = input('select (nbr): ') or '\r'

	selected_cache_table = ''
	if innerselectchar != '\r':
		for row in datalines:
			flds = row.split('\t')
			if flds[0].lower() == innerselectchar.lower():
				selected_cache_table = flds[1]
				print(selected_cache_table + ':')

		sql = "SELECT DENSE_RANK() OVER (ORDER BY counts) as nbr,* FROM " + selected_cache_table + " ORDER BY counts "
		try:
			data = cache_db.export_query_to_str(sql,'\t')
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

			logging.info('Schema: ' + selected_schema  )
			if selected_table != '':
				logging.info('Table: ' + selected_table  )
		except:
			data = '\t\n'

	return selected_schema, selected_table

def Show_ListSelectTables(db,cache_db,cache_prefix,databasetype,suggested_schema,usecache=False):
	cache_tblcounts_tablename = cache_prefix + "table_counts"
	selected_schema = ''
	selected_table = ''
	if usecache:

		try:
			sql = "SELECT DENSE_RANK() OVER (ORDER BY counts,table_name) as nbr,table_schema,table_name,counts FROM " + cache_tblcounts_tablename 
			sql += " WHERE table_schema like '%" + suggested_schema + "%'"
			sql += " ORDER BY counts,table_name "
			data = cache_db.export_query_to_str(sql,'\t')
			print(data)
			tableselecter = input('Select Table (nbr): ') or '\r'
			if tableselecter != '\r':
				datalines = data.split('\n')
				for row in datalines:
					flds = row.split('\t')
					if flds[0] == tableselecter:
						selected_schema = flds[1]
						selected_table = flds[2]

			if selected_table != '':
				print(selected_schema + '.' + selected_table + ' selected.')
		except:
			print('Cache table ' + cache_tblcounts_tablename + " may not exist yet.  Try build cache first")

	else:
		if databasetype == supported_types.dbtype.MySQL:
			sql = """
				SELECT DENSE_RANK() OVER (ORDER BY table_schema,table_name) as nbr,table_schema,table_name FROM INFORMATION_SCHEMA.TABLES
				WHERE table_schema not in ('performance_schema','sys','information_schema')
			"""
		elif databasetype == supported_types.dbtype.Postgres:
			sql = """
				SELECT DENSE_RANK() OVER (ORDER BY table_schema,table_name) as nbr,table_schema,table_name FROM INFORMATION_SCHEMA.TABLES
				WHERE table_schema not in ('pg_catalog','information_schema')
			"""

		sql += " AND table_schema like '%" + suggested_schema + "%' "				
		sql += ' ORDER BY table_schema,table_name'
			
		try:
			data = db.export_query_to_str(sql,'\t')

			print(data)
			tableselecter = input('Select Table (nbr): ') or '\r'
			if tableselecter != '\r':
				datalines = data.split('\n')
				for row in datalines:
					flds = row.split('\t')
					if flds[0] == tableselecter:
						selected_schema = flds[1]
						selected_table = flds[2]

			if selected_table != '':
				print(selected_schema + '.' + selected_table + ' selected.')
		except:
			data = '\t\n'

	return selected_schema, selected_table

def Show_ListSelectSchemas(db,cache_db,cache_prefix,databasetype,usecache=False):
	cache_schemas_tablename = cache_prefix + "schemas"
	selected_schema = ''
	
	if usecache:
	
		try:		
			sql = "SELECT rowid,A.* FROM " + cache_schemas_tablename + " A order by rowid"
			data = cache_db.export_query_to_str(sql,'\t')
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

	else:
		if databasetype == supported_types.dbtype.MySQL:
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
		elif databasetype == supported_types.dbtype.Postgres:
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
	return selected_schema

def grapharooyou(cache_prefix='',selected_schema='',selected_table=''):
	title = selected_schema + '.' + selected_table + ' (' + cache_prefix.replace('_','') + ')'
	xlabel = ''
	ylabel = 'Diversity Percent (%)'
	full_tablename = cache_prefix.lower() + 'table_metrics'
	full_counttablename = cache_prefix.lower() + 'table_counts'
	sql = """
		SELECT field_name as field
				,round(cast(distinct_values as float)/B.Counts*100,2) as diversity_pct
		FROM """ + full_tablename + """ A
		INNER JOIN """ + full_counttablename + """ B 
				ON (A.schema_name=B.TABLE_SCHEMA AND A.table_name = B.TABLE_NAME)
		WHERE A.schema_name='""" + selected_schema + "' AND	A.table_name='" + selected_table + "'"

	charter().csv_querybarchart(sql,'',title,xlabel,ylabel,8)

def grapharoo(cache_prefix='',selected_schema='',selected_table=''):
	logging.info("cache_prefix " + cache_prefix) # 
	logging.info("selected_schema " + selected_schema) # 
	logging.info("selected_table " + selected_table) # 
	print('')
	title = selected_schema + '.' + selected_table + ' (' + cache_prefix.replace('_','') + ')'
	xlabel = ''
	ylabel = 'Distinct Values'
	full_tablename = cache_prefix + 'table_metrics'
	sql = """
		SELECT field_name as field,distinct_values as counts 
		FROM """ + full_tablename + """
		WHERE schema_name='""" + selected_schema + "' AND	table_name='" + selected_table + "'"

	charter().csv_querybarchart(sql,'',title,xlabel,ylabel,8)



main()