"""
  Dave Skura, Dec,2022
"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 
import SimpleAnalysis
import readchar

from dbx import help
import sys

def showmenu(dbname,selected_schema=''):
	print('')
	title = 'Actions (' + dbname + ')'
	database_ref = dbname
	search_ref = 'Find all schemas and'
	tblcountsname = dbname.lower()
	schemacountsname = dbname.lower()
	if selected_schema != '':
		database_ref += '.' + selected_schema
		title = 'Actions (' + database_ref + ')'
		search_ref = 'In ' + database_ref 
		tblcountsname +=  '_' + selected_schema + '_table_counts'
		schemacountsname +=  '_' + selected_schema
	else:
		tblcountsname +=  '_table_counts'
		schemacountsname +=  '_schemas'

	print(title)
	print('1. List/Select schemas in ' + dbname)
	print('2. Show all tables in ' + database_ref)
	print('3. ' + search_ref + ' count all tables (cache as ' + tblcountsname + ' & ' + schemacountsname + ')') 
	print('4. Show local cache')
	print('5. Empty local cache')
	print('x. Cancel ') 

def main():
	selected_schema = ''
	sqlite = sqlite_db()
	db = None # postgres_db() or mysql_db() 

	print('\nWhat database are we analyzing ?')
	print('1. Postgres ') 
	print('2. MySQL ') 
	selectchar = readchar.readchar()

	#selectchar = input('select (1,2): ') or 'x'

	print('')
	if selectchar.upper() == '1':
		databasetype = SimpleAnalysis.dbtype.Postgres
		db = postgres_db() 
	elif selectchar.upper() == '2':
		databasetype = SimpleAnalysis.dbtype.MySQL
		db = mysql_db()
	else:
		sys.exit(0)

	db.connect()

	print('Opening Local Cache...')
	sqlite.connect()

	selectchar = 'this'
	while selectchar != '\r':
		showmenu(databasetype.name,selected_schema)
		selectchar = readchar.readchar()
		print('')
		if selectchar.upper() == '1':
			if databasetype == SimpleAnalysis.dbtype.MySQL:
				sql = """
					SELECT
						RANK() OVER (ORDER BY table_schema) as nbr
						,L.*
					FROM (
						SELECT DISTINCT table_schema FROM INFORMATION_SCHEMA.TABLES
						WHERE table_schema not in ('performance_schema','sys','information_schema')
						ORDER BY 1
					)L
				"""
			elif databasetype == SimpleAnalysis.dbtype.Postgres:
				sql = """
					SELECT
							RANK() OVER (ORDER BY table_schema) as nbr
							,L.*
					FROM (
							SELECT DISTINCT table_schema FROM INFORMATION_SCHEMA.TABLES
							WHERE table_schema not in ('pg_catalog','information_schema')
							ORDER BY 1
					)L
				
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
			if databasetype == SimpleAnalysis.dbtype.MySQL:
				sql = """
					SELECT table_schema,table_name FROM INFORMATION_SCHEMA.TABLES
					WHERE table_schema not in ('performance_schema','sys','information_schema')
				"""
			elif databasetype == SimpleAnalysis.dbtype.Postgres:
				sql = """
					SELECT DISTINCT table_schema,table_name FROM INFORMATION_SCHEMA.TABLES
					WHERE table_schema not in ('pg_catalog','information_schema')
				"""
			if selected_schema != '':
				sql += " AND table_schema like '" + selected_schema + "' "

			sql += ' ORDER BY 1,2'
				
			print(db.export_query_to_str(sql,'\t'))

		elif selectchar.upper() == '3':
			help.show_SimpleAnalysis()
			print('')
			SimpleAnalysis.runner(databasetype,selected_schema)

		elif selectchar.upper() == '4':
			selected_cache_table = ''
			innerselectchar = ''
		
			while innerselectchar != '\r':
				print('Local cache')
				sql = """
					SELECT 
							RANK() OVER (ORDER BY name) as nbr
							,name 
					FROM sqlite_master WHERE type = 'table'			
				"""
				data = sqlite.export_query_to_str(sql,'\t')
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

					sql = "SELECT * FROM " + selected_cache_table + " ORDER BY 1 "
					print(sqlite.export_query_to_str(sql,'\t'))

		elif selectchar.upper() == '5':
			print('Emptying cache')
			tables = sqlite.query("	SELECT name FROM sqlite_master WHERE type = 'table'	")
			for row in tables:
				sqlite.execute('drop table ' + row[0])


		
	sqlite.close()

main()