"""
  Dave Skura, Dec,2022
"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 
import SimpleAnalysis


from dbx import help
import sys

def main():
	sqlite = sqlite_db()
	db = None # postgres_db() or mysql_db() 

	print('\nWhat database are we analyzing ?')
	print('1. Postgres ') 
	print('2. MySQL ') 

	selectchar = input('select (1,2): ') or 'x'

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

	print('Connecting to local sqlite_db...')
	sqlite.connect()

	print('')
	print('Modules: ')
	print('1. Find all schemas and count all tables (build ' + databasetype.name.lower() + '_schemas & ' + databasetype.name.lower() + '_table_counts)') 
	print('2. Show tables stored in local sqlite_db')
	print('3. Show all ' + databasetype.name + ' schema table counts  ') 
	print('4. List/Select ' + databasetype.name + ' schema(s) ') 
	print('5. Show selected ' + databasetype.name + ' schema table count ') 
	print('6. Show selected ' + databasetype.name + ' schema table row counts ') 

	print('x. Cancel ') 
	selectchar = ''
	while selectchar != 'x':
		selectchar = input('select (1,2,3,4,5): ') or 'x'
		print('')
		if selectchar.upper() == '1':
			help.show_SimpleAnalysis()
			print('')
			SimpleAnalysis.runner(databasetype)

		elif selectchar.upper() == '2':
			sql = """
			SELECT name FROM sqlite_master WHERE type = 'table'
			"""
			print(sqlite.export_query_to_str(sql,'\t'))
			
		elif selectchar.upper() == '3':
			sql = """
			SELECT *
			FROM postgres_schemas
			"""
			print(sqlite.export_query_to_str(sql,'\t'))
		
		elif selectchar.upper() == '4':
			sql = """
			SELECT *
			FROM postgres_schemas
			"""
			print(sqlite.export_query_to_str(sql,'\t'))

		elif selectchar.upper() == '5':
			sql = """
			SELECT *
			FROM postgres_schemas
			"""
			print(sqlite.export_query_to_str(sql,'\t'))
		
	sqlite.close()

main()