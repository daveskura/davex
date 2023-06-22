"""
  Dave Skura, Dec,2022
"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 


from dbx import help
import sys

def main():
	postgres = postgres_db() 
	mysql = mysql_db() 
	sqlite = sqlite_db()

	print('\nWhat database are we analyzing ?')
	print('1. Postgres ') 
	print('2. MySQL ') 

	selectchar = input('select (1,2): ') or 'x'

	print('')
	if selectchar.upper() == '1':
		dbtype = 'postgres'
		postgres.connect()
	elif selectchar.upper() == '2':
		dbtype = 'mysql'
		mysql.connect()
	else:
		sys.exit(0)

	print('Connecting to local sqlite_db...')
	sqlite.connect()

	print('')
	print('Modules: ')
	print('1. Find all schemas and count all tables (build ' + dbtype + '_schemas & ' + dbtype + '_table_counts)') 
	print('2. Show all schema table counts  ') 
	print('3. List/Select schema(s) ') 
	print('4. Show selected schema table count ') 
	print('5. Show selected schema table row counts ') 

	print('x. Cancel ') 
	selectchar = ''
	#while selectchar != 'x':
	selectchar = input('select (1,2,3,4,5): ') or 'x'
	print('')
	if selectchar.upper() == '1':
		help.show_SimpleAnalysis()
	elif selectchar.upper() == '2':
		sql = """
		postgres_schemas
		"""
		print(sqlite.export_query_to_str(sql,'\t'))
		
	elif selectchar.upper() == '3':
	elif selectchar.upper() == '4':
	elif selectchar.upper() == '5':
	
	sqlite.close()

main()