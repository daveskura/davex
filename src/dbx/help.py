"""
  Dave Skura, Dec,2022
"""
import sys

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'run.py': # no parameters
		print('')
		print('Modules: ')
		print('1. py -m dbx.run  ') 
		print('2. py -m dbx.SimpleAnalysis  ') 
		print('3. py -m dbx.sqliter  ') 
		print('')
		print('4. py -m dbx.sqlite_export') 
		print('5. py -m dbx.sqlite_import') 
		print('6. py -m dbx.sqlite_extract') 
		print('')
		print('7. py -m dbx.postgres_import  ') 
		print('8. py -m dbx.postgres_export  ') 
		print('9. py -m dbx.postgres_extract  ') 
		print('')
		print('10. py -m dbx.mysql_import  ') 
		print('11. py -m dbx.mysql_export  ') 
		print('12. py -m dbx.mysql_extract  ') 
		print(' ')
		print('x. Cancel')

		selectchar = ''
		while selectchar != 'x':
			selectchar = input('select (1,2,3,4,5,6,7,8,9,10,11,12): ') or 'x'
			print('')
			if selectchar.upper() == '1':
				show_run()
			elif selectchar.upper() == '2':
				show_SimpleAnalysis()
			elif selectchar.upper() == '3':
				show_sqliter()
			elif selectchar.upper() == '4':
				show_sqlite_export()
			elif selectchar.upper() == '5':
				show_sqlite_import()
			elif selectchar.upper() == '6':
				show_sqlite_extract()
			elif selectchar.upper() == '7':
				show_postgres_import()
			elif selectchar.upper() == '8':
				show_postgres_export()
			elif selectchar.upper() == '9':
				show_postgres_extract()
			elif selectchar.upper() == '10':
				show_mysql_import()
			elif selectchar.upper() == '11':
				show_mysql_export()
			elif selectchar.upper() == '12':
				show_mysql_extract()

	else: 
		parameter = sys.argv[1]
		if parameter.strip().lower() == 'run':
			show_run()
		elif parameter.strip().lower() == 'simpleanalysis':
			show_SimpleAnalysis()
		elif parameter.strip().lower() == 'sqliter':
			show_sqliter()
		elif parameter.strip().lower() == 'sqlite_export':
			show_sqlite_export()
		elif parameter.strip().lower() == 'sqlite_import':
			show_sqlite_import()
		elif parameter.strip().lower() == 'sqlite_extract':
			show_sqlite_extract()
		elif parameter.strip().lower() == 'postgres_import':
			show_postgres_import()
		elif parameter.strip().lower() == 'postgres_export':
			show_postgres_export()
		elif parameter.strip().lower() == 'postgres_extract':
			show_postgres_extract()

		elif parameter.strip().lower() == 'mysql_import':
			show_mysql_import()
		elif parameter.strip().lower() == 'mysql_export':
			show_mysql_export()
		elif parameter.strip().lower() == 'mysql_extract':
			show_mysql_extract()
		else:
			print('parameter unknown')

def show_run():
	print('Usage: py -m dbx.run')
	print(' - does nothing yet')

def show_SimpleAnalysis():
	print('Usage: py -m dbx.SimpleAnalysis')

	print('\nDescription:')		
	print('Connect to Postgres')		
	print("Query MySQL or Postgres for schema counts ") # 
	print("\t build file <*schemas>.tsv with counts of tables by schema")
	print("Querying MySQL or Postgres for table counts  ") # 
	print("\t build file <*tables.tsv> with rowcounts by table")

	print('Connect to local cache)
	print("\t create/load table <*_schemas> from file <*schemas.tsv>")
	print("\t create/load table <*_table_counts> from file <*tables.tsv>")

def show_sqliter():
	print('Usage: py -m dbx.sqliter [-f sqlfilename] [SQL Statement] ')
	print('sqliter.py -f sqlfilename ') 
	print('sqliter.py <SELECT SQL Statement>')
	print('sqliter.py "SELECT name FROM sqlite_master WHERE type = \'table\'"')
	print('sqliter.py "SELECT * FROM postgres_table_counts ORDER BY row_count DESC limit 10"')

def show_sqlite_export():
	print('Usage: py -m dbx.sqlite_export [tablename] [csvfilename] [delimiter]')
	print("\t tablename <text>  eg 'anytablename'")
	print("\t csvfilename <text>  eg 'anyfilename.csv'")
	print("\t delimiter <char>  eg. '~' (tab is default)\n")

def show_sqlite_import():
	print('Usage: py -m dbx.sqlite_import [csv_filename] [tablename] [WithTruncate]')
	print("\t csv_filename <text> eg. 'anyfilename.csv'")
	print("\t tablename <text> eg. 'anytablename'")
	print('\t WithTruncate <bool> must be either True or False\n')

def show_sqlite_extract():
	print('Usage: py -m dbx.sqlite_extract [query] [csvfilename] [delimiter] ')

def show_postgres_import():
	print('Usage: py -m dbx.postgres_import [csv_filename] [tablename] [WithTruncate]') 
	print("\t csv_filename <text> eg. 'anyfilename.csv'")
	print("\t tablename <text> eg. 'anytablename'")
	print('\t WithTruncate <bool> must be either True or False\n')

def show_postgres_export():
	print('Usage: py -m dbx.postgres_export [tablename] [csvfilename] [delimiter] ') 
	print("\t tablename <text>  eg 'anytablename'")
	print("\t csvfilename <text>  eg 'anyfilename.csv'")
	print("\t delimiter <char>  eg. '~' (tab is default)\n")

def show_postgres_extract():
	print('Usage: py -m dbx.postgres_extract [query] [csvfilename] [delimiter] ')

def show_mysql_import():
	print('Usage: py -m dbx.mysql_import [csv_filename] [tablename] [WithTruncate]') 
	print("\t csv_filename <text> eg. 'anyfilename.csv'")
	print("\t tablename <text> eg. 'anytablename'")
	print('\t WithTruncate <bool> must be either True or False\n')

def show_mysql_export():
	print('Usage: py -m dbx.mysql_export [tablename] [csvfilename] [delimiter] ') 
	print("\t tablename <text>  eg 'anytablename'")
	print("\t csvfilename <text>  eg 'anyfilename.csv'")
	print("\t delimiter <char>  eg. '~' (tab is default)\n")

def show_mysql_extract():
	print('Usage: py -m dbx.mysql_extract [query] [csvfilename] [delimiter] ')

if __name__ == '__main__':
	main()

