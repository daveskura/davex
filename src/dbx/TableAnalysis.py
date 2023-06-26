"""
  Dave Skura

	Connect to MySQL/Postgres
	Connect to local sqlite_db
	read table details
	calc metrics 
	load metrics to table in sqlite cache tables
	

"""
import logging
import sys
import readchar

from PostgresTableAnalysis import runner as Postgres_runner
from MySQLTableAnalysis import runner as MySQL_runner


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info(" Starting Table Analysis") # 

	print('Which database do you want to analyze ?')
	print('1. Postgres')
	print('2. MySQL')
	selectchar = readchar.readchar()
	if selectchar.upper() == '1':
		Postgres_runner('public','tableowners')
	elif selectchar.upper() == '2':
		MySQL_runner('world','city')
	else:
		sys.exit(0)

