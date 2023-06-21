"""
  Dave Skura, Dec,2022
"""
from sqlitedave_package.sqlitedave import sqlite_db
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 

import logging
import sys
import os

def main():
	if len(sys.argv) == 1 or sys.argv[1] == 'run.py': # no parameters
		logging.info('')
		logging.info('usage: ')
		logging.info('py -m dbx.run [etl_name] ') 
		logging.info(' ')
		logging.info('py -m dbx.postgres_import [csv_filename] [tablename] [WithTruncate]') 
		logging.info('py -m dbx.postgres_export [tablename] [csvfilename] [delimiter] ') 
		logging.info(' ')
		logging.info('py -m dbx.mysql_import [csv_filename] [tablename] [WithTruncate]') 
		logging.info('py -m dbx.mysql_export [tablename] [csvfilename] [delimiter] ') 
		logging.info('-----------')

	sys.exit(0)

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info(" Starting dbx.run ") # 
	main()

		


