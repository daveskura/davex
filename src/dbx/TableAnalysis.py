"""
  Dave Skura

	Connect to MySQL/Postgres
	Connect to local sqlite_db
	read table details
	calc metrics 
	load metrics to table in sqlite cache tables
	

"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from mysqldave_package.mysqldave import mysql_db 
from schemawizard_package.schemawizard import schemawiz

import logging
import sys
import readchar


from enum import Enum

class dbtype(Enum):
	nodb		= 0
	Postgres= 1
	MySQL		= 2
	SQLite	= 3

class runner():
	def build_metrics_table(self,sqlite):

		if not sqlite.does_table_exist(self.metrics_table_name):
			logging.info('table ' + self.metrics_table_name + ' does not exist.  Creating it.')
			csql = "CREATE TABLE " + self.metrics_table_name + """ (
				schema_name text,
				table_name text,
				field_name text,
				sample_data text,
				distinct_values int,
				indexes text,
				ddl_comments text
				)
			"""
			sqlite.execute(csql)
		
	def metric_insert(self,sqlite,schema_name,table_name,field_name,sample_data,distinct_values,indexes,ddl_comments ):
		isql = 'INSERT INTO ' + self.metrics_table_name + ' (schema_name,table_name,field_name,sample_data,distinct_values,indexes,ddl_comments) VALUES ('
		isql += "'" + schema_name + "',"
		isql += "'" + table_name + "',"
		isql += "'" + field_name + "',"
		isql += "'" + sample_data + "',"
		isql += "" + distinct_values + ","
		isql += "'" + indexes + "',"
		isql += "'" + ddl_comments + "' "
		isql += ');'
		sqlite.execute(isql)

	def __init__(self,databasetype=dbtype.nodb,selected_schema='%',selected_table=''):
		self.metrics_table_name = 'table_metrics'
		self.sqlite = sqlite_db()
		self.db = None # postgres_db() or mysql_db()

		if databasetype == dbtype.nodb:
			print('Which database do you want to analyze ?')
			print('1. Postgres')
			print('2. MySQL')
			selectchar = readchar.readchar()
			if selectchar.upper() == '1':
				databasetype = dbtype.Postgres
			elif selectchar.upper() == '2':
				databasetype = dbtype.MySQL
			else:
				sys.exit(0)

		if databasetype == dbtype.Postgres:
			self.db = postgres_db()
		elif databasetype == dbtype.MySQL:
			self.db = mysql_db()
		else:
			sys.exit(0)

		self.connect()
		self.build_metrics_table(self.sqlite)
		self.sqlite.execute('DELETE FROM ' + self.metrics_table_name + " WHERE schema_name='" + selected_schema + "' and table_name = '" + selected_table + "'")

		col_list_sql = """
			SELECT column_name as field_name
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE table_Schema like '""" + selected_schema + """' and upper(table_name) like upper('""" + selected_table + """')
			ORDER BY ordinal_position
		"""
		data = self.db.query(col_list_sql)
		for row in data:
			col_name = row[0]

			#-- schema_name, table_name,field_name
			# ,sample_data,distinct_values,indexes,ddl_comments 
			sql = """
			SELECT 
					(SELECT """ + col_name + """
					FROM """ + selected_schema + """.""" + selected_table + """
					WHERE """ + col_name + """ is not null
					limit 1) as sample_data,
					(SELECT count(DISTINCT """ + col_name + """)
					FROM """ + selected_schema + """.""" + selected_table + """) as distinct_values,
					(SELECT index_qry.indexes) as indexes,
					(SELECT '') as ddl_comments
			FROM ( 
						 SELECT 
								concat(coalesce(A.indexdef,''),chr(10)
												,coalesce(B.indexdef,''),chr(10)
												,coalesce(C.indexdef,''),chr(10)
												,coalesce(D.indexdef,'')) as indexes
						 FROM 
								(SELECT tablename,indexdef FROM (SELECT tablename,rank() OVER (ORDER BY indexdef) rnk,indexdef
								 FROM pg_indexes
								 WHERE schemaname= '""" + selected_schema + """' and tablename = '""" + selected_table + """'
								 	and indexref like '%""" + col_name + """%'
								) AA WHERE rnk=1) A LEFT JOIN
								(SELECT tablename,indexdef FROM (SELECT tablename,rank() OVER (ORDER BY indexdef) rnk,indexdef
								 FROM pg_indexes
								 WHERE schemaname= '""" + selected_schema + """' and tablename = '""" + selected_table + """'
								 and indexref like '%""" + col_name + """%'
								) BB WHERE rnk=2) B ON (A.tablename = B.tablename) LEFT JOIN
								(SELECT tablename,indexdef FROM (SELECT tablename,rank() OVER (ORDER BY indexdef) rnk,indexdef
								 FROM pg_indexes
								 WHERE schemaname= '""" + selected_schema + """' and tablename = '""" + selected_table + """'
								 and indexref like '%""" + col_name + """%'
								) CC WHERE rnk=3) C ON (A.tablename = C.tablename) LEFT JOIN
								(SELECT tablename,indexdef FROM (SELECT tablename,rank() OVER (ORDER BY indexdef) rnk,indexdef
								 FROM pg_indexes
								 WHERE schemaname= '""" + selected_schema + """' and tablename = '""" + selected_table + """'
								 and indexref like '%""" + col_name + """%'
								) DD WHERE rnk=4) D ON (A.tablename = D.tablename)
					) index_qry    
			"""
			metric_data = self.db.query(sql)			
			for onerow in metric_data:
				print('schema_name		= ', selected_schema)
				print('table_name			= ', selected_table)
				print('field_name			= ', col_name)
				print('sample_data		= ', onerow[0])
				print('distinct_values= ', onerow[1])
				print('indexes				= ', onerow[2])
				print('ddl_comments 	= ', onerow[3])
				break

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
	logging.info(" Starting Table Analysis") # 

	runner(dbtype.Postgres,'public','tableowners')
