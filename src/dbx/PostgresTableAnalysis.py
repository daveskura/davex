"""
  Dave Skura

	Connect to Postgres
	Connect to local sqlite_db
	read table details
	calc metrics 
	load metrics to table in sqlite cache tables
	

"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from schemawizard_package.schemawizard import schemawiz

import logging
import sys
class runner():

	def __init__(self,selected_schema='%',selected_table=''):
		self.metrics_table_name = 'table_metrics'
		self.metrics_tablehdr_name = 'table_comments'
		self.sqlite = sqlite_db()
		
		self.db = postgres_db() 

		self.connect()
		self.build_metrics_table(self.sqlite)
		self.sqlite.execute('DELETE FROM ' + self.metrics_table_name + " WHERE schema_name='" + selected_schema + "' and table_name = '" + selected_table + "'")
		
		comment_sql = "select obj_description('" + selected_schema + '.' + selected_table + "'::regclass);"
		table_comment = self.db.queryone(comment_sql)
		self.table_comment_insert(self.sqlite,selected_schema,selected_table,table_comment)

		col_list_sql = """
			SELECT column_name as field_name,coalesce(col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position),'') as column_comment
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE table_Schema like '""" + selected_schema + """' and upper(table_name) like upper('""" + selected_table + """')
			ORDER BY ordinal_position
		"""
		data = self.db.query(col_list_sql)
		for row in data:
			col_name = row[0]
			col_comment = row[1]
			if col_comment.find("'") > -1:
				col_comment = col_comment.replace("'",'`')

			#-- schema_name, table_name,field_name
			# ,sample_data,distinct_values,indexes,ddl_comments 

			sql = """

				SELECT
				'public' as schemaname,
				'tableowners' as tablename,
				'table_catalog' as field_name,
				sample_data,
				distinct_values,
				indexes,
				(SELECT 'my table comments') as ddl_comments
				FROM 
				(SELECT count(DISTINCT table_catalog) as distinct_values FROM public.tableowners) distinct_counter
				LEFT JOIN (
						SELECT concat('[ ',table_catalog
								,' ][ ', coalesce(LEAD(table_catalog,1) OVER (ORDER BY table_catalog),'') 
								,' ][ ', coalesce(LEAD(table_catalog,2) OVER (ORDER BY table_catalog),'') 
								,' ]') as sample_data 
						FROM 
								(SELECT DISTINCT table_catalog FROM public.tableowners) dist_qry
						ORDER BY table_catalog
						limit 1
						) sampling ON (1=1)
				LEFT JOIN (
					SELECT
							concat(coalesce(indexdef,'')
							,chr(10),coalesce(LEAD(indexdef,1) OVER (ORDER BY indexdef))
							,chr(10),coalesce(LEAD(indexdef,2) OVER (ORDER BY indexdef))
							,chr(10),coalesce(LEAD(indexdef,3) OVER (ORDER BY indexdef))
							,chr(10),coalesce(LEAD(indexdef,4) OVER (ORDER BY indexdef))
							) as indexes
					FROM pg_indexes
									 WHERE schemaname= 'public' 
													and tablename = 'tableowners'
													and indexdef like '%table_catalog%'
					) indexqry ON (1=1)        
	

			"""

			sql = sql.replace('public',selected_schema)
			sql = sql.replace('tableowners',selected_table)
			sql = sql.replace('table_catalog',col_name)
			#print(sql)
			#sys.exit(0)
			metric_data = self.db.query(sql)			
			for onerow in metric_data:
				"""
				print('schema_name		= ', selected_schema)
				print('table_name			= ', selected_table)
				print('field_name			= ', col_name)
				"""
				sample_data		= onerow[3]
				distinct_values= onerow[4]
				indexes				= onerow[5]
				
				self.metric_insert(self.sqlite,selected_schema,selected_table,col_name,sample_data,distinct_values,indexes,col_comment )
			
		print('Analaysis completed. ' + self.metrics_table_name + ' updated with stats from ' + selected_schema + '.' + selected_table)

		self.disconnect()

	def build_metrics_table(self,sqlite):
		if not sqlite.does_table_exist(self.metrics_tablehdr_name):
			logging.info('table ' + self.metrics_tablehdr_name + ' does not exist.  Creating it.')
			csql = "CREATE TABLE " + self.metrics_tablehdr_name + """ (
				schema_name text,
				table_name text,
				table_comments text
				)
			"""
			sqlite.execute(csql)

		if not sqlite.does_table_exist(self.metrics_table_name):
			logging.info('table ' + self.metrics_table_name + ' does not exist.  Creating it.')
			csql = "CREATE TABLE " + self.metrics_table_name + """ (
				schema_name text,
				table_name text,
				field_name text,
				sample_data text,
				distinct_values int,
				indexes text,
				field_comments text
				)
			"""
			sqlite.execute(csql)

	def table_comment_insert(self,sqlite,schema_name,table_name,table_comments):
		dsql = 'DELETE FROM ' + self.metrics_tablehdr_name 
		dsql += " WHERE schema_name ='" + schema_name + "' and table_name='" + table_name + "';"
		sqlite.execute(dsql)

		isql = 'INSERT INTO ' + self.metrics_tablehdr_name + ' (schema_name,table_name,table_comments) VALUES ('
		isql += "'" + schema_name + "',"
		isql += "'" + table_name + "',"
		isql += "'" + table_comments + "');"
		sqlite.execute(isql)

	def metric_insert(self,sqlite,schema_name,table_name,field_name,sample_data,distinct_values,indexes,field_comments ):

		sample_data_cln = sample_data
		if sample_data.find("'") > -1:
			sample_data_cln = sample_data.replace("'",'`')

		dsql = 'DELETE FROM ' + self.metrics_table_name 
		dsql += " WHERE schema_name ='" + schema_name + "' and table_name='" + table_name + "' and field_name='" + field_name + "';"
		sqlite.execute(dsql)

		isql = 'INSERT INTO ' + self.metrics_table_name + ' (schema_name,table_name,field_name,sample_data,distinct_values,indexes,field_comments) VALUES ('
		isql += "'" + schema_name + "',"
		isql += "'" + table_name + "',"
		isql += "'" + field_name + "',"
		isql += "'" + str(sample_data_cln) + "',"
		isql += "" + str(distinct_values) + ","
		isql += "'" + str(indexes) + "',"
		isql += "'" + str(field_comments) + "' "
		isql += ');'
		sqlite.execute(isql)

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

	runner('public','tableowners')
