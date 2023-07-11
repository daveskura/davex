"""
  Dave Skura
  
"""
from sqlitedave_package.sqlitedave import sqlite_db 

from querychart_package.querychart import charter
import sys

print (" Starting ") # 
cache_prefix='demo_'
selected_schema='atlas'
selected_table='onetableforme'

def grapharooyou():
	chartingapp = charter()
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
		WHERE A.schema_name='""" + selected_schema + "' AND	A.table_name='" + selected_table + """'
		"""
	db = sqlite_db()
	data = db.query(sql)
	#total_rowcount = data.rowcount 
	#total_colcount = data.colcount
	#total_column_names = data.column_names
	chartingapp.csv_querybarchart(sql,'',title,xlabel,ylabel,8)
	

grapharooyou()