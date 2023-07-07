"""
  Dave Skura
  
"""
import logging
from querychart_package.querychart import charter

def main(cache_prefix='',selected_schema='',selected_table=''):
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

	charter().csv_querybarchart(sql,'',title,xlabel,ylabel)

	
if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info(" Start Charting ") # 
	main('demo_','world','city')


