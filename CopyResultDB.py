import pudb

import logging
import logging.config


logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')

from DBConnectors import MySQLConnector, MSSQLConnector
from DBConnectors.MySQLStruct import MySQLStruct

from TempDB import CreateTableQueries


class CopyResultDB():
	def __init__(self, globalconfig):
		self.config = globalconfig



	def copy_db(self):
		temp_db_params = self.config.temp_db_parameters
		prod_db_params = self.config.production_db_parameters
		
		temp_db = MySQLStruct(config = temp_db_params)
		temp_con = temp_db.getConnection()
		temp_cur = temp_db.getCursor()
		
		prod_db = MySQLStruct(config = prod_db_params)
		prod_con = prod_db.getConnection()
		prod_cur = prod_db.getCursor()
		
		# get a criterion when to copy
		query = """SELECT COUNT(*) FROM {0}_Specimen;""".format(self.config.db_suffix)
		temp_cur.execute(query)
		row = temp_cur.fetchone()
		temp_sp_num = row[0]
		
		try:
			prod_cur.execute(query)
			row = prod_cur.fetchone()
			prod_sp_num = row[0]
		except:
			prod_sp_num = 0
		
		# only copy the database when number of new specimens is at least 90% of the old ones
		#if temp_sp_num > (prod_sp_num - int(prod_sp_num / 10)):
		# currently: always copy
		if True:
			logger.info('Copying temp database to production started')
			logger.info('Replacing {0} specimen with {1} specimen'.format(prod_sp_num, temp_sp_num))
			
			tables = temp_db.getTableNames()
			tables2copy = []
			for table in tables:
				if table.startswith(self.config.db_suffix):
					tables2copy.append(table)
			
			sorted_tables = temp_db.getTableNamesSortedByForeignKeys(tables2copy)
			
			sorted_tables.extend(['taxa_matched', 'taxa_not_matched'])
			
			reversed_tables = reversed(sorted_tables)
			
			for table in reversed_tables:
				logger.info('Dropping table {0} of production database'.format(table))
				query = """DROP TABLE `{0}`.`{1}`;""".format(prod_db.databasename, table)
				try:
					prod_cur.execute(query)
					prod_con.commit()
				except:
					pass
			
			tablequeries = CreateTableQueries(self.config)
			for tablename, query_function in tablequeries.iter_tables_query():
				prod_cur.execute(query_function())
				prod_con.commit()
			
			for table in sorted_tables:
				logger.info('Copying data into table {0} of production database'.format(table))
				query = """INSERT INTO `{0}`.`{1}` SELECT * FROM `{2}`.`{1}`;""".format(prod_db.databasename, table, temp_db.databasename)
				log_query.info(query)
				
				prod_cur.execute(query)
				prod_con.commit()
			
			logger.info('Copying temp database to production finished')
			temp_db.closeConnection()
			prod_db.closeConnection()
			return True
		
		else:
			return False
