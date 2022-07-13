'''
maintain table Sync_Transfer_Log in web portal, that logs the time, source and table of transfers from DC to web portals database
'''

import logging
import logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')

from DBConnectors.MySQLConnector import MySQLConnector

import pudb


class TransferLog():
	"""
	get timestamp, table, and source of last transfer from web portals database
	set timestamp, table, and source of current transfer in web portals database
	"""

	def __init__(self, globalconfig):
		self.config = globalconfig
		self.dbconfig = self.config.production_db_parameters
		self.temp_db_name = globalconfig.getTempDBName()
		
		self.db_suffix = self.config.db_suffix
	
	
	def start(self):
		self.update('Start')
	
	
	def finished(self):
		self.update('finished')


	def update(self, source_name):
		prod_db = MySQLConnector(self.dbconfig)
		self.cur = prod_db.getCursor()
		self.con = prod_db.getConnection()
		logger.info("update Sync_Transfer_Log for {}".format(source_name))
		no_entries = self.get_inserted_count(prod_db, source_name)
		
		q1 = """INSERT INTO `Sync_Transfer_Log` (`id`,`transfer_table`,`source_name`,`no_entries`,`date`) VALUES
					(NULL, '{0}', '{1}', {2}, NOW())""".format(self.temp_db_name, source_name, no_entries)
		self.cur.execute(q1)
		self.cur.close()
		self.con.close()
		del prod_db
	
	
	def get_inserted_count(self, db_con, source_name):
		cur = db_con.getCursor()
		con = db_con.getConnection()
		if source_name == 'finished':
			query = """
			SELECT COUNT(s.`id`)
			FROM {0}.{1}_Specimen s
			;""".format(self.temp_db_name, self.db_suffix)
			cur.execute(query)
			row = cur.fetchone()
		
		else:
			query = """
			SELECT COUNT(s.`id`)
			FROM {0}.{1}_Specimen s INNER JOIN {0}.{1}_Datasources ds
			ON (s.DatasourceID = ds.DatasourceID)
			WHERE ds.data_source_name = %s
			;""".format(self.temp_db_name, self.db_suffix)
			log_query.info("{0} {1}".format(query, source_name))
			cur.execute(query, [source_name])
			row = cur.fetchone()
		
		return int(row[0])

