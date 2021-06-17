'''
maintain table Sync_Transfer_Log in web portal, that logs the time, source and table of transfers from DC to web portals database
'''

import logging
import logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')

from .MySQLConnector import MySQLConnector

import pudb


class TransferLog():
	"""
	get timestamp, table, and source of last transfer from web portals database
	set timestamp, table, and source of current transfer in web portals database
	"""

	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.production_db_parameters
		self.prod_db = MySQLConnector(dbconfig)
		
		self.temp_db_name = globalconfig.getTempDBName()
		
		self.cur = self.prod_db.getCursor()
		self.con = self.prod_db.getConnection()
		self.db_suffix = self.config.db_suffix
	
	
	def start(self):
		self.update('Start')
	
	
	def finished(self):
		self.update('finished')


	def update(self, source_name):
		logger.info("update Sync_Transfer_Log for {}".format(source_name))
		no_entries = self.get_inserted_count(source_name)
		
		q1 = """INSERT INTO `Sync_Transfer_Log` (`id`,`transfer_table`,`source_name`,`no_entries`,`date`) VALUES
					(NULL, '{0}', '{1}', {2}, NOW())""".format(self.temp_db_name, source_name, no_entries)
		self.cur.execute(q1)
	
	
	def get_inserted_count(self, source_name):
		if source_name == 'finished':
			query = """
			SELECT COUNT(s.`id`)
			FROM {0}.{1}_Specimen s
			;""".format(self.temp_db_name, self.db_suffix)
			self.cur.execute(query)
			row = self.cur.fetchone()
		
		else:
			query = """
			SELECT COUNT(s.`id`)
			FROM {0}.{1}_Specimen s INNER JOIN {0}.{1}_Datasources ds
			ON (s.DatasourceID = ds.DatasourceID)
			WHERE ds.data_source_name = %s
			;""".format(self.temp_db_name, self.db_suffix)
			log_query.info("{0} {1}".format(query, source_name))
			self.cur.execute(query, [source_name])
			row = self.cur.fetchone()
		
		return int(row[0])
		


	def get_datespan(self, is_reset):
		log_date = False
		if not is_reset:
			q1 = """SELECT a.id, DATE_FORMAT(a.`date`, '%Y-%m-%d %H:%i:%S') as `date` FROM `Sync_Transfer_Log` a INNER JOIN (SELECT MAX(id) as `maxid` FROM `Sync_Transfer_Log`) b ON (b.`maxid` = a.`id`);"""
			self.cur.execute(q1)
			row = self.cur.fetchone()
			log_date = row[1]
		return (log_date, self.timestamp_start)
	
	
	
