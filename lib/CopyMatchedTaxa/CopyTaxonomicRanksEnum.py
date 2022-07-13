
import pudb

import logging
import logging.config
logger = logging.getLogger('sync_webportal')
log_queries = logging.getLogger('query')

from DBConnectors.MySQLConnector import MySQLConnector


class CopyTaxonomicRanksEnum():
	def __init__(self, globalconfig):
	
		#pudb.set_trace()
		
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		self.taxamergerdb = self.config.getTaxaMergerDBName()
		
		self.insertTaxonomicRanksEnum2TargetTable()


	def insertTaxonomicRanksEnum2TargetTable(self):
		
		query = """
		INSERT INTO {0}_TaxonomicRanksEnum
		SELECT `rank`, `rank_code`
		FROM {1}.TaxonomicRanksEnum
		;""".format(self.config.db_suffix, self.taxamergerdb)
		
		log_queries.info(query)
		
		self.cur.execute(query)
		self.con.commit()

