
import pudb
import re
import pymysql  # -- for MySQl Errors


import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .MySQLConnector import MySQLConnector



class TaxonomySources():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.temp_db_parameters
		self.tempdb = MySQLConnector(dbconfig)
		self.con = self.tempdb.getConnection()
		self.cur = self.tempdb.getCursor()
		self.db_suffix = self.config.db_suffix
		
	
	def addTaxonomySource(self, taxonomysourcename):
		query = """INSERT INTO `{0}_TaxonomySources` (`taxonomy_source_name`)
			VALUES (%s);""".format(self.db_suffix)
		self.cur.execute(query, [taxonomysourcename,])
		self.con.commit()
	
	
	def getTaxonomySourceID(self, taxonomysourcename):
		query = """SELECT `TaxonomySourceID` FROM `{0}_TaxonomySources`
		WHERE `taxonomy_source_name` = %s
		;""".format(self.db_suffix)
		
		self.cur.execute(query, [taxonomysourcename,])
		row = self.cur.fetchone()
		if row is not None:
			return row[0]
		else:
			return None
		
	
	
