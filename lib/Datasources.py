
import pudb
import re
import pymysql  # -- for MySQl Errors


import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .MySQLConnector import MySQLConnector



class Datasources():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.temp_db_parameters
		self.tempdb = MySQLConnector(dbconfig)
		self.con = self.tempdb.getConnection()
		self.cur = self.tempdb.getCursor()
		self.db_suffix = self.config.db_suffix
		
	
	def addDatasource(self, datasourcename):
		query = """INSERT INTO `{0}_Datasources` (`data_source_name`)
			VALUES (%s);""".format(self.db_suffix)
		self.cur.execute(query, [datasourcename,])
		self.con.commit()
	
	
	def getDatasourceID(self, datasourcename):
		query = """SELECT `DatasourceID` FROM `{0}_Datasources`
		WHERE `data_source_name` = %s
		;""".format(self.db_suffix)
		
		self.cur.execute(query, [datasourcename,])
		row = self.cur.fetchone()
		if row is not None:
			return row[0]
		else:
			return None
		
	
	
