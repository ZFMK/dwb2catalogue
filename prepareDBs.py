import pudb

from DBConnectors.DBParameters import DBParameters
from DBConnectors import MySQLConnector



class DBPreparation():
	def __init__(self):
		# pudb.set_trace()
		
		self.taxamerger_db = DBParameters(conf_section = 'taxamergerdb')
		self.catalog_db = DBParameters(conf_section = 'catalog_db')
		
		self.create_DBs()
		
		self.setRights()


	
	def create_DBs(self):
		try:
			if self.catalog_db.db_parameters['root_pw'] is not None:
				self.root_dbcon = MySQLConnector(self.catalog_db.db_parameters, user='root', passwd=self.catalog_db.db_parameters['root_pw'])
			else:
				#not yet working waiting for idea of how to connect via auth_socket plugin
				self.root_dbcon = MySQLConnector(self.catalog_db.db_parameters, user='root', host='localhost')
		
			cur = self.root_dbcon.getCursor()
			con = self.root_dbcon.getConnection()
			
			'''
			query = """
			DROP DATABASE IF EXISTS `{0}` 
			;""".format(self.catalog_db.db_parameters['db'])
			
			cur.execute(query)
			con.commit()
			'''
			
			query = """
			CREATE DATABASE IF NOT EXISTS `{0}` 
			;""".format(self.catalog_db.db_parameters['db'])
			
			cur.execute(query)
			con.commit()
			
			query = """
			CREATE TABLE IF NOT EXISTS `Sync_Transfer_Log` (
			  `id` int unsigned NOT NULL AUTO_INCREMENT,
			  `transfer_table` varchar(100) DEFAULT NULL,
			  `source_name` varchar(100) DEFAULT NULL,
			  `no_entries` int unsigned DEFAULT NULL,
			  `date` datetime DEFAULT NULL,
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
			"""
			cur.execute(query)
			con.commit()
			
			'''
			query = """
			DROP DATABASE IF EXISTS `{0}` 
			;""".format(self.taxamerger_db.db_parameters['db'])
			
			cur.execute(query)
			con.commit()
			'''
			
			query = """
			CREATE DATABASE IF NOT EXISTS `{0}` 
			;""".format(self.taxamerger_db.db_parameters['db'])
			
			cur.execute(query)
			con.commit()
			self.root_dbcon.closeConnection()
		
		except:
			print("""Could not connect to database as root / sudo user, check config parameters""")
			raise
	
	
	def setRights(self):
		try:
			if self.catalog_db.db_parameters['root_pw'] is not None:
				self.root_dbcon = MySQLConnector(self.catalog_db.db_parameters, user='root', passwd=self.catalog_db.db_parameters['root_pw'])
			else:
				self.root_dbcon = MySQLConnector(self.catalog_db.db_parameters, user='root')
		
			cur = self.root_dbcon.getCursor()
			con = self.root_dbcon.getConnection()
			
			if self.catalog_db.db_parameters['user_host'] is not None:
				query = """
				GRANT ALL ON `{0}`.* TO '{1}'@'{2}'
				;""".format(self.catalog_db.db_parameters['db'], self.catalog_db.db_parameters['user'], self.catalog_db.db_parameters['user_host'])
			
			else:
				query = """
				GRANT ALL ON `{0}`.* TO '{1}'
				;""".format(self.catalog_db.db_parameters['db'], self.catalog_db.db_parameters['user'])
			
			cur.execute(query)
			con.commit()
			
			if self.catalog_db.db_parameters['user_host'] is not None:
				query = """GRANT ALL PRIVILEGES ON `Transfer2Catalog\_%`.* TO '{1}'@'{2}'
				;""".format(self.catalog_db.db_parameters['db'], self.catalog_db.db_parameters['user'], self.catalog_db.db_parameters['user_host'])
			
			else:
				query = """GRANT ALL PRIVILEGES ON `Transfer2Catalog\_%`.* TO '{1}'
				;""".format(self.catalog_db.db_parameters['db'], self.catalog_db.db_parameters['user'])
			
			cur.execute(query)
			con.commit()
			
			query = """FLUSH PRIVILEGES;"""
			
			cur.execute(query)
			con.commit()
			
			self.root_dbcon.closeConnection()
		
		except:
			print("""Could not connect to database as root / sudo user, check config parameters""")
			raise
	




if __name__ == "__main__":
	# pudb.set_trace()
	dbpreparation = DBPreparation()
