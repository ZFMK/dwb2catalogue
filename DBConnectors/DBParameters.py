from DBConnectors import MySQLConnector
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')

class DBParameters():
	def __init__(self, conf_section = None):
		self.conf_section = conf_section
		
		self.config = ConfigParser()
		self.config.read('config.ini')
		
		self.readConnectionParams()
		
		
	def connect(self):
		self.dbcon = MySQLConnector(self.db_parameters)
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
	
	def close(self):
		self.dbcon.closeConnection()

	def readConnectionParams(self):
		self.db_parameters = {}
		self.db_parameters['host'] = self.config[self.conf_section].get('host', 'localhost')
		self.db_parameters['user'] = self.config[self.conf_section].get('user', None)
		self.db_parameters['user_host'] = self.config[self.conf_section].get('user_host', None)
		self.db_parameters['root_pw'] = self.config[self.conf_section].get('root_pw', None)
		self.db_parameters['passwd'] = self.config[self.conf_section].get('passwd', None)
		self.db_parameters['port'] = self.config[self.conf_section].get('port', 3306)
		self.db_parameters['db'] = self.config[self.conf_section].get('db', 'gbol-python')
		self.db_parameters['charset'] = self.config[self.conf_section].get('charset', None)
		

