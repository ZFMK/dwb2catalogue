
import pudb
import re
import pymysql  # -- for MySQl Errors

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')

from ..MySQLConnector import MySQLConnector



class DBInsert():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.temp_db_parameters
		self.tempdb = MySQLConnector(dbconfig)
		
		self.cur = self.tempdb.getCursor()
		self.con = self.tempdb.getConnection()
		self.db_suffix = self.config.db_suffix
		
		self.valuelists = []
		self.values =[]
		self.placeholderstrings = []
		
		
		self.insertquery = ''
		
		self.pagesize = 1000
		

	
	
	def setPageSize(self, pagesize):
		if int(pagesize) > 0:
			self.pagesize = int(pagesize)
	
	
	def setData(self, dcdata):
		self.valuelists = dcdata
	
	def getNextDataSlice(self):
		cached_data = []
		if len(self.valuelists) <= 0:
			return None
		if len(self.valuelists) >= self.pagesize:
			cached_data = self.valuelists[:self.pagesize]
			del self.valuelists[:self.pagesize]
		else:
			cached_data = self.valuelists[:]
			del self.valuelists[:]
		return cached_data
	
	
	def setValuesFromLists(self, dataslice):
		self.values = []
		for valuelist in dataslice:
			self.values.extend(valuelist)
	
	def setPlaceholderStrings(self, dataslice):
		"""
		extra method to get a list(!) of placeholder strings to be able to combine them with fixed values like NULL in the VALUES lists of the INSERT query
		"""
		self.placeholderstrings = []
		for valuelist in dataslice:
			placeholders = ['%s'] * len(valuelist)
			self.placeholderstrings.append(', '.join(placeholders))
	
	def setPlaceholderString(self, dataslice):
		self.setPlaceholderStrings(dataslice)
		self.placeholderstring = '(' + '), ('.join(self.placeholderstrings) + ')'
	
	
	def insertData(self):
		dataslice = self.getNextDataSlice()
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			self.setInsertQuery()
			self.cur.execute(self.insertquery, self.values)
			self.con.commit()
			dataslice = self.getNextDataSlice()
		
