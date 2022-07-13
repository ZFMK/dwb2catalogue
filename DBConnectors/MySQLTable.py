# -*- coding: utf8 -*-
"""
Class to read table structure
Provides an object describing the table structure
Author: Björn Quast b.quast@zfmk.de
CC By 4.0
"""

import pymysql
import warnings
import traceback
import time
import sys
import re
import pudb
import logging

log = logging.getLogger(__name__)

#from .sql_debug import debug_print
# import package and use absolute names to adress MySQLStruct needed to make circular import possible here
# see: https://stackoverflow.com/questions/7336802/how-to-avoid-circular-imports-in-python
# import mysql_tools

class MySQLTable():
	def __init__(self, database, tablename, createtemptableobj = False):
		self.tablename = tablename
		# check that database is a MySQLStruct object
		#if not isinstance(database, mysql_tools.MySQLStruct):
		#	raise TypeError("MySQLTable(): Parameter database must be a MySQLStruct object")
		self.con = database.con
		self.cur = database.cur

		self.databasename = database.databasename
		self.database = database
		self.columns = []
		# self.columnnames = [] is checked by try in getAllColumnNames
		
		if createtemptableobj is True:
			# columns must be set explicitely (by setTempTableColumns) because they can not be read from table information schema (at least in mysql)
			# MySQLStruct.addTempTable does it
			pass
		else:
			# check if table is in database (table names are read by MySQLStruct):
			if tablename in self.database.temptablenames:
				# self.columns and self.columnnames have been set before when the temporary table was added
				self.columns = self.readColumns()
				# keys can not be read
				#self.readForeignKeys()
				#self.readPrimaryKeys()
				pass
			elif tablename in self.database.tablenames:
				self.columns = self.readColumns()
				self.readForeignKeys()
				self.readPrimaryKeys()
			else:
				raise ValueError ('Table with name {0} does not exist in database {1}'.format(tablename, self.database.databasename))
	
	def setTempTableColumns(self, columndicts = None):
		"""
		add columns to temporary table object
		takes over given column dictionaries, obtained from the table the temporary table is build from
		"""
		self.columns = []
		self.columnnames = []
		if isinstance(columndicts, list):
			for columndict in columndicts:
				if isinstance(columndict, dict):
					self.columns.append(columndict)
					self.columnnames.append(columndict['colname'])
				else:
					raise ValueError ('MySQLTable.setTempTableColumns: column is not a dictionary')
		else:
			raise ValueError ('MySQLTable.setTempTableColumns: a list of column dictionaries must be provided by parameter columns')
		return self
	
	
	def readColumns(self):
		sql_getcolumns = """DESCRIBE `{0}`.`{1}`""".format(self.databasename, self.tablename)
		# the following does not work with temporary tables therefore I prefer to use pattern matching to get the type and length
		#sql_getcolumns = """select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, EXTRA from information_schema.columns
		#	where TABLE_SCHEMA = '{0}' and TABLE_NAME = '{1}'""".format(self.databasename, self.tablename)
		cur = self.cur
		# debug_print (sql_getcolumns, caller = 'sql_getcolumns: ')
		cur.execute(sql_getcolumns)
		rows = cur.fetchall()
		self.columns = []
		self.columnnames =[]
		for row in rows:
			column = {}
			column['colname'] = row[0]
			column['coldef'] = row[1]
			# print (row[1])
			mgroups = re.match(r"(\w+)(\((\d+)\))?\s*(\w+)?", row[1])
			matches = mgroups.groups() # returns a tuple of matches
			# print(matches)
			column['coltype'] = matches[0] # is this ever used? yes for casts in MySQLTable.formatSelectColumns / MySQLTable.formatInsertColumns
			column['collength'] = matches[2]
			column['coltype_spec'] = matches[3]
			column['extra'] = row[5]
			# 0 and 1 corresponds to MSSQL here
			if row[2] == 'YES':
				column['nullable'] = 1
			else:
				column['nullable'] = 0
			# column['colalias'] = self.tablename + "_" + row[0]
			column['alias'] = None
			self.columns.append(column)
			self.columnnames.append(column['colname'])
		return self.columns

	def readPrimaryKeys(self):
		'''search for primary keys
		'''
		sql_getprimarykeys = """
			select c.COLUMN_NAME, c.COLUMN_KEY, c.COLUMN_TYPE, 
			u.CONSTRAINT_NAME, u.ORDINAL_POSITION
			from information_schema.columns c 
			inner join information_schema.key_column_usage u using (TABLE_SCHEMA, COLUMN_NAME, TABLE_NAME)
			where TABLE_SCHEMA = '{0}' and TABLE_NAME = '{1}' and u.CONSTRAINT_NAME = 'PRIMARY'
			ORDER BY CONSTRAINT_NAME, u.ORDINAL_POSITION
			""".format(self.databasename, self.tablename)
		cur = self.cur
		cur.execute(sql_getprimarykeys)
		rows = cur.fetchall()
		self.primarykeys = []
		for row in rows:
			primarykey = {}
			primarykey['index_name'] = row[3]
			primarykey['primary_key'] = row[0]
			primarykey['ordinal_position'] = row[4]
			mgroups = re.match(r"(\w+)(\((\d+)\))?\s*(\w+)?", row[2])
			matches = mgroups.groups() # returns a tuple of matches
			primarykey['coltype'] = matches[0]
			primarykey['collength'] = matches[2]
			primarykey['coltype_spec'] = matches[3]
			self.primarykeys.append(primarykey)
		return self.primarykeys

	def readForeignKeys(self):
		'''search for foreign keys'''
		sql_getforeignkeys = """
			select c.COLUMN_NAME, c.COLUMN_KEY, c.COLUMN_TYPE, 
			u.REFERENCED_TABLE_SCHEMA, u.CONSTRAINT_NAME, u.REFERENCED_TABLE_NAME, u.REFERENCED_COLUMN_NAME, u.ORDINAL_POSITION
			from information_schema.columns c
			inner join information_schema.key_column_usage u using (TABLE_SCHEMA, COLUMN_NAME, TABLE_NAME)
			where TABLE_SCHEMA = '{0}' and TABLE_NAME = '{1}' and u.REFERENCED_TABLE_NAME IS NOT NULL
			ORDER BY CONSTRAINT_NAME, u.ORDINAL_POSITION
			""".format(self.databasename, self.tablename)
		cur = self.cur
		cur.execute(sql_getforeignkeys)
		rows = cur.fetchall()
		self.foreignkeys = []
		for row in rows:
			foreignkey = {}
			foreignkey['foreignkey_name'] = row[4]
			foreignkey['foreign_key'] = row[0]
			foreignkey['referenced_database'] = row[3]
			foreignkey['referenced_table'] = row[5]
			foreignkey['referenced_column'] = row[6]
			foreignkey['ordinal_position'] = row[7]
			mgroups = re.match(r"(\w+)(\((\d+)\))?\s*(\w+)?", row[2])
			matches = mgroups.groups() # returns a tuple of matches
			foreignkey['coltype'] = matches[0]
			foreignkey['collength'] = matches[2]
			foreignkey['coltype_spec'] = matches[3]
			self.foreignkeys.append(foreignkey)
		return self.foreignkeys

	def printTableProperties(self, showdatatype = False):
		colnames = []
		for column in self.getColumns():
			if showdatatype is True:
				colnames.append(column['colname'] + ', type: ' + column['coltype'])
			else:
				colnames.append(column['colname'])
		colstring = ",\n\t\t\t". join(colnames)
		pknames = self.getPrimaryKeyNames()
		pkstring = ', '.join(pknames)
		fks = []
		for fk in self.getForeignKeys():
			fktext = "{0}, references: {1}({2})".format(fk['foreign_key'], fk['referenced_table'], fk['referenced_column'])
			fks.append(fktext)
		fkstring = ",\n\t\t\t". join(fks)
		tableproperties = ("{0}\n\tcolumns:\t{1}\n\tprimary keys:\t{2}\n\tforeign keys:\t{3}".format(self.getTableName(), colstring, pkstring, fkstring))
		print (tableproperties)


	def setColumnAlias(self, column, alias):
		columndict = self.getColumnDict(column)
		columndict['alias'] = alias

	def formatSelectColumns(self, columns = None, tableprefix = True):
		'''wrapping column names with casts and converts for select queries'''
		self.selectcols = []
		if columns is None:
			# get all columns in table
			columns = self.getColumns()
		if tableprefix is True:
			tprefix = "`" + self.tablename + "`."
		elif tableprefix is False:
			tprefix = ''
		else:
			tprefix = tableprefix + "."
		for column in columns:
			if column['alias'] is not None:
				colaliasstring = " AS `{0}`".format(column['alias'])
			else:
				colaliasstring = ""
			# add cast and convert statements here
			if column['coltype'] == 'datetime':
				self.selectcols.append(tprefix + "`" + column['colname'] + "`" + colaliasstring) #'convert(varchar, {0}, 120),25) as {0}'.format(column['colname'])
			else:
				self.selectcols.append(tprefix + "`" + column['colname'] + "`" + colaliasstring)
		return self.selectcols


	def formatInsertColumns(self, columns = None):
		'''wrapping column names with casts and converts for insert queries'''
		self.insertcols = []
		if columns is None:
			# get all columns in table
			columns = self.columns
		for column in columns:
			# add cast and convert statements here
			if column['coltype'] == 'datetime':
				self.insertcols.append("`" + column['colname'] + "`") #'convert(varchar, {0}, 120),25) as {0}'.format(column['colname'])
			else:
				self.insertcols.append("`" + column['colname'] + "`")
		return self.insertcols


	def getTableName(self):
		if isinstance(self, MySQLTable):
			return self.tablename

	def getColumns(self):
		try: 
			self.columns
		except AttributeError:
			self.readColumns()
		return self.columns

	def getAllColumnNames(self):
		try:
			return self.columnnames
		except AttributeError:
			self.__setAllColumnNames()
			return self.columnnames
		
	def __setAllColumnNames(self):
		try:
			self.columnnames
		except AttributeError:
			self.readColumns() # self.columnnames are set by self.readColumns()
		return self

	def checkColumnsExist(self, columns):
		exists = True
		for column in columns:
			exists = self.checkColumnExists(column)
			# return if one of the columns does not exist
			if exists is False:
				return False
		return exists
		
	def checkColumnExists(self, column):
		try:
			columnname = self.getColumnName(column)
			return True
		except ValueError: 
			return False

	def getColumnNamesByColumns(self, columns):
		columnnames = []
		for column in columns:
			columnname = self.getColumnName(column)
			columnnames.append(columnname)
		return columnnames

	def getColumnsByNames(self, colnames):
		columndicts = []
		for colname in colnames:
			column = self.getColumnByName(colname)
			columndicts.append(column)
		return columndicts

	def getColumnByName(self, colname):
		try:
			self.columnnames
		except AttributeError:
			self.getAllColumnNames()
		if isinstance(colname, dict): # if column dict was given instead of column name
			if colname['colname'] not in self.columnnames:
				self.readColumns()
			for column in self.columns:
				if column['colname'] == colname['colname']:
					return colname
			# return None # copy_mssql.py checks for existence by getColumn, not sure if error is needed by other scripts
			raise ValueError ('MySQLTable.getColumnByName: column with name {0} can not be found in table {1}'.format(column['colname'], self.tablename))
		elif isinstance(colname, str):
			if colname not in self.columnnames:
				self.readColumns()
			for column in self.columns:
				if column['colname'] == colname:
					return column
			# return None
			raise ValueError ('MySQLTable.getColumnByName: column with name {0} can not be found in table {1}'.format(colname, self.tablename))
		else:
			raise ValueError ('MySQLTable.getColumnByName: parameter colname must be a dict or string instance')

	def getColumnName(self, column):
		if isinstance(column, dict):
			if column['colname'] not in self.columnnames:
				self.readColumns()
			if column['colname'] in self.columnnames:
				return column['colname']
			else:
				raise ValueError('column with name {0} does not exist'.format(column))
		elif isinstance(column, str):
			checkedcolumn = self.getColumnByName(column)
			# getColumnByName(column) raises an error when the column name could not be found
			#if checkedcolumn is None:
			#	raise ValueError('column with name {0} does not exist'.format(column))
			return checkedcolumn['colname']
		else:
			raise ValueError ('MySQLTable.getColumnName: parameter colname must be a dict or string instance')

	def getColumnDict(self, colname):
		return self.getColumnByName(colname)


	def getPrimaryKeys(self):
		try:
			self.primarykeys
		except AttributeError:
			self.readPrimaryKeys()
		return self.primarykeys


	def getPrimaryKeyNames(self):
		try:
			return self.primarykeynames
		except AttributeError:
			self.__setPrimaryKeyNames()
			return self.primarykeynames
		
	def __setPrimaryKeyNames(self):
		try:
			self.primarykeys
		except AttributeError:
			self.readPrimaryKeys()
		self.primarykeynames = []
		for pk in self.primarykeys:
			self.primarykeynames.append(pk['primary_key'])
		return self

	def getPrimaryKeyByColumn(self, columnname):
		primarykeys = self.getPrimaryKeys()
		columnname = self.getColumnName(columnname)
		for pk in primarykeys:
			if pk['primary_key'] == columnname:
				return pk
		return None

	def getForeignKeys(self):
		try:
			self.foreignkeys
		except AttributeError:
			self.readForeignKeys()
		return self.foreignkeys

	def getForeignKeyNames(self):
		try:
			return self.foreignkeynames
		except AttributeError:
			self.__setForeignKeyNames()
			return self.foreignkeynames

	def __setForeignKeyNames(self):
		try:
			self.foreignkeys
		except AttributeError:
			self.foreignkeys = self.readForeignKeys()
		self.foreignkeynames = []
		for fk in self.foreignkeys:
			self.foreignkeynames.append(fk['foreign_key'])
		return self

	def isForeignKey (self, columnname):
		fknames = self.getForeignKeyNames()
		columnname = self.getColumnName(columnname)
		if columnname in fknames:
			return True
		else:
			return False
	
	def getForeignKeyByColumn(self, columnname):
		foreignkeys = self.getForeignKeys()
		columnname = self.getColumnName(columnname)
		for fk in foreignkeys:
			if fk['foreign_key'] == columnname:
				return fk
		return None

	def getForeignKeyByReference(self, reftable, refcolumn):
		reftableobj = self.database.getTableByName(reftable)
		foreignkeys = self.getForeignKeys()
		reftablename = reftableobj.getTableName()
		refcolname = reftableobj.getColumnName(refcolumn)
		for fk in foreignkeys:
			if (fk['referenced_table'], fk['referenced_column']) == (reftablename, refcolname):
				return fk
		return None

	def hasAutoIncrement(self, column):
		columndict = self.getColumnByName(column)
		if columndict['extra'] == 'auto_increment':
			return True
		else:
			return False
			

	def getDatabase(self):
		'''return parent MYSQLStruct-Object''' 
		return self.database


