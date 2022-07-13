
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertProject(DBInsert):
	def __init__(self, globalconfig, projectgetter):
		DBInsert.__init__(self, globalconfig)
		self.projectgetter = projectgetter
		self.insertProjects()
	
	
	def insertProjects(self):
		dataslice = self.projectgetter.getNextDataPage()
		
		self.setTempTableQueries()
		self.setInsertQuery()
		
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			
			self.cur.execute(self.createtemptable)
			self.con.commit()
			
			self.setTempTableFillQuery()
			self.cur.execute(self.filltemptable, self.values)
			self.con.commit()
			
			self.cur.execute(self.insertquery)
			self.con.commit()
			
			self.cur.execute(self.droptemptable)
			self.con.commit()
			
			dataslice = self.projectgetter.getNextDataPage()
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_Project_Temp` (
			`DatasourceID` int(10) NOT NULL,
			`ProjectID` int(10) unsigned NOT NULL,
			`Project` VARCHAR(50),
			`ProjectURI` VARCHAR(255),
			KEY (`DatasourceID`),
			KEY (`ProjectID`)
			)
		;""".format(self.db_suffix)
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_Project_Temp`
		;""".format(self.db_suffix)
	
	
	def setTempTableFillQuery(self):
		self.filltemptable = """INSERT INTO `{0}_Project_Temp` (`DatasourceID`, `ProjectID`, `Project`, `ProjectURI`) VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
	
	
	def setInsertQuery(self):
		self.insertquery = """INSERT INTO `{0}_Project`
		 (`DatasourceID`, `ProjectID`, `Project`, `ProjectURI`)
			SELECT ct.`DatasourceID`, ct.`ProjectID`, ct.`Project`, ct.`ProjectURI`
			FROM `{0}_Project_Temp` ct
			GROUP BY ct.`DatasourceID`, ct.`ProjectID`, ct.`Project`, ct.`ProjectURI`
		;""".format(self.db_suffix)
	
	
	
	

