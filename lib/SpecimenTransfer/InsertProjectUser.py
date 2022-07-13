
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_queries = logging.getLogger('query')


from .DBInsert import DBInsert


class InsertProjectUser(DBInsert):
	def __init__(self, globalconfig, projectusergetter):
		DBInsert.__init__(self, globalconfig)
		self.projectusergetter = projectusergetter
		self.insertProjects()
	
	
	def insertProjects(self):
		dataslice = self.projectusergetter.getNextDataPage()
		
		self.setTempTableQueries()
		self.setInsertQuery()
		
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			
			log_queries.info(self.createtemptable)
			self.cur.execute(self.createtemptable)
			self.con.commit()
			
			self.setTempTableFillQuery()
			log_queries.info(self.filltemptable)
			self.cur.execute(self.filltemptable, self.values)
			self.con.commit()
			
			log_queries.info(self.insertquery)
			self.cur.execute(self.insertquery)
			self.con.commit()
			
			self.cur.execute(self.droptemptable)
			self.con.commit()
			
			dataslice = self.projectusergetter.getNextDataPage()
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_ProjectUser_Temp` (
			`DatasourceID` int(10) NOT NULL,
			`LoginName` VARCHAR(255),
			`ProjectID` int(10) unsigned NOT NULL,
			KEY (`DatasourceID`),
			KEY (`LoginName`),
			KEY (`ProjectID`)
			)
		;""".format(self.db_suffix)
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_ProjectUser_Temp`
		;""".format(self.db_suffix)
	
	
	def setTempTableFillQuery(self):
		self.filltemptable = """INSERT INTO `{0}_ProjectUser_Temp` (`DatasourceID`, `LoginName`, `ProjectID`) VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
	
	
	def setInsertQuery(self):
		self.insertquery = """INSERT INTO `{0}_ProjectUser`
		 (project_id, `DatasourceID`, `LoginName`, `ProjectID`)
			SELECT p.`id`, ct.`DatasourceID`, ct.`LoginName`, ct.`ProjectID`
			FROM `{0}_ProjectUser_Temp` ct
			INNER JOIN `{0}_Project` p ON ct.`DatasourceID` = p.`DatasourceID` AND ct.`ProjectID` = p.`ProjectID`
			GROUP BY p.`id`, ct.`DatasourceID`, ct.`LoginName`, ct.`ProjectID`
		;""".format(self.db_suffix)
	
	
	
	

