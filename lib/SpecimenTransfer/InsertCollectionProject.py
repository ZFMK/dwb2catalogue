
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertCollectionProject(DBInsert):
	def __init__(self, globalconfig, collectionprojectgetter):
		DBInsert.__init__(self, globalconfig)
		self.collectionprojectgetter = collectionprojectgetter
		self.insertProjects()
	
	
	def insertProjects(self):
		dataslice = self.collectionprojectgetter.getNextDataPage()
		
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
			
			dataslice = self.collectionprojectgetter.getNextDataPage()
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_CollectionProject_Temp` (
			`DatasourceID` int(10) NOT NULL,
			`CollectionSpecimenID` int(10) NOT NULL,
			`IdentificationUnitID` int(10) NOT NULL,
			`ProjectID` INT NOT NULL,
			KEY (`DatasourceID`),
			KEY (`CollectionSpecimenID`),
			KEY (`IdentificationUnitID`),
			KEY (`ProjectID`)
			)
		;""".format(self.db_suffix)
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_CollectionProject_Temp`
		;""".format(self.db_suffix)
	
	
	def setTempTableFillQuery(self):
		self.filltemptable = """INSERT INTO `{0}_CollectionProject_Temp` (`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `ProjectID`) VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
	
	
	def setInsertQuery(self):
		self.insertquery = """INSERT INTO `{0}_CollectionProject`
		 (`specimen_id`, `ProjectID`, `DatasourceID`)
			SELECT s.`id`, ct.`ProjectID`, ct.`DatasourceID`
			FROM `{0}_CollectionProject_Temp` ct
			INNER JOIN `{0}_Specimen` s ON ((ct.DatasourceID = s.DatasourceID) 
			AND (ct.`CollectionSpecimenID` = s.`CollectionSpecimenID`)
			AND (ct.`IdentificationUnitID` = s.`IdentificationUnitID`))
			GROUP BY s.`id`, ct.`ProjectID`, ct.`DatasourceID`
		;""".format(self.db_suffix)
	
	
	
	

