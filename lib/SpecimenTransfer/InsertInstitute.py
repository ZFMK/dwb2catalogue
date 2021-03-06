
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')


from .DBInsert import DBInsert


class InsertInstitute(DBInsert):
	def __init__(self, globalconfig, institutegetter):
		DBInsert.__init__(self, globalconfig)
		self.institutegetter = institutegetter
		
		self.insertInstitutes()


	def insertInstitutes(self):
		dataslice = self.institutegetter.getNextDataPage()
		
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			
			self.createTempTable()
			self.fillTempTable()
			self.copyInstitutes()
			self.updateSpecimens()
			self.dropTempTable()
			
			dataslice = self.institutegetter.getNextDataPage()
		
		return


	def createTempTable(self):
		query ="""CREATE TEMPORARY TABLE `{0}_Institutes_Temp` (
			`DatasourceID` int(10) NOT NULL,
			`CollectionSpecimenID` int(10) NOT NULL,
			`IdentificationUnitID` int(10) NOT NULL,
			`ExternalDatasourceID` int(10) NOT NULL,
			`project_institute` varchar(255) NOT NULL,
			`project_name` varchar(255) NULL,
			`institute_short` varchar(80) NULL,
			`institute_name` varchar(255) NULL,
			KEY (`DatasourceID`),
			KEY (`CollectionSpecimenID`),
			KEY (`IdentificationUnitID`),
			KEY (`ExternalDatasourceID`)
			)
		;""".format(self.db_suffix)
		
		self.cur.execute(query)
		self.con.commit()


	def dropTempTable(self):
		query = """DROP TEMPORARY Table `{0}_Institutes_Temp`
		;""".format(self.db_suffix)
		
		self.cur.execute(query)
		self.con.commit()


	def fillTempTable(self):
		query = """INSERT INTO `{0}_Institutes_Temp` 
		(`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, 
		`ExternalDatasourceID`, `project_institute`, `project_name`, `institute_short`, `institute_name`)
		VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
		
		
		self.cur.execute(query, self.values)
		self.con.commit()


	def copyInstitutes(self):
		"""
		only copy institutes from temp table that have not been inserted before
		"""
		
		query = """
		INSERT INTO `{0}_Institutes`
		(`DatasourceID`,`ExternalDatasourceID`, `project_institute`, `project_name`, `institute_short`, `institute_name`)
		SELECT DISTINCT itemp.`DatasourceID`, itemp.`ExternalDatasourceID`, itemp.`project_institute`, itemp.`project_name`, itemp.`institute_short`, itemp.`institute_name`
		FROM `{0}_Institutes_Temp` itemp
		LEFT JOIN `{0}_Institutes` i
		ON (i.`DatasourceID` = itemp.`DatasourceID` AND i.`ExternalDatasourceID` = itemp.`ExternalDatasourceID`)
		WHERE i.institute_id IS NULL
		;""".format(self.db_suffix)
		
		self.cur.execute(query)
		self.con.commit()


	def updateSpecimens(self):
		query = """
		UPDATE `{0}_Specimen` cs
		INNER JOIN `{0}_Institutes_Temp` it
		ON (
			cs.DatasourceID = it.DatasourceID
			AND cs.`CollectionSpecimenID` = it.`CollectionSpecimenID`
			AND cs.`IdentificationUnitID` = it.`IdentificationUnitID`
			)
		INNER JOIN `{0}_Institutes` i
		ON (
			i.`ExternalDatasourceID` = it.ExternalDatasourceID
			AND i.DatasourceID = it.DatasourceID
		)
		SET cs.`institute_id` = i.`institute_id`
		;""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()

