
import pudb
import re
import pymysql  # -- for MySQl Errors

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertSpecimen(DBInsert):
	def __init__(self, globalconfig, specimengetter):
		DBInsert.__init__(self, globalconfig)
		self.specimengetter = specimengetter
		
		# prevent the insertion of double entries with the same CollectionSpecimenID and AccessionNumber when the Specimens are copied between Projects?
		self.preventdoubles = True
		
		
		if self.preventdoubles is True:
			self.insertSpecimensPreventDoubles()
		else:
			self.insertSpecimens()
		
		
		
	
	def insertSpecimens(self):
		self.data = self.specimengetter.getNextDataPage()
		
		while self.data is not None:
			self.setPlaceholderString(self.data)
			self.setValuesFromLists(self.data)
			
			query = """
			INSERT INTO `{0}_Specimen` (`DataSourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `taxon_id`, `taxon`, `FamilyCache`, `barcode`, `AccDate`, `AccessionNumber`, `withhold_flagg`)
			VALUES {1}
			;""".format(self.config.db_suffix, self.placeholderstring)
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.data = self.specimengetter.getNextDataPage()
	
	
	
	def insertSpecimensPreventDoubles(self):
		query = """CREATE TEMPORARY TABLE 
		`specimentemptable`
		SELECT * FROM 
		`{0}_Specimen`
		LIMIT 0
		;""".format(self.config.db_suffix)
		
		self.cur.execute(query)
		self.con.commit()
		
		self.data = self.specimengetter.getNextDataPage()
		
		while self.data is not None:
			self.setPlaceholderString(self.data)
			self.setValuesFromLists(self.data)
			
			query = """
			INSERT INTO `specimentemptable` (`DataSourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `taxon_id`, `taxon`, `FamilyCache`, `barcode`, `AccDate`, `AccessionNumber`, `withhold_flagg`)
			VALUES {0}
			;""".format(self.placeholderstring)
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.data = self.specimengetter.getNextDataPage()
		
		query = """
		INSERT INTO `{0}_Specimen` (`DataSourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `taxon_id`, `taxon`, `FamilyCache`, `barcode`, `AccDate`, `AccessionNumber`, `withhold_flagg`)
		SELECT st.`DataSourceID`, st.`CollectionSpecimenID`, st.`IdentificationUnitID`, st.`taxon_id`, st.`taxon`, st.`FamilyCache`, st.`barcode`, st.`AccDate`, st.`AccessionNumber`, st.`withhold_flagg`
		FROM `specimentemptable` st 
		LEFT JOIN `{0}_Specimen` s2 ON (st.`CollectionSpecimenID` = s2.`CollectionSpecimenID` AND st.`AccessionNumber` = s2.`AccessionNumber`)
		WHERE s2.CollectionSpecimenID IS NULL
		;""".format(self.config.db_suffix)
		
		self.cur.execute(query)
		self.con.commit()
		
		# update the withhold flagg when data comming from different sources and the last source has less restrictions
		query = """
		 -- update the withhold_flaggs for specimen from zfmk and gbol. gbol will always have withhold_flagg 0 and the lesser restriction is what counts
		UPDATE `{0}_Specimen` cs
		INNER JOIN `specimentemptable` st
		ON (st.`CollectionSpecimenID` = cs.`CollectionSpecimenID` AND st.`AccessionNumber` = cs.`AccessionNumber`)
		set cs.withhold_flagg = 0
		WHERE st.withhold_flagg = 0
		;""".format(self.config.db_suffix)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
		
	

