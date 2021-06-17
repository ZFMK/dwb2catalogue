#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')

import pudb



class TaxaMatchTable():
	"""
	The class uses the data available for taxa in _Taxa table to compare them with 
	the given taxon names in the data comming from specimen
	"""
	
	
	
	def __init__(self, tempdb_con, globalconfig):
		
		self.config = globalconfig
		
		self.dbcon = tempdb_con
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		self.taxamergerdb = self.config.getTaxaMergerDBName()
		
		self.taxamergetable = "`{0}`.TaxaMergeTable".format(self.taxamergerdb)
		self.taxamergerelationtable = "`{0}`.TaxaMergeRelationTable".format(self.taxamergerdb)
		
		self.synonymsmergetable = "`{0}`.TaxaSynonymsMergeTable".format(self.taxamergerdb)
		self.specimentable = "{0}_Specimen".format(self.db_suffix)
		
		
		self.resetMatched_in_specimen_Flagg()
		


	def resetMatched_in_specimen_Flagg(self):
		query = """
		UPDATE {0}
		SET matched_in_specimens = 0
		;""".format(self.taxamergetable)
		
		self.cur.execute(query)
		self.con.commit()
		


	def createTempTable(self):
		# table that holds a set of specimen data from DC, according to pagesize
		# this is used to compare it against the ZFMK_Coll_Taxa table that contains all taxa extracted from TaxonNames and / or GBIF
		temptablequery = """
		CREATE TEMPORARY TABLE taxonmatcher
		(
		`specimen_id` INT(10),
		`scientificName` varchar(255),
		`taxon_name` varchar(255),
		`genus_name` varchar(255),
		`family_name` varchar(255),
		KEY `specimen_id` (`specimen_id`),
		KEY `scientificName` (`scientificName`),
		KEY `taxon_name` (`taxon_name`),
		KEY `genus_name` (`genus_name`),
		KEY `family_name` (`family_name`)
		)
		;
		"""
		
		self.cur.execute(temptablequery)
		self.con.commit()
		
		
		# all matched taxa of the current set will be stored in matchingresults table 
		# later on they will also be stored in a persistent table taxa_matched that collects all matched taxa
		# not only that of the current set 
		# a table taxa_not_matched collects specimen_ids and taxa that have not been found in the ZFMK_Coll_Taxa table
		resulttablequery = """
		CREATE TEMPORARY TABLE matchingresults
		(
		`specimen_id` INT(10),
		`taxon_id` INT(10),
		`taxon` varchar(255),
		`author` varchar(255),
		`rank` varchar(25),
		KEY `specimen_id` (`specimen_id`)
		)
		;
		"""
		
		self.cur.execute(resulttablequery)
		self.con.commit()
		
		
	def createTaxaNotMatchedTable(self):
		createquery = """
		CREATE TABLE IF NOT EXISTS taxa_not_matched
		(
		`specimen_id` INT(10),
		`scientificName` varchar(255),
		`taxon_name` varchar(255),
		KEY `specimen_id` (`specimen_id`),
		KEY `scientificName` (`scientificName`),
		KEY `taxon_name` (`taxon_name`)
		)
		;
		"""
		self.cur.execute(createquery)
		self.con.commit()
	
	def createTaxaMatchedTable(self):
		createquery = """
		CREATE TABLE IF NOT EXISTS taxa_matched
		(
		`specimen_id` INT(10),
		`taxon_id` INT(10),
		`taxon_name` varchar(255),
		KEY `specimen_id` (`specimen_id`),
		KEY `taxon_id` (taxon_id),
		KEY `taxon_name` (`taxon_name`)
		)
		;
		"""
		self.cur.execute(createquery)
		self.con.commit()
	
	
	def insertIntoTempTable(self, placeholderstring, values):
		insertquery = """
		INSERT INTO taxonmatcher
		(
			`specimen_id`,
			`scientificName`,
			`taxon_name`,
			`genus_name`,
			`family_name`
		)
		VALUES {0}
		;
		""".format(placeholderstring)
		self.cur.execute(insertquery, values)
		self.con.commit()
		return
	
	
	def deleteMatched(self):
		delete_matched_query = """
		DELETE tm FROM taxonmatcher tm 
		INNER JOIN matchingresults r ON(tm.specimen_id = r.specimen_id)
		;
		"""
		self.cur.execute(delete_matched_query)
		self.con.commit()
		return
	
	
	def matchScientificName(self):
		matchquery = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.scientificName = mt.scientificName)
		WHERE tm.scientificName != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(matchquery)
		self.con.commit()
		
		match_log_query = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.scientificName = mt.scientificName)
		WHERE tm.scientificName != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(match_log_query)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchTaxonNameInFamily(self):
		matchquery = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.taxon_name = mt.`taxon` AND tm.family_name = mt.familyCache)
		WHERE tm.taxon_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(matchquery)
		self.con.commit()
		
		
		match_log_query = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.taxon_name = mt.`taxon` AND tm.family_name = mt.familyCache)
		WHERE tm.taxon_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(match_log_query)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchTaxonName(self):
		matchquery_2 = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.taxon_name = mt.`taxon`)
		WHERE tm.taxon_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(matchquery_2)
		self.con.commit()
		
		
		match_log_query_2 = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.taxon_name = mt.`taxon`)
		WHERE tm.taxon_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(match_log_query_2)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchSynonymInFamily(self):
		matchquery_4 = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} st ON (tm.`taxon_name` = st.`taxon`)
		INNER JOIN {1} mt ON (st.syn_taxon_id = mt.id AND tm.family_name = mt.familyCache)
		WHERE tm.taxon_name != ''
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		self.cur.execute(matchquery_4)
		self.con.commit()
		
		match_log_query_4 = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} st ON (tm.`taxon_name` = st.`taxon`)
		INNER JOIN {1} mt ON (st.syn_taxon_id = mt.id AND tm.family_name = mt.familyCache)
		WHERE tm.taxon_name != ''
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		self.cur.execute(match_log_query_4)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchSynonym(self):
		matchquery_4 = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} st ON (tm.`taxon_name` = st.`taxon`)
		INNER JOIN {1} mt ON (st.syn_taxon_id = mt.id)
		WHERE tm.taxon_name != ''
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		self.cur.execute(matchquery_4)
		self.con.commit()
		
		match_log_query_4 = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} st ON (tm.`taxon_name` = st.`taxon`)
		INNER JOIN {1} mt ON (st.syn_taxon_id = mt.id)
		WHERE tm.taxon_name != ''
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		self.cur.execute(match_log_query_4)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchGenusNameInFamily(self):
		matchquery_3 = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.genus_name = mt.`taxon` AND tm.family_name = mt.familyCache)
		WHERE tm.genus_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(matchquery_3)
		self.con.commit()
		
		match_log_query_3 = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.genus_name = mt.`taxon` AND tm.family_name = mt.familyCache)
		WHERE tm.genus_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(match_log_query_3)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchGenusName(self):
		matchquery_3 = """
		INSERT INTO matchingresults
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`, mt.`author`, mt.`rank`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.genus_name = mt.`taxon`)
		WHERE tm.genus_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(matchquery_3)
		self.con.commit()
		
		match_log_query_3 = """
		INSERT INTO taxa_matched
		SELECT tm.specimen_id, mt.`id`, mt.`taxon`
		FROM taxonmatcher tm
		INNER JOIN {0} mt ON (tm.genus_name = mt.`taxon`)
		WHERE tm.genus_name != ''
		;
		""".format(self.taxamergetable)
		self.cur.execute(match_log_query_3)
		self.con.commit()
		
		self.deleteMatched()
		return
	
	
	def matchTaxa(self):
		self.matchScientificName()
		self.matchTaxonNameInFamily()
		self.matchSynonymInFamily()
		self.matchGenusNameInFamily()
		
		self.matchTaxonName()
		self.matchSynonym()
		self.matchGenusName()
		
		store_not_matched_query = """
		INSERT INTO taxa_not_matched
		SELECT specimen_id, scientificName, taxon_name
		FROM taxonmatcher
		;
		"""
		self.cur.execute(store_not_matched_query)
		self.con.commit()
		
		
	def updateTaxonIDsInSpecimens(self):
		query = """
		 -- after matchTaxa method matchingresults holds the ids of specimens for that a taxon have been found
		UPDATE `{0}` s
		INNER JOIN matchingresults mr
			ON (s.id = mr.specimen_id)
		SET s.taxon_id = mr.taxon_id
		;""".format(self.specimentable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		 -- after matchTaxa method taxonmatcher holds the ids of specimens that could not be matched with a taxon
		UPDATE `{0}` s
		INNER JOIN taxonmatcher tm
			ON(s.id = tm.specimen_id)
		SET s.taxon_id = NULL
		;""".format(self.specimentable)
		self.cur.execute(query)
		self.con.commit()
	
	
	def updateTaxonAndAuthorInSpecimens(self):
		query = """
		 -- after matchTaxa method matchingresults holds the ids of specimens for that a taxon have been found
		UPDATE `{0}` s
		INNER JOIN matchingresults mr
			ON (s.id = mr.specimen_id)
		SET s.taxon = mr.taxon, 
		s.author = mr.author
		;""".format(self.specimentable)
		self.cur.execute(query)
		self.con.commit()
	
	
	def markMatchedTaxaInMergeTable(self):
		query = """
		 -- after matchTaxa method matchingresults holds the ids of taxa for that have been found in specimens
		UPDATE {0} mt
		INNER JOIN (
			SELECT DISTINCT `taxon_id`
			FROM matchingresults
		) mr
		ON (mt.id = mr.`taxon_id`)
		SET mt.matched_in_specimens = 1
		""".format(self.taxamergetable)
		self.cur.execute(query)
		self.con.commit()
		
	
	def deleteTempTableData(self):
		deletequery = """
		DELETE FROM taxonmatcher
		;
		"""
		self.cur.execute(deletequery)
		self.con.commit()
		
		deletequery = """
		DELETE FROM matchingresults
		;
		"""
		self.cur.execute(deletequery)
		self.con.commit()
	
	



