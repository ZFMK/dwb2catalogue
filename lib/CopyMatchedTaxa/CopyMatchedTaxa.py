#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')

import pudb

from DBConnectors.MySQLConnector import MySQLConnector


class CopyMatchedTaxa():
	"""
	Copy taxa matched in TaxaMergeTable to Taxa table
	"""
	
	def __init__(self, globalconfig):
		
		#pudb.set_trace()
		
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		
		self.taxamergerdb = self.config.getTaxaMergerDBName()
		self.taxamergetable = "{0}.TaxaMergeTable".format(self.taxamergerdb)
		
		self.taxatargettable = "{0}_Taxa".format(self.db_suffix)
		self.synonymsmergetable = "{0}.TaxaSynonymsMergeTable".format(self.taxamergerdb)
		self.synonymstargettable = "{0}_TaxaSynonyms".format(self.db_suffix)
	
		self.markParentTaxa()
		self.copyMatchedTaxa()
		self.copySynonyms()
	
	
	
	
	def copyMatchedTaxa(self):
		query = """
		INSERT INTO `{0}` (`id`, `taxon`, `author`, `rank`, `parent_id`, `rank_code`, `scientificName`, `matched_in_specimens`)
		SELECT DISTINCT `id`, `taxon`, `author`, `rank`, `parent_id`, `rank_code`, `scientificName`, `matched_in_specimens`
		FROM {1}
		WHERE `matched_in_specimens` = 1
		""".format(self.taxatargettable, self.taxamergetable)
		self.cur.execute(query)
		self.con.commit()
		
		
	def copySynonyms(self):
		query = """
		INSERT INTO `{0}` (`taxon_id`, `syn_taxon_id`, `taxon`, `author`, `rank_code`)
		SELECT DISTINCT sm.`id`, mt.`id`, sm.`taxon`, sm.`author`, mt.`rank_code`
		FROM {1} sm
		INNER JOIN {2} mt ON(
			sm.SourceAcceptedTaxonID = mt.SourceTaxonID
			AND sm.TaxonomySourceID = mt.TaxonomySourceID
			AND sm.SourceProjectID = mt.SourceProjectID
		)
		WHERE mt.`matched_in_specimens` = 1 AND sm.`taxon` != '' AND sm.`taxon` IS NOT NULL
		""".format(self.synonymstargettable, self.synonymsmergetable, self.taxamergetable)
		self.cur.execute(query)
		self.con.commit()
	
	
	def countNextParentTaxa(self):
		query = """
		SELECT COUNT(mt2.`id`) FROM {0} mt1
		INNER JOIN {0} mt2 ON(
			mt1.`parent_id` = mt2.`id`
		)
		WHERE mt1.matched_in_specimens = 1 AND mt2.matched_in_specimens = 0
		;""".format(self.taxamergetable)
		
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is not None:
			count = row[0]
		else:
			count = 0
		return count
		
	
	def markParentTaxa(self):
		taxanum = self.countNextParentTaxa()
		
		while taxanum != 0:
			query = """
			UPDATE {0} mt2
			INNER JOIN {0} mt1 ON(
				mt1.`parent_id` = mt2.`id`
			)
			SET mt2.matched_in_specimens = 1
			WHERE mt1.matched_in_specimens = 1 AND mt2.matched_in_specimens = 0
			;""".format(self.taxamergetable)
			
			self.cur.execute(query)
			self.con.commit()
			taxanum = self.countNextParentTaxa()
	
	
	
