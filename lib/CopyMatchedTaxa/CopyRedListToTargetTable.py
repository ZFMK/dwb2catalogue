#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')
log_queries = logging.getLogger('query')

import pudb

from ..MySQLConnector import MySQLConnector


class CopyRedListToTargetTable():
	def __init__(self, globalconfig):
	
		#pudb.set_trace()
		
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		self.taxamergerdb = self.config.getTaxaMergerDBName()
		
		self.insertRedList2TargetTable()


	def insertRedList2TargetTable(self):
		
		query = """
		INSERT INTO {0}_TaxaPropertyTerms
		SELECT DISTINCT NULL, trm.`term`, 'rl_category', 'de'
		FROM {1}.TaxaRedListTempTable trm
		LEFT JOIN {0}_TaxaPropertyTerms tp 
		ON (
			trm.`term` = tp.`term` AND tp.`category` = 'rl_category' AND tp.`lang` = 'de'
		)
		WHERE tp.`id` IS NULL
		;""".format(self.config.db_suffix, self.taxamergerdb)
		
		log_queries.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		
		query = """
		INSERT INTO {0}_TaxaPropertyTerms
		SELECT DISTINCT NULL, trm.`reference`, 'rl_reference', 'de'
		FROM {1}.TaxaRedListTempTable trm
		LEFT JOIN {0}_TaxaPropertyTerms tp 
		ON (
			trm.`reference` = tp.`term` AND tp.`category` = 'rl_reference' AND tp.`lang` = 'de'
		)
		WHERE tp.`id` IS NULL
		;""".format(self.config.db_suffix, self.taxamergerdb)
		
		log_queries.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		'''
		query = """
			INSERT INTO `{0}_TaxaRedLists`
			(`taxon_id`, `value`, `category_id`, `reference_id`)
			SELECT mt.id, trm.value, tp1.id, tp2.id
			FROM {1}.TaxaMergeTable mt
			INNER JOIN {1}.TaxaRedListTempTable trm
			ON(
				trm.SourceTaxonID = mt.SourceTaxonID
				AND trm.TaxonomySourceID = mt.TaxonomySourceID
			)
			INNER JOIN `{0}_TaxaPropertyTerms` tp1 ON(
				trm.term = tp1.term
				AND tp1.category = 'rl_category'
				AND tp1.lang = 'de'
			)
			INNER JOIN `{0}_TaxaPropertyTerms` tp2 ON(
				trm.reference = tp2.term
				AND tp2.category = 'rl_reference'
				AND tp2.lang = 'de'
			)
			INNER JOIN `{0}_Taxa` t ON(
				mt.id = t.id
			)
		;""".format(self.config.db_suffix, self.taxamergerdb)
		'''
		
		query = """
			INSERT INTO `{0}_TaxaRedLists`
			(`taxon_id`, `value`, `category_id`, `reference_id`)
			SELECT mt.id, trm.value, tp1.id, tp2.id
			FROM {1}.TaxaMergeTable mt
			INNER JOIN {1}.TaxaRedListTempTable trm
			ON(
				trm.taxon_id = mt.id
			)
			INNER JOIN `{0}_TaxaPropertyTerms` tp1 ON(
				trm.term = tp1.term
				AND tp1.category = 'rl_category'
				AND tp1.lang = 'de'
			)
			INNER JOIN `{0}_TaxaPropertyTerms` tp2 ON(
				trm.reference = tp2.term
				AND tp2.category = 'rl_reference'
				AND tp2.lang = 'de'
			)
			INNER JOIN `{0}_Taxa` t ON(
				mt.id = t.id
			)
		;""".format(self.config.db_suffix, self.taxamergerdb)
		
		
		log_queries.info(query)
		
		self.cur.execute(query)
		self.con.commit()


