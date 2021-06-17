#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')

import pudb

from ..MySQLConnector import MySQLConnector


class CopyCommonNamesToTargetTable():
	def __init__(self, globalconfig):
	
		#pudb.set_trace()
		
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		self.taxamergerdb = self.config.getTaxaMergerDBName()
		
		self.insertCommonNames2TargetTable()
			
		


	def insertCommonNames2TargetTable(self):
		
		query = """
			INSERT INTO `{0}_TaxaCommonNames`
			(`taxon_id`, `name`, `code`, `db_name`)
			SELECT mt.id, tc.`name`, tc.`code`, tc.`db_name`
			FROM {1}.TaxaMergeTable mt
			INNER JOIN {1}.TaxaCommonNamesTempTable tc
			ON(
				tc.SourceTaxonID = mt.SourceTaxonID
				AND tc.TaxonomySourceID = mt.TaxonomySourceID
				AND tc.SourceProjectID = mt.SourceProjectID
			)
			 -- select the common name that was first inserted into the TaxaCommonNamesTempTable to prevent multiple results from that only one is shown in the portal
			INNER JOIN (
				SELECT MIN(tc2.id) as min_id FROM {1}.TaxaCommonNamesTempTable tc2
				GROUP BY tc2.SourceTaxonID, tc2.SourceProjectID, tc2.TaxonomySourceID, tc2.`code`
			) tc2
			ON (tc.id = tc2.min_id)
			 -- limit the common names to english and german common names
			WHERE `code` IN ('de', 'en')
			ORDER BY tc.id
		;""".format(self.config.db_suffix, self.taxamergerdb)
			
		self.cur.execute(query)
		self.con.commit()


