#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')

import pudb

from ..MySQLConnector import MySQLConnector


class CalculateStatistics():
	def __init__(self, globalconfig):
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		
		self.taxatable = "{0}_Taxa".format(self.db_suffix)
		self.specimentable = "{0}_Specimen".format(self.db_suffix)
		
		logger.info("Calculate statistics")
		self.calc_taxa_stats()
		


	def calc_taxa_stats(self):
		""" subsequently adds the number of known, collected and barcoded specimen up to the root
			called after Specimen data are inserted
		"""
		
		# reset all taxa statistics
		query = """UPDATE {0} 
			SET known=0, 
			collected=0, 
			barcode=0, 
			collected_individuals=0, 
			barcode_individuals=0""".format(self.taxatable)
		self.cur.execute(query)
		self.con.commit()
		
		# initialize the count of known taxa by setting `known` for the species at leaf level to 1
		query = """UPDATE `{0}` t1
			LEFT JOIN `{0}` t2 ON(t1.`id` = t2.parent_id)
			SET t1.known=1
			WHERE t2.parent_id IS NULL
			""".format(self.taxatable)
		self.cur.execute(query)
		self.con.commit()
		
		
		self.cur.execute(self.update_stats_from_specimen())  # -- update leaf taxa
		self.con.commit()
		q = self.get_taxa_stats()
		self.cur.execute(q)
		rows = self.cur.fetchall()
		rownum = len(rows)
		# print('number of rows: ', rownum)
		current_row = 0
		q = self.set_stats()
		for row in rows:
			current_row += 1
			# print ('row number: {0}, number of rows: {1}'.format(current_row, rownum))
			self.cur.execute(q, [row[0], row[1], row[2], row[3], row[4], row[5]])
			self.con.commit()
	
	def update_stats_from_specimen(self):
		return """UPDATE `{0}` t
			LEFT JOIN (SELECT
					taxon_id, 1 AS amount,
					count(taxon_id) AS indivuals
				FROM `{1}`
				GROUP BY taxon_id) AS s_c ON s_c.taxon_id=t.id
			LEFT JOIN (SELECT
					taxon_id, 1 AS amount,
					count(taxon_id) AS indivuals
				FROM `{1}`
				WHERE barcode>0 AND barcode IS NOT NULL
				GROUP BY taxon_id) AS s_b ON s_b.taxon_id=t.id
			SET t.collected = s_c.amount,
				t.barcode = IF(s_b.amount IS NOT NULL, s_b.amount, 0),
				t.`collected_individuals` = s_c.indivuals,
				t.`barcode_individuals` = IF(s_b.indivuals IS NOT NULL, s_b.indivuals, 0)
			WHERE s_c.amount>0""".format(self.taxatable, self.specimentable)
	
	
	def get_taxa_stats(self):
		return """SELECT `id`,  -- 1
				IFNULL(`known`,0) AS known, -- 2
				IFNULL(`collected`,0) AS collected,  -- 3
				IFNULL(`barcode`,0) AS barcode, -- 4
				IFNULL(`collected_individuals`,0) AS collected_individuals, -- 5
				IFNULL(`barcode_individuals`,0) AS barcode_individuals -- 6
			FROM `{0}` WHERE (`known`>0 OR
				`collected`> 0 OR
				`barcode`>0 OR
				`collected_individuals`>0 OR
				`barcode_individuals`>0)""".format(self.taxatable)
	
	def set_stats(self):
		q = """
			UPDATE {0} p
			right join (
				SELECT p.id, p.taxon, p.parent_id,
					p.`known`,
					p.`collected`,
					p.`barcode`,
					p.`collected_individuals`,
					p.`barcode_individuals`
				FROM {0} n, {0} p
				WHERE n.lft BETWEEN p.lft AND p.rgt AND (n.id=%s)
				 -- ORDER BY n.lft DESC
			) n ON n.parent_id=p.id
			SET
				p.`known`=p.`known`+%s,
				p.`collected`=p.`collected`+%s,
				p.`barcode`=p.`barcode`+%s,
				p.`collected_individuals`=p.`collected_individuals`+%s,
				p.`barcode_individuals`=p.`barcode_individuals`+%s
			;
			""".format(self.taxatable)
		return q
	

