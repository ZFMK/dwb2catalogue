#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')

import pudb

from ..MySQLConnector import MySQLConnector


class SpecimenCompleteness():
	def __init__(self, globalconfig):
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		logger.info("Calculate specimen completeness")
		self.setSpecimenCompleteness()
		
	
	
	def setSpecimenCompleteness(self):
		# fill the table ZFMK_Coll_SpecimenCompleteness that counts how many of the available data fields are filled for a specimen
		logger.info("set_specimen_completeness")
		self.set_specimen_completeness()
		
		logger.info("update_specimen_completeness")
		self.update_specimen_completeness()
		
		logger.info("add_specimen_completeness_level")
		self.add_specimen_completeness_level()
	
	
	def set_specimen_completeness(self):
		"""
		count how many of the terms in ZFK_Coll_Data are filled for a specimen
		"""
		query = """
		INSERT INTO `{0}_SpecimenCompleteness`
		SELECT NULL,
		d2s.specimen_id, COUNT(DISTINCT d.term) as filled_terms, 0
		FROM {0}_Data d INNER JOIN {0}_Data2Specimen d2s
		ON(d.id = d2s.data_id)
		GROUP BY d2s.specimen_id
		ORDER BY filled_terms, d2s.specimen_id
		;
		""".format(self.db_suffix)
		log_query.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
	
	def update_specimen_completeness(self):
		"""
		add a count for media, barcode and geo fileds to the completeness level of a specimen
		"""
		query = """
		UPDATE `{0}_SpecimenCompleteness` sc1
		INNER JOIN (
		SELECT sc3.specimen_id,
		IF (COUNT(g.`specimen_id`) > 0, 1, 0) as geo_count, 
		IF (COUNT(b.`specimen_id`) > 0, 1, 0) as barcode_count, 
		IF (COUNT(m.`specimen_id`) > 0, 1, 0) as media_count
		FROM `{0}_SpecimenCompleteness` sc3
		LEFT JOIN `{0}_Geo` g ON(sc3.specimen_id = g.specimen_id)
		LEFT JOIN `{0}_Barcode` b ON(sc3.specimen_id = b.specimen_id)
		LEFT JOIN `{0}_Media` m ON(sc3.specimen_id = m.specimen_id)
		GROUP BY sc3.specimen_id
		) sc2 ON (sc1.specimen_id = sc2.specimen_id) 
		SET sc1.filled_terms = sc1.filled_terms + sc2.geo_count + sc2.barcode_count + sc2.media_count
		;
		""".format(self.db_suffix)
		log_query.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
	
	def add_specimen_completeness_level(self):
		"""
		add a percent value in 10 percent steps as completeness level
		"""
		
		query = """
		CREATE TEMPORARY TABLE `temp_SpecimenCompleteness` (
			`specimen_id` int(10) unsigned NOT NULL,
			`completeness` decimal(15,0) DEFAULT NULL,
			PRIMARY KEY(`specimen_id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		"""
		log_query.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO temp_SpecimenCompleteness (specimen_id, completeness)
		SELECT specimen_id, ROUND(sc_i.filled_terms/mc.maxfilled*10) * 10 as completeness FROM `{0}_SpecimenCompleteness` sc_i,
		(SELECT max(filled_terms) as maxfilled FROM `{0}_SpecimenCompleteness`) AS mc
		""".format(self.db_suffix)
		log_query.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		UPDATE `{0}_SpecimenCompleteness` sc
		INNER JOIN temp_SpecimenCompleteness tsc
		ON(sc.specimen_id = tsc.specimen_id)
		SET sc.grouped_percent = tsc.completeness
		;
		""".format(self.db_suffix)
		log_query.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return

