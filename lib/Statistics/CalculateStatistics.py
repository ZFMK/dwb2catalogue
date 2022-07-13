
import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')

from DBConnectors.MySQLConnector import MySQLConnector

class CalculateStatistics():
	def __init__(self, globalconfig):
		dbconfig = globalconfig.temp_db_parameters
		self.tempdb = MySQLConnector(dbconfig)
		
		self.con = self.tempdb.getConnection()
		self.cur = self.tempdb.getCursor()
		
		self.db_suffix = globalconfig.db_suffix
		
		#pudb.set_trace()
		
		self.createTaxaRelationTable()
		self.fillTaxaRelationTable()
		self.setLevels()
		self.init_statistics()
		self.set_statistics()
		self.dropLevelColumn()


	def createTaxaRelationTable(self):
		query = """
		DROP TABLE IF EXISTS taxa_relations_temp
		;"""
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE `taxa_relations_temp` (
		`id` INT NOT NULL AUTO_INCREMENT,
		`AncestorTaxonID` INT NOT NULL,
		`DescendantTaxonID` INT NOT NULL,
		`PathLength` INT(10),
		PRIMARY KEY (`id`),
		KEY (`AncestorTaxonID`),
		KEY (`DescendantTaxonID`),
		KEY (`PathLength`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;"""
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def fillTaxaRelationTable(self):
		# fill table with the self referencing relation for each node
		query = """
		INSERT INTO `taxa_relations_temp` (`id`, `AncestorTaxonID`, `DescendantTaxonID`, `PathLength`)
		SELECT NULL, `id`, `id`, 0 FROM `{0}_Taxa`;
		""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pathlength = 0
		count = self.getCountByPathLength(pathlength)
		
		while count > 0:
			#pudb.set_trace()
			# set the parent relations
			logger.info("Calculate statistics: Fill taxa_relation_table pathlength {0}, with {1} possible childs".format(pathlength, count))
			query = """
			INSERT INTO `taxa_relations_temp` (`id`, `AncestorTaxonID`, `DescendantTaxonID`, `PathLength`)
			SELECT NULL, t2.`id`, tr.`DescendantTaxonID`, tr.pathLength + 1
			FROM `taxa_relations_temp` tr
			INNER JOIN `{0}_Taxa` t1 
			ON (
				t1.`id` = tr.`AncestorTaxonID`
			)
			INNER JOIN `{0}_Taxa` t2
			ON(
				t2.`id` = t1.`parent_id`
			)
			WHERE t1.id != t1.parent_id
			AND tr.pathLength = %s
			""".format(self.db_suffix)
			
			log_query.info(query)
			self.cur.execute(query, [pathlength])
			self.con.commit()
			
			pathlength += 1
			count = self.getCountByPathLength(pathlength)
		return


	def setLevels(self):
		query = """
		ALTER TABLE {0}_Taxa add column `level` INT DEFAULT NULL
		;""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}_Taxa` t
		INNER JOIN (
			SELECT `DescendantTaxonID`, MAX(pathlength) AS `level`
			FROM `taxa_relations_temp`
			GROUP BY `DescendantTaxonID`
		) ml 
		ON (ml.`DescendantTaxonID` = t.id)
		SET t.`level` = ml.`level`
		""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def getCountByPathLength(self, pathlength):
		query = """
		SELECT COUNT(*)
		FROM `taxa_relations_temp` tr
		WHERE tr.pathLength = %s
		"""
		
		log_query.info(query)
		self.cur.execute(query, [pathlength])
		row = self.cur.fetchone()
		if row is not None:
			count = row[0]
		else:
			count = 0
		return count


	def getNextLevelCount(self, level):
		query = """
		SELECT COUNT(*)
		FROM `{0}_Taxa` t
		WHERE t.`level` = %s
		;"""
		
		log_query.info(query)
		self.cur.execute(query, [level])
		row = self.cur.fetchone()
		if row is not None:
			count = row[0]
		else:
			count = 0
		return count


	def init_statistics(self):
		
		logger.info("Calculate statistics: init data on Taxa table")
		query = """UPDATE {0}_Taxa 
			SET known=0, 
			collected=0, 
			barcode=0, 
			collected_individuals=0, 
			barcode_individuals=0""".format(self.db_suffix)
		self.cur.execute(query)
		self.con.commit()
		query = """UPDATE `{0}_Taxa`
			SET known=1 WHERE rgt=lft+1 AND rank_code < 180""".format(self.db_suffix)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}_Taxa` t
			LEFT JOIN (SELECT
					taxon_id, 1 AS amount,
					count(taxon_id) AS indivuals
				FROM {0}_Specimen
				GROUP BY taxon_id) AS s_c ON s_c.taxon_id=t.id
			LEFT JOIN (SELECT
					taxon_id, 1 AS amount,
					count(taxon_id) AS indivuals
				FROM {0}_Specimen
				WHERE barcode>0 AND barcode IS NOT NULL
				GROUP BY taxon_id) AS s_b ON s_b.taxon_id=t.id
			SET t.collected = s_c.amount,
				t.barcode = IF(s_b.amount IS NOT NULL, s_b.amount, 0),
				t.`collected_individuals` = s_c.indivuals,
				t.`barcode_individuals` = IF(s_b.indivuals IS NOT NULL, s_b.indivuals, 0)
			WHERE s_c.amount>0
		;""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)  # -- update leaf taxa
		self.con.commit()
		
		return


	def set_statistics(self):
		query = """
		SELECT MAX(`level`)
		FROM `{0}_Taxa`
		;""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is not None:
			max_level = int(row[0])
		else:
			max_level = 0
		
		for level in range (max_level-1, -1, -1):
			query = """
			UPDATE `{0}_Taxa` p
			INNER JOIN (
			SELECT 
				t.id,
				SUM(c.`known`) AS known,
				SUM(c.`collected`) AS collected,
				SUM(c.`barcode`) AS barcode,
				SUM(c.`collected_individuals`) AS collected_individuals,
				SUM(c.`barcode_individuals`) AS barcode_individuals
			FROM 
			`{0}_Taxa` t
			INNER JOIN {0}_Taxa c 
			ON (c.parent_id = t.id)
			GROUP BY c.parent_id
			) csum ON (p.id = csum.id)
			SET 
					p.`known` = p.`known` + csum.`known`,
					p.`collected` = p.`collected` + csum.`collected`,
					p.`barcode` = p.`barcode` + csum.`barcode`,
					p.`collected_individuals` = p.`collected_individuals` + csum.`collected_individuals`,
					p.`barcode_individuals` = p.`barcode_individuals` + csum.`barcode_individuals`
			WHERE p.`level` = %s
			;""".format(self.db_suffix)
			
			logger.info('adding statistics in {0}_Taxa with level {1}'.format(self.db_suffix, level))
			log_query.info(query)
			self.cur.execute(query, [level])
			self.con.commit()
		
		return


	def dropLevelColumn(self):
		query = """
		ALTER TABLE {0}_Taxa drop column `level`
		;""".format(self.db_suffix)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
