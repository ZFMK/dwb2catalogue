#!/usr/bin/env python
# -*- coding: utf8 -*-

import logging
import logging.config
logger = logging.getLogger('sync_webportal')

import pudb

from DBConnectors.MySQLConnector import MySQLConnector


class FlatTaxaTable():
	def __init__(self, globalconfig):
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		
		self.produce_flat_table()
		


	def produce_flat_table(self):
		"""
		insert all taxon_ids from specimen into _taxa
		loop over sp., subfam., ..., kingdom:
		select taxon as <loop-var>, parent as <loop-var>_id where id=parent_id group by tax_id => parent_ids
		update all taxon_ids with parent_ids, use prepare statements
		"""
		logger.info('Generate flat taxon table')
		logger.debug("  - create stored procedure `flat_taxon_table`")
		self.cur.execute(self.sql_flat_tax_table_outer_proc())
		self.con.commit()
		logger.debug("  - create stored procedure `set_flat_taxon_table`")
		self.cur.execute(self.sql_flat_tax_table_inner_proc())
		self.con.commit()
		logger.debug("  - call proc")
		self.cur.callproc('flat_taxon_table')
		self.con.commit()
		logger.info('Generate flat taxon table end')
		
		
	
	
	def sql_flat_tax_table_outer_proc(self):
		q = """CREATE PROCEDURE flat_taxon_table()
			BEGIN
				DECLARE _code, done INT DEFAULT 0;
				DECLARE _name VARCHAR(200);
				DECLARE cur_o CURSOR FOR
					SELECT rank_code, rank_name FROM _ranks order by rank_code;
				DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

				DROP TEMPORARY TABLE IF EXISTS _ranks;
				CREATE TEMPORARY TABLE _ranks(rank_code INT NOT NULL, rank_name VARCHAR(200));
				INSERT INTO _ranks (rank_code, rank_name) VALUES (0, 'tax_species');
				INSERT INTO _ranks (rank_code, rank_name)
				SELECT rank_code,
					CASE WHEN rank_code=260 THEN 'tax_subgenus' ELSE -- 1
						CASE WHEN rank_code=270 THEN 'tax_genus' ELSE -- 2
							CASE WHEN rank_code=280 THEN 'tax_infratribe' ELSE -- 3
								CASE WHEN rank_code=290 THEN 'tax_subtribe' ELSE -- 4
									CASE WHEN rank_code=300 THEN 'tax_tribe' ELSE -- 5
										CASE WHEN rank_code=310 THEN 'tax_supertribe' ELSE -- 6
											CASE WHEN rank_code=320 THEN 'tax_infrafamily' ELSE -- 7
												CASE WHEN rank_code=330 THEN 'tax_subfamily' ELSE -- 8
													CASE WHEN rank_code=340 THEN 'tax_family' ELSE -- 9
														CASE WHEN rank_code=350 THEN 'tax_superfamily' ELSE -- 10
															CASE WHEN rank_code=360 THEN 'tax_infraorder' ELSE -- 11
																CASE WHEN rank_code=370 THEN 'tax_suborder' ELSE -- 12
																	CASE WHEN rank_code=380 THEN 'tax_order' ELSE -- 13
																		CASE WHEN rank_code=390 THEN 'tax_superorder' ELSE -- 14
																			CASE WHEN rank_code=400 THEN 'tax_infraclass' ELSE -- 15
																				CASE WHEN rank_code=410 THEN 'tax_subclass' ELSE -- 16
																					CASE WHEN rank_code=420 THEN 'tax_class' ELSE -- 17
																						CASE WHEN rank_code=430 THEN 'tax_superclass' ELSE -- 18
																							CASE WHEN rank_code=440 THEN 'tax_infraphylum' ELSE -- 19
																								CASE WHEN rank_code=450 THEN 'tax_subphylum' ELSE -- 20
																									CASE WHEN rank_code=460 THEN 'tax_phylum' ELSE -- 21
																										CASE WHEN rank_code=470 THEN 'tax_superphylum' ELSE -- 22
																											CASE WHEN rank_code=480 THEN 'tax_infrakingdom' ELSE -- 23
																												CASE WHEN rank_code=490 THEN 'tax_subkingdom' ELSE -- 24
																													CASE WHEN rank_code=500 THEN 'tax_kingdom' ELSE -- 25
																													'tax_species'
					END END END END END END END END END END END END END END END END END END END END END END END END END AS rank_name
				FROM {0}_Taxa WHERE rank_code>160 AND rank_code<530 GROUP BY rank_code;
				TRUNCATE {0}_TaxaFlat;
				INSERT INTO {0}_TaxaFlat (taxon_id)
					SELECT id FROM {0}_Taxa GROUP BY id ORDER BY id; -- changed from {0}_Specimen to {0}_Taxa, to get taxa that are not in specimens
				OPEN cur_o;
				WHILE NOT done DO
					FETCH cur_o INTO _code, _name;
					IF NOT done THEN
						call set_flat_taxon_table(_code, _name);
					END IF;
				END WHILE;
				CLOSE cur_o;
				/*ALTER TABLE `{0}_TaxaFlat`
					DROP COLUMN `tax_kingdom_parent_id`,
					DROP COLUMN `tax_phylum_parent_id`,
					DROP COLUMN `tax_subphylum_parent_id`,
					DROP COLUMN `tax_class_parent_id`,
					DROP COLUMN `tax_order_parent_id`,
					DROP COLUMN `tax_family_parent_id`,
					DROP COLUMN `tax_subfamily_parent_id`,
					DROP COLUMN `tax_species_parent_id`;*/
			END""".format(self.db_suffix)
		return q

	def sql_flat_tax_table_inner_proc(self):
		q = """CREATE PROCEDURE set_flat_taxon_table(IN start_code INT, tax_rank_name VARCHAR(200))
		BEGIN
			DECLARE _code, done INT DEFAULT 0;
			DECLARE _col_name VARCHAR(200);
			DECLARE cur_i CURSOR FOR
				SELECT rank_code, rank_name as col_name FROM _ranks WHERE rank_code>start_code and rank_code>0 ORDER BY rank_code;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
			IF tax_rank_name='tax_species' and start_code=0 THEN
				SET tax_rank_name='taxon';
			ELSE
				SET tax_rank_name=CONCAT(tax_rank_name,'_parent');
			END IF;
			OPEN cur_i;
			WHILE NOT done DO
				FETCH cur_i INTO _code, _col_name;
				IF NOT done THEN
					SET @sql = CONCAT('update {0}_TaxaFlat _t left join {0}_Taxa t on _t.', tax_rank_name, '_id=t.id ',
								'set ', _col_name, '=t.taxon, ',
								_col_name, '_parent_id=t.parent_id ');
					CASE WHEN _col_name='tax_species' THEN
						SET @sql = CONCAT(@sql, 'where t.rank_code<180');
					ELSE
						SET @sql = CONCAT(@sql, 'where t.rank_code=', _code);
					END CASE;
					PREPARE stmt FROM @sql;
					EXECUTE stmt;
					DROP PREPARE stmt;
				END IF;
			END WHILE;
			CLOSE cur_i;
		END""".format(self.db_suffix)
		return q
	
	
	
