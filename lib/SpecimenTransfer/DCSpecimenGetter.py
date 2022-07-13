
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCSpecimenGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 100000
		
		self.temptable = "#SpecimenTempTable"
		self.createSpecimenTempTable()
		self.setMaxPage()
		#logger.info("[%s] Get Specimen entries...\n\tNo. specimen found: %i" % (self.data_source_name, self.record_num))
	
	
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM [dbo].[{0}]
		;
		""".format(self.temptable)
		return query
	
	def getPageQuery(self):
		
		# (`DataSourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `taxon_id`, `taxon`, `FamilyCache`, `barcode`, `AccDate`, `AccessionNumber`)
		
		query = """
		SELECT {0} AS [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [NameID],
		[TaxonomicName], [FamilyCache], [Barcode],
		[AccDate], [AccessionNumber], [withhold_flagg]
		FROM [{1}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.datasourceid, self.temptable)
		return query
	

	def createSpecimenTempTable(self):
		""" Query to get IdentificationUnits with connected CollectionSpecimen data"""
		if self.respect_withhold is True:
			withholdclause = """
			AND (
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		
		if self.respect_withhold is False and self.mark_specimen_withhold is True:
			withhold_select = """CASE WHEN (s.[DataWithholdingReason] IS NOT NULL AND s.[DataWithholdingReason] != '')
				OR (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '')
			THEN 1
			ELSE 0
			END AS [withhold_flagg]"""
		
		else:
			withhold_select = """0 AS [withhold_flagg]"""
		
		query = """SELECT DISTINCT
			IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
			CollectionSpecimenID, IdentificationUnitID, FamilyCache, TaxonomicName, NameID, Barcode, RowGUID, AccDate, AccessionNumber, withhold_flagg
			INTO [{0}]
			FROM
			(SELECT i_m.CollectionSpecimenID AS CollectionSpecimenID,
				i_m.IdentificationUnitID,
				iu.FamilyCache,
				LTRIM(RTRIM(replace(replace(replace(replace(replace(replace(replace(i.TaxonomicName,'sp.',''),'Gen.',''),'cf.',''),'?',''),'"',''),'spec. 1',''),'spec.2',''))) AS TaxonomicName,
				CASE WHEN i.NameURI IS NOT NULL AND CHARINDEX('?',i.NameURI)=0 AND CHARINDEX('/',i.NameURI)>0 THEN
						RIGHT(i.NameURI,CHARINDEX('/',REVERSE(i.NameURI))-1)
					ELSE '0' END AS NameID,
					0 AS Barcode,
					CAST(iu.RowGUID AS VARCHAR(255)) as RowGUID,
					s.AccessionDate as AccDate,
					s.AccessionNumber,
					{3}
				FROM IdentificationSequenceMax i_m
				INNER JOIN [{1}] ids_temp 
					ON (ids_temp.CollectionSpecimenID = i_m.CollectionSpecimenID AND ids_temp.IdentificationUnitID = i_m.IdentificationUnitID)
				INNER JOIN Identification i on (i_m.IdentificationSequenceMax=i.IdentificationSequence
					and i_m.IdentificationUnitID=i.IdentificationUnitID
					and i_m.CollectionSpecimenID=i.CollectionSpecimenID)
				INNER JOIN IdentificationUnit iu on (i.IdentificationUnitID=iu.IdentificationUnitID
					and i.CollectionSpecimenID=iu.CollectionSpecimenID)
				INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s on iu.CollectionSpecimenID = s.CollectionSpecimenID
				 -- to prevent that identifications of host taxa are used here, use only the identificationunit that has no relation
				 -- due to the fact that organisms found on another have a relation type but no RelatedUnitID
				 -- really weird
				WHERE iu.RelatedUnitID IS NULL {2}
				GROUP BY i_m.CollectionSpecimenID, i_m.IdentificationUnitID, iu.RowGUID, iu.FamilyCache, i.TaxonomicName, i.NameURI,
					i.ResponsibleName, s.ExternalDatasourceID, s.AccessionDate, s.AccessionNumber, s.DataWithholdingReason, iu.DataWithholdingReason
			) AS t
			WHERE LEN(t.NameID)>0 -- AND t.FamilyCache IS NOT NULL
			ORDER BY CollectionSpecimenID, IdentificationUnitID
			;""".format(self.temptable, self.transfer_ids_temptable, withholdclause, withhold_select)
		
		log_query.info("\nSelect data for _Specimens table:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE stt 
		SET stt.[Barcode] = 1
		FROM [{0}] stt
		INNER JOIN IdentificationUnitAnalysis iua on (
			stt.IdentificationUnitID=iua.IdentificationUnitID 
			AND stt.CollectionSpecimenID = iua.CollectionSpecimenID)
			AND {1}
		;""".format(self.temptable, self.analysis_id_combined_string)
		
		log_query.info("\nUpdate Barcode flag in #SpecimenTempTable:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
		
