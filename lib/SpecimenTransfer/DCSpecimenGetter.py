
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCSpecimenGetter(DCGetter):
	def __init__(self, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, data_source_name, globalconfig)
		self.institute_id = self.datasource.institute_id
		self.datasourceid = datasourceid
		
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
		
		# (`DataSourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `taxon_id`, `taxon`, `FamilyCache`, `institute_id`, `barcode`, `AccDate`, `AccessionNumber`)
		
		query = """
		SELECT {0} AS [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [NameID],
		[TaxonomicName], [FamilyCache], [Barcode],
		[AccDate], [AccessionNumber]
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
		
		sql = ["""SELECT DISTINCT
			IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
			CollectionSpecimenID, IdentificationUnitID, FamilyCache, TaxonomicName, NameID, Barcode, RowGUID, AccDate, AccessionNumber
			INTO [{0}]
			FROM
			(SELECT i_m.CollectionSpecimenID AS CollectionSpecimenID,
				i_m.IdentificationUnitID,
				iu.FamilyCache,
				LTRIM(RTRIM(replace(replace(replace(replace(replace(replace(replace(i.TaxonomicName,'sp.',''),'Gen.',''),'cf.',''),'?',''),'"',''),'spec. 1',''),'spec.2',''))) AS TaxonomicName,
				CASE WHEN i.NameURI IS NOT NULL AND CHARINDEX('?',i.NameURI)=0 AND CHARINDEX('/',i.NameURI)>0 THEN
						RIGHT(i.NameURI,CHARINDEX('/',REVERSE(i.NameURI))-1)
					ELSE '0' END AS NameID,""".format(self.temptable), ]
		
		sql.append("""CASE WHEN iua.AnalysisID IS NULL THEN 0 ELSE 1 END AS Barcode,
			CAST(iu.RowGUID AS VARCHAR(255)) as RowGUID,
			s.AccessionDate as AccDate,
			s.AccessionNumber
		FROM IdentificationSequenceMax i_m
			INNER JOIN Identification i on (i_m.IdentificationSequenceMax=i.IdentificationSequence
				and i_m.IdentificationUnitID=i.IdentificationUnitID
				and i_m.CollectionSpecimenID=i.CollectionSpecimenID)
			INNER JOIN IdentificationUnit iu on (i.IdentificationUnitID=iu.IdentificationUnitID
				and i.CollectionSpecimenID=iu.CollectionSpecimenID)
			INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
			INNER JOIN CollectionSpecimen s on iu.CollectionSpecimenID = s.CollectionSpecimenID
			LEFT JOIN IdentificationUnitAnalysis iua on (iu.CollectionSpecimenID=s.CollectionSpecimenID
				AND iu.IdentificationUnitID=iua.IdentificationUnitID)
				AND ({1})
			WHERE {0} {2}
			 -- to prevent that identifications of host taxa are used here, use only the identificationunit that has no relation
			 -- due to the fact that organisms found on another have a relation type but no RelatedUnitID
			 -- really weird
			AND iu.RelatedUnitID IS NULL
			""".format(self.project_id_string, self.analysis_id_combined_string, withholdclause))
		if self.update_date_span[0]:
			sql.append("""AND i.LogUpdatedWhen > CONVERT(DATETIME, '{0}', 102)""".format(*self.update_date_span))
		sql.append("""AND i.LogUpdatedWhen < CONVERT(DATETIME, '{1}', 102)""".format(*self.update_date_span))
		sql.append("""GROUP BY i_m.CollectionSpecimenID, i_m.IdentificationUnitID, iu.RowGUID, iu.FamilyCache, i.TaxonomicName, i.NameURI,
				i.ResponsibleName, s.ExternalDatasourceID, iua.AnalysisID, s.AccessionDate, s.AccessionNumber
			) AS t
			WHERE LEN(t.NameID)>0 -- AND t.FamilyCache IS NOT NULL
			ORDER BY CollectionSpecimenID, IdentificationUnitID""")
		log_query.info("\nSelect data for _Specimens table:\n\t%s" % "\n\t".join(sql))
		selectquery = "\n".join(sql)
		
		
		self.cur.execute(selectquery)
		self.con.commit()
		
		
	
	
	
	
	
