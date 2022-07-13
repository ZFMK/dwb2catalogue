
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCBarcodeGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 1000
		
		self.temptable = "#BarcodeTempTable"
		self.createBarcodeTempTable()
		self.setMaxPage()
	

	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [AnalysisID],
		[AnalysisNumber], [marker] AS [region], [sequence], [sequencing_date], [responsible],
		[withhold]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query



	def createBarcodeTempTable(self):
		###### TODO: hard coded numerical parameters in the SQL query must be replaced
		
		if self.respect_withhold is True:
			withholdclause = """
			AND (
			(iu2.[DataWithholdingReason] IS NULL OR iu2.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		
		selectquery = """WITH XMLNAMESPACES (DEFAULT 'http://diversityworkbench.net/Schema/tools' )
			SELECT
					IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
					{0} AS DatasourceID
					, iua.CollectionSpecimenID
					, iua.IdentificationUnitID
					, iua.AnalysisID
					, iua.AnalysisNumber
					-- , RANK() OVER (PARTITION BY iua.CollectionSpecimenID ORDER BY iua.AnalysisResult DESC)-1 AS AnalysisSequence
					, CASE WHEN iuamp.value IS NULL THEN
							p1.t.value('(@Value)', 'nvarchar(max)')
						ELSE
							iuamp.Value END AS marker
					, CAST(iua.AnalysisResult AS VARCHAR(MAX)) AS sequence
					, CAST(iua.AnalysisDate AS VARCHAR(50)) AS sequencing_date
					, CAST(iua.ResponsibleName AS VARCHAR(255)) AS responsible
					-- ,CAST(left(convert(varchar, iua.LogCreatedWhen, 120),25) as VARCHAR(59)) AS Created
					, a.withhold
				INTO [{1}]
				FROM (
					SELECT iu.RowGUID,
						CASE WHEN (s.DataWithholdingReason='' OR s.DataWithholdingReason IS NULL) THEN 0 ELSE 1 END AS withhold
					FROM IdentificationUnitAnalysis iu
						INNER JOIN [{2}] ids_temp
							ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
						 -- INNER JOIN CollectionProject p ON p.CollectionSpecimenID=iu.CollectionSpecimenID
						INNER JOIN IdentificationUnit iu2 ON iu2.IdentificationUnitID = iu.IdentificationUnitID
						LEFT JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
					WHERE ({3}) {4}
					GROUP BY iu.RowGUID, s.DataWithholdingReason
				) AS a
					INNER JOIN IdentificationUnitAnalysis iua ON (a.RowGUID=iua.RowGUID)
					LEFT JOIN IdentificationUnitAnalysisMethodParameter iuamp ON (iuamp.AnalysisID=iua.AnalysisID
						AND iuamp.CollectionSpecimenID=iua.CollectionSpecimenID
						AND iuamp.IdentificationUnitID=iua.IdentificationUnitID
						AND iuamp.AnalysisID=iua.AnalysisID
						AND iuamp.AnalysisNumber=iua.AnalysisNumber
						AND iuamp.MethodID=12 AND iuamp.ParameterID=62)
					OUTER APPLY
						iua.ToolUsage.nodes( '/Tools/Tool[@Name="Barcoding"]/Usage[@Name="locus"]' ) AS p1(t)
			""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, self.analysis_id_combined_string, withholdclause)
		log_query.info("\nSpecimen Barcode:\n\t%s" % selectquery)
		
		
		self.cur.execute(selectquery)
		self.con.commit()
		
		
	
	


