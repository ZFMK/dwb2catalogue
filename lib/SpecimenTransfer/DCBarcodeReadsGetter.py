
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCBarcodeReadsGetter(DCGetter):
	def __init__(self, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, data_source_name, globalconfig)
		self.datasourceid = datasourceid
		
		self.pagesize = 10000
		
		self.temptable = "#BarcodeReadsTempTable"
		self.createBarcodeReadsTempTable()
		self.setMaxPage()
		
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [AnalysisID],
		[AnalysisNumber], [ReadID], [Term], [FieldID]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createBarcodeReadsTempTable(self):
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
		
		query = """SELECT TOP 0
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS [DatasourceID]
				, iua.CollectionSpecimenID
				, iua.IdentificationUnitID
				, iua.AnalysisID
				, iua.AnalysisNumber
				, iuamp.MethodMarker AS ReadID
				, CASE WHEN iuamp.ParameterID IN (78, 79)
					THEN 'url to tracefile not available'
					ELSE iuamp.Value END AS Term
				, iuamp.ParameterID AS FieldID
			INTO [{1}]
			FROM
			IdentificationUnitAnalysis iua, IdentificationUnitAnalysisMethodParameter iuamp
			;""".format(self.datasourceid, self.temptable)
		
		log_query.info("\nSpecimen BarcodeReads:\n\t%s" % query)
		self.cur.execute(query)
		self.con.commit()
		
		parameter_id_offset = 3
		
		if self.analysis_id_method_string is not None:
			query = """
				INSERT INTO [{1}] (
				[DatasourceID],
				[CollectionSpecimenID],
				[IdentificationUnitID],
				[AnalysisID],
				[AnalysisNumber],
				[ReadID],
				[Term],
				[FieldID]
				)
				SELECT
					 -- IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
					{0} AS [DatasourceID]
					, iua.CollectionSpecimenID
					, iua.IdentificationUnitID
					, iua.AnalysisID
					, iua.AnalysisNumber
					, iuamp.MethodMarker AS ReadID
					, CASE WHEN iuamp.ParameterID IN (78, 79)
						THEN 'url to tracefile not available'
						ELSE iuamp.Value END AS Term
					, iuamp.ParameterID+{4} AS FieldID
				FROM (
					SELECT iu.CollectionSpecimenID, iu.IdentificationUnitID,
						iu.AnalysisID, max(iu.LogCreatedWhen) as LogCreatedWhen
					FROM IdentificationUnitAnalysis iu
						INNER JOIN CollectionProject p ON p.CollectionSpecimenID=iu.CollectionSpecimenID
						INNER JOIN IdentificationUnit iu2 ON iu2.IdentificationUnitID = iu.IdentificationUnitID
						INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
					WHERE ({3}) and ({2}) {5}
					GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, iu.AnalysisID
				) AS a
					INNER JOIN IdentificationUnitAnalysis iua ON (
						a.IdentificationUnitID=iua.IdentificationUnitID
						AND a.CollectionSpecimenID=iua.CollectionSpecimenID
						AND a.AnalysisID=iua.AnalysisID
						AND a.LogCreatedWhen=iua.LogCreatedWhen)
					LEFT JOIN IdentificationUnitAnalysisMethodParameter iuamp ON (
						iuamp.AnalysisID=iua.AnalysisID
						AND iuamp.CollectionSpecimenID=iua.CollectionSpecimenID
						AND iuamp.IdentificationUnitID=iua.IdentificationUnitID
						AND iuamp.AnalysisNumber=iua.AnalysisNumber
						AND iuamp.MethodID=16
					)
				WHERE iuamp.Value IS NOT NULL
				ORDER By iua.CollectionSpecimenID, iua.IdentificationUnitID, iuamp.MethodID, iuamp.MethodMarker, iuamp.ParameterID
				;""".format(self.datasourceid, self.temptable, self.project_id_string, self.analysis_id_method_string, parameter_id_offset, withholdclause)
			log_query.info("\nSpecimen BarcodeReads:\n\t%s" % query)
			
			self.cur.execute(query)
			self.con.commit()
		
		if self.analysis_id_string is not None:
			query = """
				WITH XMLNAMESPACES (DEFAULT 'http://diversityworkbench.net/Schema/tools' )
				INSERT INTO [{1}] (
				[DatasourceID],
				[CollectionSpecimenID],
				[IdentificationUnitID],
				[AnalysisID],
				[AnalysisNumber],
				[ReadID],
				[Term],
				[FieldID]
				)
				SELECT
						{0} AS [DatasourceID],
						b.CollectionSpecimenID
						, b.IdentificationUnitID
						, b.AnalysisID
						, b.AnalysisNumber
						, b.ReadID
						, b.Term
						, CASE WHEN b.fieldname='sequencing_lab' THEN 72+{4}
						  ELSE
							CASE WHEN b.fieldname='pcr_primer_forward' THEN 63+{4}
						  ELSE
							CASE WHEN b.fieldname='pcr_primer_reverse' THEN 63+{4}
						  ELSE
							CASE WHEN b.fieldname='format' THEN 81+{4}
						  ELSE
							p.ParameterID+{4} END END END END AS FieldID
					FROM (
						SELECT iua.CollectionSpecimenID AS CollectionSpecimenID
								, iua.IdentificationUnitID
								, iua.AnalysisID
								, iua.AnalysisNumber
								, 2 AS ReadID
								, p.t.value('@Value', 'nvarchar(max)') AS Term
								, p.t.value('@Name', 'nvarchar(255)') AS Fieldname
						FROM  (
							SELECT iu.CollectionSpecimenID, iu.IdentificationUnitID,
								iu.AnalysisID, max(iu.LogCreatedWhen) as LogCreatedWhen
							FROM IdentificationUnitAnalysis iu
								INNER JOIN CollectionProject p ON p.CollectionSpecimenID=iu.CollectionSpecimenID
							WHERE ({3}) and ({2}) AND iu.ToolUsage IS NOT NULL  -- withhold not implemented here
							GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, iu.AnalysisID
						) AS a
						INNER JOIN IdentificationUnitAnalysis iua ON (a.CollectionSpecimenID=iua.CollectionSpecimenID
							AND a.IdentificationUnitID=iua.IdentificationUnitID
							AND a.AnalysisID=iua.AnalysisID
							AND a.LogCreatedWhen=iua.LogCreatedWhen)
						OUTER APPLY
							iua.ToolUsage.nodes( '/Tools/Tool[@Name="Trace"]/Usage[@Name!="trace_file_encoded"]' ) AS p(t)
					) AS b
						INNER JOIN [Parameter] p ON p.DisplayText=b.Fieldname
					-- should we add a where clause to prevent empty term and field_id? This occures when ToolUsage contains /Tools/Tool[@Name=Barcode] instead of Trace e. g. IDs: 500449 = Coregonen01C11 vs. 2000543 = ZFMK-TIS-2000543
					-- Done with inner join
					ORDER By b.CollectionSpecimenID, b.IdentificationUnitID
				""".format(self.datasourceid, self.temptable, self.project_id_string, self.analysis_id_string, parameter_id_offset)
			log_query.info("\nSpecimen BarcodeReads:\n\t%s" % query)
			
			self.cur.execute(query)
			self.con.commit()
	
	

