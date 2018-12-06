-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
IF DB_NAME() <> 'DBADefault'
	USE DBADefault;

SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET CONCAT_NULL_YIELDS_NULL ON;
IF OBJECT_ID('tempdb..#Server') IS NOT NULL DROP TABLE #Server;
IF OBJECT_ID('tempdb..#Database') IS NOT NULL DROP TABLE #Database;	
GO

DECLARE	@ScriptPrefix	varchar(12) = '20140506_01',
		@ServerName		nvarchar(128) = CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('ServerName'))),
		@DBName			nvarchar(128) = DB_NAME(),
		@RunDateTime	datetime = GETDATE(),
		@DBASessionId	int;
		
DECLARE	@Title			varchar(1000) = 'Database Summary';		
PRINT	'DBA Database Management';
PRINT	'"' + @Title + '"'
PRINT	'DB: ' + @ServerName + '.' + @DBName;
PRINT	'Run: ' + CONVERT(char(23), @RunDateTime, 121);
PRINT	SPACE(0);

DECLARE	@Error			int,
		@ErrMsg			nvarchar(2048),
		@RC				int,
		@RowCount		int;
		
DECLARE	@SQL_SELECT_Template	varchar(max),
		@SQL_SELECT				varchar(max);
		
DECLARE	@RowSeq				int,
		@RowSeq_Max			int,
		@RowServerName		nvarchar(128);

				
DECLARE	@ResultsPath		varchar(255) = '\\localhost\Ad-hoc\Results\' + @ScriptPrefix,
		@ResultsFileName	varchar(255) = @ScriptPrefix + '.' + @ServerName + '.' + @DBName + + '.' +
										DBADefault.dbo.f_DBAFileDateStamp(@RunDateTime) + 
										'.csv',
		@ResultsFileId		int;
		
DECLARE	@Email_To			varchar(1000) = '',		--';;',
		@Email_CC			varchar(1000) = SPACE(0),		--'',
		@Email_Subject		varchar(1000) =	'(All Servers) ' + @Title,			-- '(' + ISNULL(@InstanceName, @ServerName) + '.' + @DBName + ') ' + @Title,
		@Email_Body			varchar(1000);
		
CREATE TABLE #Server
		(RowSeq				tinyint				NOT NULL IDENTITY(1, 1),
		ServerName			nvarchar(128)		NOT NULL,
		Note				nvarchar(2048)		NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(ServerName)
			WITH FILLFACTOR = 100);
			
CREATE TABLE #Database
		(RowSeq				int					NOT NULL IDENTITY(1, 1),
		ServerName			nvarchar(128)		NOT NULL,
		DBName				nvarchar(128)		NOT NULL,
		FileType			nvarchar(60)		NOT NULL,
		PhysicalName		nvarchar(260)		NOT NULL,
		SourceDB			nvarchar(128)		NULL,
		CreationDateTime	datetime			NOT NULL,
		RestoreDateTime		datetime			NULL,
		RecoveryModel		nvarchar(60)		NOT NULL,
		DBState				nvarchar(60)		NOT NULL,
		DataPages			bigint				NOT NULL,
		LogPages			bigint				NOT NULL,
		FullTextPages		bigint				NOT NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

-- Servers against which to run this logic.
BEGIN TRY
	INSERT	#Server
			(ServerName)
	VALUES	(N'prod-sql'), (N'localhost'),(N'test-sql'), (N'Dev-SQL');
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#Server - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;
			
SET	@SQL_SELECT_Template = 
'SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET CONCAT_NULL_YIELDS_NULL ON;
INSERT	#Database
		(ServerName,
		DBName,
		FileType,
		PhysicalName,
		SourceDB,
		CreationDateTime,
		RestoreDateTime,
		RecoveryModel,
		DBState,
		DataPages,
		LogPages,
		FullTextPages)
SELECT	ServerName = ''@ServerName@'',
		DBName = sd."name",
		FileType = smf.type_desc,
		PhysicalName = smf.physical_name,
		SourceDB = sd_source."name",
		CreationDateTime = sd.create_date,
		RestoreDateTime = lastrestore.restore_date,
		RecoveryModel = sd.recovery_model_desc,
		DBState = smf.state_desc,
		DataPages = SUM(CASE WHEN smf.type_desc = ''ROWS'' THEN smf.size ELSE 0 END),
		LogPages = SUM(CASE WHEN smf.type_desc = ''LOG'' THEN smf.size ELSE 0 END),
		FullTextPages = SUM(CASE WHEN smf.type_desc = ''FULLTEXT'' THEN smf.size ELSE 0 END)
FROM	"@ServerName@".master.sys.databases sd
		LEFT JOIN "@ServerName@".master.sys.databases sd_source
			ON sd_source.database_id = sd.source_database_id
		INNER JOIN "@ServerName@".master.sys.master_files smf
			ON smf.database_id = sd.database_id
		OUTER APPLY
			(SELECT	TOP (1)
					rh.restore_date
			FROM	"@ServerName@".msdb.dbo.restorehistory rh
			WHERE	rh.destination_database_name = sd."name"
			AND		sd_source.database_id IS NULL
			ORDER BY rh.restore_date DESC)	lastrestore
GROUP BY sd."name", sd_source."name", sd.create_date, sd.recovery_model_desc, smf.database_id, smf.physical_name, smf.type_desc, smf.state_desc, lastrestore.restore_date;';

SELECT	@RowSeq = 0,
		@RowSeq_Max = MAX(RowSeq)
FROM	#Server;

WHILE @RowSeq < @RowSeq_Max
BEGIN;
	-- Get next server to process.
	SELECT	@RowSeq = RowSeq,
			@RowServerName = ServerName
	FROM	#Server
	WHERE	RowSeq = (@RowSeq + 1);

	SET	@SQL_SELECT = 
			REPLACE(
				REPLACE(@SQL_SELECT_Template,
						'@ServerRowSeq@',
						CONVERT(varchar(10), @RowSeq)),
				'@ServerName@',
				@RowServerName);

	BEGIN TRY
		EXEC(@SQL_SELECT);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @ServerName + ' - Select: ' + ERROR_MESSAGE();
				
		SELECT @SQL_SELECT;
		
	END CATCH
	IF @Error <> 0
		GOTO Failed;
		
END;

IF NOT EXISTS(SELECT 1 FROM #Database)
BEGIN;
	BEGIN TRY
		SET @Email_Subject = 'Nothing to Report ' + @Email_Subject;
		
		EXEC @RC = DBADefault.dbo.s_DBAEmailINS
						@ToList = @Email_To,
						@CCList = @Email_CC,
						@Subject = @Email_Subject,
						@Body = @Email_Body;
		
		IF @RC <> 0
		BEGIN;
			SET @ErrMsg = N's_DBAEmail_Ins - Nothing to Report - RC: ' + CONVERT(nvarchar(10), @RC);
		END;
		ELSE
		BEGIN;
			PRINT	'Email queued.'
		END;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N's_DBAEmailINS: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;		
	
	GOTO Done;
		
END;	

------------------------------

-- Create DBASession.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBASessionINS
				@SessionDateTime = @RunDateTime,
				@DBName = @DBName,
				@DBASessionId = @DBASessionId OUT;
	IF @RC = 0
	AND @DBASessionId IS NOT NULL
	BEGIN;
		PRINT	'DBASessionId: ' + CONVERT(varchar(10), @DBASessionId);
		PRINT	SPACE(0);
	END;
	ELSE
	BEGIN;
		IF @RC <> 0
		BEGIN;
			SET @ErrMsg = N's_DBASessionINS  RC: ' + CONVERT(nvarchar(10), @RC);
		END;
		ELSE
		BEGIN;
			SET @ErrMsg = 'DBASessionId is NULL';
		END;
	END;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N's_DBASessionINS: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;
	
-- Archive data.	
BEGIN TRY
	INSERT	"prod-sql".DBAArchives.dbo.DBADatabase_Archive
			(DBASessionId,
			ServerName,
			DBName,
			FileType,
			PhysicalName,
			SourceDB,
			CreationDateTime,
			RestoreDateTime,
			RecoveryModel,
			DBState,
			DataPages,
			LogPages,
			FullTextPages)
	SELECT	DBASessionId = @DBASessionId,
			ServerName,
			DBName,
			FileType,
			PhysicalName,
			SourceDB,
			CreationDateTime,
			RestoreDateTime,
			RecoveryModel,
			DBState,
			DataPages,
			LogPages,
			FullTextPages
	FROM	#Database;
END TRY	
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'DBADatabase_Archive - Insert: ' + ERROR_MESSAGE();

select * from #Database ORDER BY ServerName, SourceDB, DBName	--debug			
END CATCH
IF @Error <> 0
	GOTO Failed;
	
-- Create results file.
BEGIN TRY				
	EXEC @RC = DBADefault.dbo.s_DBAFileINS
				@FilePath = @ResultsPath,
				@FileName = @ResultsFileName,
				@FileSet = 2,
				@UserState = 0,
				@DBASessionId = @DBASessionId,
				@FileId = @ResultsFileId OUT;
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = N's_DBAFIleINS  RC: ' + CONVERT(nvarchar(10), @RC)
	END;
	ELSE
	BEGIN;
		PRINT	'ResultsFileId: ' + CONVERT(varchar(10), @ResultsFileId) + SPACE(2) + @ResultsFileName;
	END;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N's_DBAFileIns: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;
	
BEGIN TRY
	INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, 'DBA Database Maintenance'),
			(@ResultsFileId, '"' + REPLACE(@Title, '"', '""') + '"'),
			(@ResultsFileId, 'DB: ' + @DBName),
			(@ResultsFileId, 'Run: ' + CONVERT(char(23), @RunDateTime, 121)),
			(@ResultsFileId, SPACE(0)),
			(@ResultsFileId, 'Server,Source DB,DB,Created,Restored,Recovery Model,State,FileType,Data Pages,Data size (GB),Log Pages, Log size (GB),FullText Pages,FullText size (GB),File')
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'DBAFileRow - Insert - File headings: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;
	
BEGIN TRY
		INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	SELECT	FileId = @ResultsFileId,
			Contents = 
				CASE
				WHEN A2.ServerDisplaySeq = 1
				THEN A2.ServerName
				ELSE SPACE(0)
				END + ',' +
				CASE
				WHEN A2.SourceDisplaySeq = 1
				THEN A2.SourceDB
				ELSE SPACE(0)
				END + ',' +
				CASE
				WHEN A2.DBDisplaySeq = 1
				THEN A2.DBName
				ELSE SPACE(0)
				END + ',' +
				CASE
				WHEN A2.DBDisplaySeq = 1
				THEN A2.CreationDateTime
				ELSE SPACE(0)
				END + ',' +
				CASE
				WHEN A2.DBDisplaySeq = 1
				THEN A2.RestoreDateTime
				ELSE SPACE(0)
				END + ',' +
				CASE
				WHEN A2.DBDisplaySeq = 1
				THEN A2.RecoveryModel
				ELSE SPACE(0)
				END + ',' +
				CASE
				WHEN A2.DBDisplaySeq = 1
				THEN A2.DBState
				ELSE SPACE(0)
				END + ',' +
				A2.FileType + ',' +
				A2.DataPages + ',' +
				A2.DataMB + ',' +
				A2.LogPages + ',' +
				A2.LogMB + ',' +
				A2.FullTextPages + ',' +
				A2.FullTextMB + ',' +
				A2.PhysicalName
	FROM	(SELECT	A1.Persist_ServerName,
					A1.Persist_SourceDB,
					A1.Persist_CreationDateTime,
					A1.ServerName,
					A1.DBName,
					A1.FileType,
					A1.PhysicalName,
					A1.SourceDB,
					CreationDateTime = 
						CASE 
						WHEN LEN(A1.CreationDateTime) > 10
						THEN '"''' + A1.CreationDateTime + '"'
						ELSE A1.CreationDateTime
						END,
					RestoreDateTime = 
						CASE
						WHEN LEN(A1.RestoreDateTime) > 10
						THEN '"''' + A1.RestoreDateTime + '"'
						ELSE A1.RestoreDateTime
						END,
					A1.RecoveryModel,
					A1.DBState,
					A1.DataPages,
					A1.DataMB,
					A1.LogPages,
					A1.LogMB,
					A1.FullTextPages,
					A1.FullTextMB,
					ServerDisplaySeq = 
						ROW_NUMBER() 
							OVER(PARTITION BY A1.Persist_ServerName 
								ORDER BY A1.Persist_SourceDB, A1.Persist_DBName, A1.Persist_CreationDateTime,
										CASE A1.FileType
										WHEN '"Rows"' THEN 1
										WHEN '"Log"'  THEN 2
										ELSE 3
										END),
					SourceDisplaySeq = 
						ROW_NUMBER() 
							OVER(PARTITION BY A1.Persist_ServerName, A1.Persist_SourceDB 
								ORDER BY A1.Persist_DBName, A1.Persist_CreationDateTime,
										CASE A1.FileType
										WHEN '"Rows"' THEN 1
										WHEN '"Log"'  THEN 2
										ELSE 3
										END),
					DBDisplaySeq = 
						ROW_NUMBER() 
							OVER(PARTITION BY A1.Persist_ServerName, A1.Persist_SourceDB, A1.Persist_DBName
								ORDER BY A1.Persist_CreationDateTime, 
										CASE A1.FileType
										WHEN '"Rows"' THEN 1
										WHEN '"Log"'  THEN 2
										ELSE 3
										END)
			FROM	(SELECT	Persist_ServerName = ServerName,
							Persist_SourceDB = SourceDB,
							Persist_DBName = DBName,
							Persist_CreationDateTime = CreationDateTime,
							ServerName = '"' + REPLACE(ServerName, '"', '""') + '"',
							DBName = '"' + REPLACE(DBName, '"', '""') + '"',
							FileType = '"' + REPLACE(FileType, '"', '""') + '"',
							PhysicalName = '"' + REPLACE(PhysicalName, '"', '""') + '"',
							SourceDB = ISNULL('"' + REPLACE(SourceDB, '"', '""') + '"', SPACE(0)),
							CreationDateTime = CONVERT(varchar(23), REPLACE(CONVERT(varchar(23), CreationDateTime, 121), ' 00:00:00.000', SPACE(0))),
							RestoreDateTime = ISNULL(CONVERT(varchar(23), REPLACE(CONVERT(varchar(23), RestoreDateTime, 121), ' 00:00:00.000', SPACE(0))), SPACE(0)),
							RecoveryModel = '"' + REPLACE(RecoveryModel, '"', '""') + '"',
							DBState = '"' + REPLACE(DBState, '"', '""') + '"',
							DataPages = ISNULL(CONVERT(varchar(40), NULLIF(DataPages, 0)), SPACE(0)),
							DataMB = ISNULL(CONVERT(varchar(40), ROUND((NULLIF(DataPages, 0) * 8) / 1024000.000, 3, 0)), SPACE(0)),
							LogPages = ISNULL(CONVERT(varchar(40), NULLIF(LogPages, 0)), SPACE(0)),
							LogMB = ISNULL(CONVERT(varchar(40), ROUND((NULLIF(LogPages, 0) * 8) / 1024000.000, 3, 0)), SPACE(0)),
							FullTextPages = ISNULL(CONVERT(varchar(40), NULLIF(FullTextPages, 0)), SPACE(0)),
							FullTextMB = ISNULL(CONVERT(varchar(40), ROUND((NULLIF(FullTextPages, 0) * 8) / 1024000.000, 3, 0)), SPACE(0))
					FROM	#Database) A1) A2
	ORDER BY CASE
			 WHEN A2.Persist_ServerName LIKE 'Prod%' THEN 1
			 WHEN A2.Persist_ServerName LIKE 'Test%' THEN 2
			 ELSE 3
			 END, A2.Persist_ServerName, A2.ServerDisplaySeq;

	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, SPACE(0));
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Write results detail: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;
	
	
------------------------------

-- Export the file.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBAFile_ExportSimple
				@FileId = @ResultsFileId,
				@ErrMsg = @ErrMsg OUT;
				
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = N's_DBAFile_ExportSimple  RC: ' + CONVERT(nvarchar(10), @RC);
	END
	ELSE
	BEGIN;
		PRINT	'FileId ' + CONVERT(varchar(10), @ResultsFileId) + ' exported.'
		PRINT	SPACE(0);
	END;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N's_DBAFile_ExportSimple: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;
					
------------------------------

BEGIN TRY
	--PRINT 'Recovery Model or Database State Changes'
	--PRINT 'Identifiable by the snapshot run of this script'
	--PRINT 'Note: This does not take into account log chain''s broken in between this script runs.'
	
	SET @Email_Body = 'Changes between recovery model or database state from last 1 runs: ' + ISNULL((SELECT 
			CAST(dda.ServerName + '.' + dda.DBName + ': ' + dda.RecoveryModel + ' ' + dda.DBState + ' ' + CHAR(10) + CHAR(13) AS varchar(1000))
	  FROM  [prod-sql].DBAArchives.dbo.DBADatabase_Archive AS dda
		  LEFT JOIN [prod-sql].DBAArchives.dbo.DBADatabase_Archive AS dda2
			  ON dda2.DBName          = dda.DBName
			 AND dda.DBASessionId - 1 = dda2.DBASessionId
		  INNER JOIN (
						 SELECT MAX(dda_lastweek.DBASessionId) - 1 AS DBASessionId
						   FROM [prod-sql].DBAArchives.dbo.DBADatabase_Archive AS dda_lastweek
					 ) AS dda_lasteek
			  ON dda.DBASessionId     >= dda_lasteek.DBASessionId
	 WHERE
			(
				dda.DBState <> dda2.DBState
			 OR dda.RecoveryModel <> dda2.RecoveryModel
			) 
	FOR XML PATH(''), 
		TYPE).value('(./text())[1]', 'varchar(max)'), '''Nothing to report''')

END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Email body for recovery model/database state changes: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;

BEGIN TRY
	
	SET @Email_Body = @Email_Body + CHAR(10) + CHAR(13) + ' Datbaases not ''ONLINE'': ' + ISNULL(
			(SELECT 
					CAST( dda.ServerName + ' ' + dda.DBName + ' '
						 + dda.DBState AS varchar(100)) 
			  FROM  [prod-sql].DBAArchives.dbo.DBADatabase_Archive AS dda
				  INNER JOIN (
								 SELECT MAX(dda_lastsess.DBASessionId) AS DBASessionId
								   FROM [prod-sql].DBAArchives.dbo.DBADatabase_Archive AS dda_lastsess
							 ) AS dda_lastsession
					  ON dda_lastsession.DBASessionId = dda.DBASessionId
			 WHERE  dda.DBState <> 'ONLINE'), 'all ''ONLINE''')
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Email body for databases online: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;

BEGIN TRY

	SET @Email_Body =  ISNULL(@Email_Body, SPACE(0)) + CHAR(10) + CHAR(13) + CHAR(9) + 
			'"' + @ResultsPath + '\' + @ResultsFileName + '"';
	
	EXEC @RC = DBADefault.dbo.s_DBAEmailINS
					@ToList = @Email_To,
					@CCList = @Email_CC,
					@Subject = @Email_Subject,
					@Body = @Email_Body;
	IF @RC <> 0
		SET @ErrMsg = N's_DBAEmailINS  RC: ' + CONVERT(nvarchar(10), @RC);
	ELSE
		PRINT	'Email queued.';
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N's_DBAEmailINS: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;				

------------------------------
GOTO Done;
Failed:
	IF @Error <> 0
	BEGIN;
		PRINT	'Error: ' + CONVERT(varchar(10), @Error);
	END;
	
	IF LEN(@ErrMsg) > 0
	BEGIN;
		PRINT	'ErrMsg: ' + @ErrMsg;
	END;
	
	RAISERROR('Script failed!', 18, 1);
Done:
GO
