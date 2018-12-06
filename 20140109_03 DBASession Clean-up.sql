--USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET XACT_ABORT ON;
IF OBJECT_ID('tempdb..#DynamicError') IS NOT NULL DROP TABLE #DynamicError;
IF OBJECT_ID('tempdb..#Server') IS NOT NULL DROP TABLE #Server;
IF OBJECT_ID('tempdb..#DBRestore') IS NOT NULL DROP TABLE #DBRestore;
IF OBJECT_ID('tempdb..#DBASession') IS NOT NULL DROP TABLE #DBASession;
IF OBJECT_ID('tempdb..#BeforeAfter') IS NOT NULL DROP TABLE #BeforeAfter;
IF OBJECT_ID('tempdb..#DBASessionParticipation') IS NOT NULL DROP TABLE #DBASessionParticipation;
GO

-- This table holds error information generated during execution of dynamic SQL.
CREATE TABLE #DynamicError
		(RowSeq					int					NOT NULL IDENTITY(1, 1),
		ServerName				nvarchar(128)		NOT NULL,
		Item					nvarchar(2048)		NOT NULL,
		Msg						nvarchar(2048)		NOT NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

DECLARE	@ServerName		nvarchar(128) = CONVERT(varchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('MachineName'))),
		@DBName			nvarchar(128) =  DB_NAME(),
		@RunDateTime	datetime = GETDATE(),
		@DBASessionId	int,
			
		@DailyRestoreServers			nvarchar(1000) = 'dev-sql, test-sql',
		@DailyRestoreDBs				nvarchar(128) = 'DBADevRestore, DBADailyProdNighlyRestore',
		@DailyRestoreDB_RetentionDays	int = 14,			-- Days to retain DBADev-related before/after data.
		
		@MaxRowsDeletable	int = 500000;					-- Threshold for individual run's row deletions.		

DECLARE	@Title				varchar(1000) = 'DBASession Before/After Table Clean-up';

PRINT	'DBA Databse Maintenance'
PRINT	'"' + REPLACE(@Title, '"', '""') + '"';
PRINT	'DB: ' + @ServerName + '.' + @DBName;
PRINT	'Run: ' + CONVERT(char(23), @RunDateTime, 121);
PRINT	SPACE(0);

DECLARE	@RC				int,
		@Error			int,
		@ErrMsg			nvarchar(2048),
		@ExpectedCount	int,
		@RowCount		int,

		@SQL			nvarchar(max),
		@ServerID		tinyint,
		@ServerName2		nvarchar(128),
		@TableID		int,
		@TableName		nvarchar(128),
		@TableId_Max	int,
		@SessionCount	int;

DECLARE	@SQL_Template_LastRestore		nvarchar(max),
		@SQL_Template_OutdatedSession	nvarchar(max),
		@SQL_Template_BeforeAfterSEL	nvarchar(max),
		@SQL_Template_ToBeDeleted		nvarchar(max),
		@SQL_Template_SessionDEL		nvarchar(max);
		
DECLARE	@ResultsPath		varchar(255) = '\\localhost\ad-hoc\Results\20140109_03',
		@ResultsFileName	varchar(255) = @ServerName + '.' + @DBName + '.20140109_03.' + 
											DBADefault.dbo.f_DBAFileDateStamp(@RunDateTime) + '.csv',
		@ResultsFileId		int;

--debug begin
--DECLARE	@Email_To			varchar(4000) = '',
--		@Email_CC			varchar(4000) = SPACE(0);
--debug end

DECLARE	@Email_To			varchar(4000) = '',
		@Email_CC			varchar(4000) = '',
		@Email_Subject		varchar(4000) = '(All Environments) ' + @Title,
		@Email_Body			varchar(4000) = SPACE(0);

DECLARE	@NoActivity_To		varchar(4000) = '',
		@NoActivity_CC		varchar(4000) = SPACE(0),
		@NoActivity_Subject	varchar(4000) = '(Nothing to Do) ' + @Title;

-- The servers to be processed.
CREATE TABLE #Server
		(ServerID			tinyint				NOT NULL IDENTITY(1, 1),
		ServerName			nvarchar(128)		NOT NULL
		PRIMARY KEY NONCLUSTERED
			(ServerID)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(ServerName)
			WITH FILLFACTOR = 100);

-- Holds Database names, and their last restoration date/time.
CREATE TABLE #DBRestore
		(ServerID		tinyint					NOT NULL,
		DBName			nvarchar(128)			NOT NULL,
		RestoreDateTime	datetime				NOT NULL
		PRIMARY KEY NONCLUSTERED
			(ServerID, DBName)
			WITH FILLFACTOR = 100);
			
-- Holds DBASession ID's to be deleted.
CREATE TABLE #DBASession
		(ServerID				tinyint			NOT NULL,
		DBASessionID			int				NOT NULL,
		SessionDateTime			datetime		NOT NULL,
		DBName					nvarchar(128)	NULL,
		IssueNumber				int				NULL
		PRIMARY KEY NONCLUSTERED
			(ServerID, DBASessionID)
			WITH FILLFACTOR = 100);
		
-- Holds the Before/After table names.
CREATE TABLE #BeforeAfter
		(ServerID			tinyint			NOT NULL,
		TableID				int				NOT NULL IDENTITY(1, 1),
		TableName			nvarchar(128)	NOT NULL
		PRIMARY KEY NONCLUSTERED
			(ServerID, TableID)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(ServerID, TableName)
			WITH FILLFACTOR = 100);

-- Holds summary information for the Sessions to be deleted, and the affected row counts for each related table.			
CREATE TABLE #DBASessionParticipation
		(ServerID				tinyint			NOT NULL,
		DBASessionID			int				NOT NULL,
		TableID					int				NOT NULL,
		"RowCount"				int				NOT NULL
		PRIMARY KEY NONCLUSTERED
			(ServerID, DBASessionID, TableID)
			WITH FILLFACTOR = 100);

------------------------------

SET @SQL_Template_LastRestore = 
'BEGIN TRY
	INSERT	#DBRestore (ServerId, DBName, RestoreDateTime)
	SELECT	ServerId = @ServerID@,
			A2.DBName,
			A2.RestoreDateTime
	FROM	(SELECT	A1.DBName,
					A1.RestoreDateTime,
					DBDisplaySeq = ROW_NUMBER() OVER(PARTITION BY A1.DBName ORDER BY A1.RestoreDateTime DESC)
			FROM	(SELECT	DBName = destination_database_name,
							RestoreDateTime = restore_date
					FROM	[@ServerName@].msdb.dbo.restorehistory rh
							INNER JOIN [@ServerName@].master.dbo.sysdatabases sd
								ON sd.name = rh.destination_database_name
					WHERE	(destination_database_name LIKE ''DBA%''
								OR destination_database_name LIKE ''ReportServer'')
					AND		sd.name NOT LIKE ''DBA[0-9][0-9][0-9][0-9]''
					AND		sd.name NOT IN (''DBADefault'', ''DBAImages'', ''DBAArchives'', ''DBAInvestments'', ''DBAMPE'', ''DBASystems'')) A1) A2
	WHERE	A2.DBDisplaySeq = 1;
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, Item, Msg)
	VALUES(N''@ServerName@'', N''#DBRestore - Insert'', ERROR_MESSAGE());
END CATCH
';

SET @SQL_Template_OutdatedSession = 
'BEGIN TRY
	INSERT	#DBASession (ServerId, DBASessionId, SessionDateTime, DBName, IssueNumber)
	SELECT	ServerId = @ServerID@,
			ds.DBASessionId,
			ds.SessionDateTime,
			ds.DBName,
			ds.IssueNumber
	FROM	[@ServerName@].DBADefault.dbo.DBASession ds
	WHERE	ds.SessionDateTime < (SELECT MIN(RestoreDateTime) FROM #DBRestore WHERE ServerId = @ServerID@)
	UNION
	SELECT	ServerId = @ServerID@,
			ds.DBASessionId,
			ds.SessionDateTime,
			ds.DBName,
			ds.IssueNumber
	FROM	[@ServerName@].DBADefault.dbo.DBASession ds
			INNER JOIN #DBRestore dr
				ON dr.ServerId = @ServerID@
				AND dr.DBName = ds.DBName
	WHERE	ds.SessionDateTime < dr.RestoreDateTime
	AND		(dr.DBName NOT IN ( ''DBADefault'')
				OR (dr.DBName IN ( ''DBADailyProdDBA'')
					AND ds.SessionDateTime < DATEADD(DAY, (-1 * @DailyRestoreDB_RetentionDays@), dr.RestoreDateTime)));
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, Item, Msg)
	VALUES(N''@ServerName@'', N''#DBASession - Insert'', ERROR_MESSAGE());
END CATCH
';

SET @SQL_Template_BeforeAfterSEL = 
'BEGIN TRY
	INSERT	#BeforeAfter (ServerId, TableName)
	SELECT	ServerId = @ServerID@,
			TableName = name
	FROM	[@ServerName@].DBADefault.sys.objects
	WHERE	type = ''U''
	AND		name LIKE ''zDBA_%_BeforeAfter'';
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, Item, Msg)
	VALUES(N''@ServerName@'', N''#BeforeAfter - Insert'', ERROR_MESSAGE());
END CATCH
';

SET @SQL_Template_ToBeDeleted =
'BEGIN TRY
	INSERT #DBASessionParticipation (ServerId, TableId, DBASessionId, "RowCount")
	SELECT	ServerId = @ServerID@,
			TableId = @TableID@, 
			t.DBASessionId, 
			"RowCount" = COUNT(*)
	FROM	[@ServerName@].DBADefault.dbo.[@TableName@] t
			INNER JOIN #DBASession ds 
				ON ds.ServerID  = @ServerID@
				AND ds.DBASessionID = t.DBASessionID
	WHERE	ServerId = @ServerID@
	GROUP BY t.DBASessionId;
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, Item, Msg)
	VALUES(N''@ServerName@'', N''#DBASessionParticipation - Insert'', ERROR_MESSAGE());
END CATCH
';

SET @SQL_Template_SessionDEL = 
'BEGIN TRY
	DELETE	[@ServerName@].DBADefault.dbo.DBASession
	FROM	[@ServerName@].DBADefault.dbo.DBASession ds1
			INNER JOIN #DBASession ds2
				ON ds2.ServerId = @ServerID@
				AND ds2.DBASessionId = ds1.DBASessionId;
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, Item, Msg)
	VALUES(N''@ServerName@'', N''DBASession - Delete'', ERROR_MESSAGE());
END CATCH
'
------------------------------

-- Define the servers to be processed.
BEGIN TRY
	INSERT	#Server (ServerName)
	VALUES	('Dev-SQL')('test-sql');
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#Server - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

------------------------------

-- Gather the latest restore date/times for all databases.	
-- Note: System databases are excluded.	
BEGIN TRY
	SET @SQL = SPACE(0);
	SELECT	@SQL = @SQL +
				REPLACE(
					REPLACE(@SQL_Template_LastRestore,
							'@ServerName@',
							ServerName),
					'@ServerID@',
					CONVERT(nvarchar(3), ServerID))
	FROM	#Server;
	SELECT @SQL;		--debug
	EXEC(@SQL); 
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = '#DBRestore - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

------------------------------

-- Select any DBASession whose SessionDateTime is before the earliest database restoration date.
-- Select any DBASession whose SessionDateTime is before the related database's restoration date.
-- Note: Retain DBADev or DBADailyProdDBA-related before/after tables for a pre-specified number of days after the latest restoration.
BEGIN TRY
	SET @SQL = SPACE(0);
	SELECT	@SQL = @SQL +
				REPLACE(
					REPLACE(
						REPLACE(@SQL_Template_OutdatedSession,
								'@ServerName@',
								ServerName),
						'@ServerID@',
						CONVERT(nvarchar(3), ServerID)),
					'@DailyRestoreDB_RetentionDays@',
					CONVERT(varchar(10), @DailyRestoreDB_RetentionDays))
	FROM	#Server;
	SELECT @SQL;		--debug

	EXEC(@SQL);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = '#DBASession - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;


-- Exit, if nothing to do.
IF NOT EXISTS(SELECT 1 FROM #DBASession)
BEGIN;
	-- Queue 'No activity' email.
	BEGIN TRY
		EXEC @RC = DBADefault.dbo.s_DBAEmailINS
						@ToList  = @NoActivity_To,
						@CCList  = @NoActivity_CC,
						@Subject = @NoActivity_Subject;
		
		IF @RC = 0
			PRINT	'No Activity email queued: ' + @ResultsFileName;
		ELSE
			SET @ErrMsg = 's_DBAEmailINS (No Activity) RC: ' + CONVERT(nvarchar(10), @RC);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N's_DBAEmailINS (No Activity): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;

	GOTO Done;
END;

----------

-- Store the names of the before/after tables.
BEGIN TRY
	SET @SQL = SPACE(0);
	SELECT	@SQL = @SQL +
				REPLACE(
					REPLACE(@SQL_Template_BeforeAfterSEL,
							'@ServerName@',
							ServerName),
					'@ServerId@',
					CONVERT(nvarchar(3), ServerID))
	FROM	#Server;

	EXEC(@SQL);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = '#BeforeAfter - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

--IF @RowCount = 0
--BEGIN;
--	PRINT	'No "Before/After" tables found; terminating run.';
--	GOTO Done;
--END;

-- Store the row counts for the table / session combinations.
BEGIN TRY
	SET @SQL = SPACE(0);
	SELECT	@SQL = @SQL +
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(@SQL_Template_ToBeDeleted,
									'@ServerName@',
									s.ServerName),
							'@ServerID@',
							CONVERT(nvarchar(3), s.ServerID)),
						'@TableID@',
						CONVERT(varchar(10), ba.TableID)),
					'@TableName@',
					ba.TableName)
	FROM	#Server s
			INNER JOIN #BeforeAfter ba
				ON ba.ServerID = s.ServerID;

	EXEC(@SQL);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = '#DBASessionParticipation - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;


-- Exit, if nothing to do.
IF NOT EXISTS(SELECT 1 FROM #DBASessionParticipation)
BEGIN;
	-- Remove the DBASession entries.
	BEGIN TRY
		SET @SQL = SPACE(0);
		SELECT	@SQL = @SQL +
					REPLACE(
						REPLACE(@SQL_Template_SessionDEL,
								'@ServerName@',
								ServerName),
						'@ServerId@',
						CONVERT(nvarchar(3), ServerID))
		FROM	#Server;

		EXEC(@SQL);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = 'DBASession - Delete (1): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		GOTO Failed;

	-- Queue 'No activity' email.
	BEGIN TRY
		EXEC @RC = DBADefault.dbo.s_DBAEmailINS
						@ToList  = @NoActivity_To,
						@CCList  = @NoActivity_CC,
						@Subject = @NoActivity_Subject;
		
		IF @RC = 0
			PRINT	'No Activity email queued: ' + @ResultsFileName;
		ELSE
			SET @ErrMsg = 's_DBAEmailINS (No Activity) RC: ' + CONVERT(nvarchar(10), @RC);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N's_DBAEmailINS (No Activity): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;

	GOTO Done;
END;

-- Count the Sessions, Tables, and Rows to be deleted.
--SELECT	@SessionCount = COUNT(DISTINCT DBASessionId),
--		@TableCount = COUNT(DISTINCT TableId),
--		@RowCount = SUM("RowCount") 
--FROM	#DBASessionParticipation;

-- Exit if no sessions need to be cleaned-up.				
--IF @SessionCount = 0
--BEGIN;
--	PRINT	'No Tables need cleanup; terminating run.';
--	GOTO Done;
--END;

-- Create a session for the clean-up process.
BEGIN TRY
	EXEC DBADefault.dbo.s_DBASessionINS
			@SessionDateTime = @RunDateTime,
			@DBName = @DBName,
			@DBASessionId = @DBASessionId OUT;			
	PRINT	'DBASessionId: ' + CONVERT(varchar(10), @DBASessionId);
	PRINT	SPACE(0);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'DBASession - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;
	
------------------------------
	
-- Create the results file.
BEGIN TRY
	EXEC @RC =  DBADefault.dbo.s_DBAFileINS
					@FilePath = @ResultsPath,
					@FileName = @ResultsFileName,
					@DBASessionId = @DBASessionId,
					@FileId = @ResultsFileId OUT;
	PRINT	'Results file created: ' + CONVERT(varchar(10), @ResultsFileId) + SPACE(2) + @ResultsFileName;
	PRINT	SPACE(0);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = 's_DBAFileINS (Results): ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

------------------------------
	
-- Write results file headings.
BEGIN TRY
	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, 'DBA Database Maintenance'),
			(@ResultsFileId, '"' + REPLACE(@Title, '"', '""') + '"'),
			(@ResultsFileId, 'DB: ' + @ServerName + '.' + @DBName),
			(@ResultsFileId, 'Run: ' + CONVERT(char(23), @RunDateTime, 121)),
			(@ResultsFileId, SPACE(0));
			
	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, '"Minimum retention for ' + @DailyRestoreDBs + ' database(s): ' + CONVERT(varchar(10), @DailyRestoreDB_RetentionDays) + ' days."'),
			(@ResultsFileId, SPACE(0));
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = 'Write Results file headings: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;
		
------------------------------
			
-- Write Restoration report.			
BEGIN TRY			
	-- Store report title and column headings.
	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, 'Restoration History'),
			(@ResultsFileId, 'Server,DB,Restore Date/Time,Earliest');

	-- Store report detail.
	INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	SELECT	FileId = @ResultsFileId,
			Contents = 
				CASE
				WHEN A1.Persist_ServerDisplaySeq = 1
				THEN '"' + REPLACE(A1.Persist_ServerName, '"', '""') +'"'
				ELSE SPACE(0)
				END + ',' +
				A1.DBName + ',' +
				A1.RestoreDateTime + ',' +
				CASE
				WHEN A1.Persist_RestoreDateTime = X1.RestoreDateTime_Min
				THEN 'Yes'
				ELSE SPACE(0)
				END
	FROM	(SELECT	Persist_ServerID = s.ServerID,
					Persist_ServerName = s.ServerName,
					Persist_RestoreDateTime = dr.RestoreDateTime,
					Persist_ServerDisplaySeq = ROW_NUMBER() OVER(PARTITION BY s.ServerID ORDER BY dr.DBName),
					DBName = ISNULL('"' + REPLACE(dr.DBName, '"', '""') + '"', SPACE(0)),
					RestoreDateTime = ISNULL('''' + CONVERT(char(23), dr.RestoreDateTime, 121), SPACE(0))
			FROM	#Server s
					LEFT JOIN #DBRestore dr
						ON dr.ServerId = s.ServerId) A1
			LEFT JOIN
					(SELECT	ServerId,
							RestoreDateTime_Min = MIN(RestoreDateTime)
					FROM	#DBRestore
					GROUP BY ServerId) X1
					ON X1.ServerID = A1.Persist_ServerID
	ORDER BY A1.Persist_ServerID, A1.Persist_ServerDisplaySeq;

	INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, SPACE(0));
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Write Restoration History report: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;	

------------------------------

-- Write the 'Deletable Session' report.
BEGIN TRY
	-- Store report title and column headings.
	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, '"Deletable DBASession Participation, by Table"'),
			(@ResultsFileId, 'Server,Session,Session Date/Time,DB Name,Issue,Rows,Table Name');
	
	-- Store report detail.	
	INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	SELECT	FileId = @ResultsFileId,
			Contents = 
				A2.ServerName + ',' +
				A2.DBASessionId + ',' +
				A2.SessionDateTime + ',' +
				A2.DBName + ',' +
				A2.IssueNumber + ',' +
				A2."RowCount" + ',' +
				A2."TableName"
	FROM	(SELECT	A1.Persist_ServerId,
					A1.Persist_ServerName,
					A1.Persist_DBASessionId,
					A1.Persist_ServerDisplaySeq,
					A1.Persist_SessionDisplaySeq,
					ServerName = 
						CASE
						WHEN A1.Persist_ServerDisplaySeq = 1
						THEN '"' + REPLACE(A1.Persist_ServerName, '"', '""') + '"'
						ELSE SPACE(0)
						END,
					DBASessionId = 
						CASE
						WHEN A1.Persist_SessionDisplaySeq = 1
						THEN ISNULL(CONVERT(varchar(10), A1.Persist_DBASessionId), SPACE(0))
						ELSE SPACE(0)
						END,
					SessionDateTime = 
						CASE
						WHEN A1.Persist_SessionDisplaySeq = 1
						THEN ISNULL('''' + CONVERT(char(23), ds.SessionDateTime, 121), SPACE(0))
						ELSE SPACE(0)
						END,
					DBName  = 
						CASE
						WHEN A1.Persist_SessionDisplaySeq = 1
						THEN ISNULL('"' + REPLACE(ds.DBName, '"', '""') + '"', SPACE(0))
						ELSE SPACE(0)
						END,
					IssueNumber = 
						CASE
						WHEN A1.Persist_SessionDisplaySeq = 1
						THEN ISNULL(CONVERT(varchar(10), ds.IssueNumber), SPACE(0))
						ELSE SPACE(0)
						END,
					A1."RowCount",
					A1.TableName
			FROM	(SELECT	Persist_ServerID =  dsp.ServerID,
							Persist_ServerName = s.ServerName,
							Persist_DBASessionID = dsp.DBASessionID,
							Persist_ServerDisplaySeq = ROW_NUMBER() OVER(PARTITION BY s.ServerID ORDER BY dsp.DBASessionID, ba.TableName),
							Persist_SessionDisplaySeq = ROW_NUMBER() OVER(PARTITION BY s.ServerID, dsp.DBASessionID ORDER BY ba.TableName),
							"RowCount" = ISNULL(CONVERT(varchar(10), dsp."RowCount"), SPACE(0)),
							TableName = ISNULL('"' + REPLACE(ba.TableName, '"', '""') + '"', SPACE(0))
					FROM	#Server s
							LEFT JOIN #DBASessionParticipation dsp
								ON dsp.ServerId = s.ServerId
								LEFT JOIN #BeforeAfter ba
									ON ba.ServerId = dsp.ServerId
									AND ba.TableId = dsp.TableId) A1
							LEFT JOIN #DBASession ds
								ON ds.ServerId = A1.Persist_ServerId
								AND ds.DBASessionId = A1.Persist_DBASessionId) A2
	ORDER BY A2.Persist_ServerID, Persist_ServerDisplaySeq;

	INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, SPACE(0));
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_MESSAGE(),
			@ErrMsg = N'Write Deletable Sessions report: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

------------------------------

-- Determine the highest TableId for deletion(s).
SELECT	@TableId = 0,
		@TableId_Max = MAX(TableId)
FROM	#DBASessionParticipation;

-- Process each Table in turn.
WHILE @TableId < @TableId_Max
BEGIN;

	-- Gather info about the next TableId to process.
	BEGIN TRY
		SELECT	@ServerID = A1.ServerID,
				@ServerName2 = s.ServerName,
				@TableId = A1.TableId,
				@TableName = ba.TableName,
				@ExpectedCount = A1.ExpectedCount,
				@SessionCount = A1.SessionCount
		FROM	(SELECT	TOP (1)
						ServerID,
						TableID,
						ExpectedCount = SUM("RowCount"),
						SessionCount = COUNT(DISTINCT DBASessionId)
				FROM	#DBASessionParticipation
				WHERE	TableID > @TableID
				GROUP BY ServerID, TableID
				ORDER BY ServerID, TableID) A1
				INNER JOIN #Server s
					ON s.ServerID = A1.ServerID
				INNER JOIN #BeforeAfter ba
					ON ba.TableID = A1.TableID
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Loop: Get next table to process: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		BREAK;

	-- Construct the SQL to delete affected DBASessionId entries from specified table.				
	BEGIN TRY
		SET @SQL = 'DELETE [' + @ServerName2 + '].DBADefault.dbo.[' + @TableName + '] ' +
					'FROM [' + @ServerName2 + '].DBADefault.dbo.[' + @TableName + '] ba WITH (TABLOCK)' +
							'INNER JOIN #DBASessionParticipation dsp ' +
								'ON dsp.ServerId = ' + CONVERT(varchar(3), @ServerID) +
								' AND dsp.DBASessionId = ba.DBASessionId ' +
								'AND dsp.TableId = ' + CONVERT(varchar(10), @TableID);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Loop: Build deletion SQL for [' + @ServerName2 + '].dbo.[' + @TableName + ']: ' + ERROR_MESSAGE();
		SELECT @SQL;
	END CATCH
	IF @Error <> 0
		BREAK;
	
	-- Execute the deletion SQL.		
	BEGIN TRY	
		SET @RowCount = 0;		
		EXEC(@SQL);
		SET @RowCount = @@ROWCOUNT;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Execute deletion SQL for [' + @ServerName2 + '].dbo.[' + @TableName + ']; ' + ERROR_MESSAGE();
		SELECT @SQL;
	END CATCH
		
	-- Note results of deletion attempt.
	BEGIN TRY		
		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		VALUES	(@ResultsFileId, 	
					'"' + REPLACE(@ServerName2, '"', '""') + '.dbo.' + @TableName + ': Deleted ' + 
					CONVERT(varchar(10), ISNULL(@RowCount, 0)) + ' rows for ' + 
					CONVERT(varchar(10), @SessionCount) + ' sessions."');

		PRINT	@ServerName2 + '.dbo.' + @TableName + ': Deleted ' + 
				CONVERT(varchar(10), ISNULL(@RowCount, 0)) + ' rows for ' + 
				CONVERT(varchar(10), @SessionCount) + ' sessions."'

		IF @RowCount <> @ExpectedCount
		BEGIN;
			INSERT	DBADefault.dbo.DBAFileRow
					(FileId, Contents)
				OUTPUT INSERTED.Contents					
			VALUES(@ResultsFileId, CONVERT(varchar(10), @ExpectedCount) + ' rows were expected.');
		END;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Note results of deletion for [' + @ServerName2 + '].dbo.[' + @TableName + ']: ' + ERROR_MESSAGE();
		SELECT @SQL;
	END CATCH
	IF @Error <> 0
	OR @RowCount <> @ExpectedCount
		BREAK;
		
	-- Re-index the table after deletion.
	BEGIN TRY		
		SET @SQL = N'ALTER INDEX ALL ON [' + @ServerName2 + N'].DBADefault.dbo.[' + @TableName + N'] REBUILD';
		EXEC(@SQL);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Re-index [' + @ServerName2 + '].DBADefault.dbo.[' + @TableName + ']: ' + ERROR_MESSAGE();
		SELECT	@SQL;
	END CATCH
	IF @Error <> 0
		BREAK;
END;

IF @Error <> 0
	GOTO Failed;

-- Remove the DBASession entries.
BEGIN TRY
	SET @SQL = SPACE(0);
	SELECT	@SQL = @SQL +
				REPLACE(
					REPLACE(@SQL_Template_SessionDEL,
							'@ServerName@',
							ServerName),
					'@ServerID@',
					CONVERT(nvarchar(3), ServerID))
	FROM	#Server;

	EXEC(@SQL);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = 'DBASession - Delete: ' + ERROR_MESSAGE();
	SELECT	@SQL
END CATCH
IF @Error <> 0
	GOTO Failed;

---------------------------------
---------------------------------
--ExportStep:	--debug
-- Export the results.
IF @ResultsFileId IS NOT NULL
BEGIN;
	BEGIN TRY
		EXEC @RC = DBADefault.dbo.s_DBAFile_ExportSimple 
						@FileId = @ResultsFileId,
						@ErrMsg = @ErrMsg OUT;
						
		IF @RC = 0
		AND LEN(ISNULL(@ErrMsg, SPACE(0))) = 0
			PRINT	'Results file exported: ' + @ResultsFileName;
		ELSE
			SET @ErrMsg = N's_DBAFile_ExportSimple  RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(SPACE(2) + @ErrMsg, SPACE(0));
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Export Results file: ' + ISNULL(NULLIF(@ErrMsg, SPACE(0)), ERROR_MESSAGE());
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
		
	-- Email the results file.
	BEGIN TRY
		SET @Email_Body = '"' + @ResultsPath + '\' + @ResultsFileName + '"';

		EXEC @RC = DBADefault.dbo.s_DBAEmailINS
						@ToList  = @Email_To,
						@CCList  = @Email_CC,
						@Subject = @Email_Subject,
						@Body    = @Email_Body;
		
		IF @RC = 0
		BEGIN;
			PRINT	'Results email queued: ' + @ResultsFileName;
		END
		ELSE
		BEGIN;
			SET @ErrMsg = N's_DBAEmailINS  RC: ' + CONVERT(nvarchar(10), @RC);
		END;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Queue results email: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;
			
---------------------------------
---------------------------------

GOTO Done;
Failed:

	PRINT	'Error: ' + ISNULL(CONVERT(varchar(10), @Error), 'none');
	PRINT	'"ErrMsg: ' + ISNULL(REPLACE(@ErrMsg, '"', '""'), 'none') + '"';

	RAISERROR('Script failed!', 18, 1);
Done:
GO
	
		

