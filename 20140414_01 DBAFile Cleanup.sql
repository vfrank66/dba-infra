-- Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
IF DB_NAME() <> 'DBADefault'
	USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;
IF OBJECT_ID('tempdb..#Server') IS NOT NULL DROP TABLE #Server;
IF OBJECT_ID('tempdb..#DBAFile') IS NOT NULL DROP TABLE #DBAFile;
GO
DECLARE	@ScriptPrefix	varchar(12),
		@ServerName		nvarchar(128),
		@DBName			nvarchar(128),
		@RunDateTime	datetime,
		@DBASessionId	int;

SELECT	@ScriptPrefix	= '20140414_01',
		@ServerName		= CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('IstanceName'), SERVERPROPERTY('ServerName'))),
		@DBName			= DB_NAME(),
		@RunDateTime	= GETDATE()
		
-- Number of days to retain files with DBASessoniId;		
DECLARE	@FileRetention_Days	int;	

SELECT	@FileRetention_Days	= 21;	

DECLARE	@RetentionDate	datetime,
		@Title			varchar(1000);
		
SELECT	@RetentionDate = DATEADD(DAY, -1 * @FileRetention_Days, @RunDateTime),
		@Title = 'DBAFile Clean-up - All Servers';
					
PRINT	'DBA Database Maintenance';										
PRINT	'"' + REPLACE(@Title, '"', '""') + '"';
PRINT	'Retention (Days): ' + CONVERT(varchar(10), @FileRetention_Days) + ' - Before ' + CONVERT(char(23), @RetentionDate, 121);
PRINT	'DB: ' + CONVERT(varchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('ServerName'))) + @DBName;
PRINT	'Run: ' + CONVERT(char(23), @RunDateTime, 121);
PRINT	SPACE(0);

DECLARE	@Error				int,
		@ErrMsg				nvarchar(2048),
		@RC					int,
		@RowCount			int;
		
DECLARE	@RowSeq_Max			int,
		@RowSeq				int,
		@FileServerName		nvarchar(128),
		@FileId				int,
		@FileId_Min			int;

DECLARE	@SQL						nvarchar(max);

DECLARE	@SQL_SELECT_Template1		nvarchar(max),
		@SQL_SELECT_Template2		nvarchar(max),
		@SQL_SELECT					nvarchar(max),
		
		@SQL_DELETE_Template		nvarchar(max),
		@SQL_DELETE					nvarchar(max),

		@SQL_DELETE_Queue			nvarchar(max),
		
		@SQL_REORG_Template			nvarchar(max),
		@SQL_REORG					nvarchar(max);
				
DECLARE	@ResultsPath			varchar(255),
		@ResultsFileName		varchar(255),
		@ResultsFileId			int;

SELECT	@ResultsPath		= '\\localhost\Ad-hoc\Results\' + @ScriptPrefix,
		@ResultsFileName	= @ScriptPrefix + '.' + @ServerName + '.' + @DBName + + '.' +
							  DBADefault.dbo.f_DBAFileDateStamp(@RunDateTime) + '.csv';
		
DECLARE	@Email_To			varchar(1000),
		@Email_CC			varchar(1000),
		@Email_Subject		varchar(1000),
		@Email_Body			varchar(1000);

SELECT	@Email_To			= '',
		@Email_CC			= '',
		@Email_Subject		= '(' + @ServerName + '.' + @DBName + ') ' + @Title;
				
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
	
CREATE TABLE #DBAFile
		(RowSeq				int				NOT NULL IDENTITY(1, 1),
		ServerRowSeq		tinyint			NOT NULL,
		FileId				int				NOT NULL,
		FilePathId			int				NOT NULL,
		"FileName"			varchar(255)	NOT NULL,
		FileDateTime		datetime		NOT NULL,
		IssueNumber			int				NULL,
		FileRow_Count		int				NOT NULL,
		FileCol_Count		int				NOT NULL,
		FileNote_Count		int				NOT NULL,
		Note				nvarchar(2048)	NULL
		PRIMARY KEY CLUSTERED
			(ServerRowSeq, FileId)
			WITH FILLFACTOR = 100);

-- Servers against which to run this logic.
BEGIN TRY
	INSERT	#Server
			(ServerName)
	--VALUES	(N'prod-sql'), (N'test-sql'), ;
	SELECT	N'prod-sql' AS ServerName UNION ALL
	SELECT	N'localhost' UNION ALL
	SELECT	N'test-sql' UNION ALL
	SELECT	N'Dev-SQL' ;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#Server - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;
	
SET @SQL_SELECT_Template1 = 
	N'INSERT	#DBAFile
			(ServerRowSeq,
			FileId,
			FilePathId,
			"FileName",
			FileDateTime,
			IssueNumber,
			FileRow_Count,
			FileCol_Count,
			FileNote_Count)
	SELECT	ServerRowSeq = @ServerRowSeq@,
			A1.FileId,
			A1.FilePathId,
			A1."FileName",
			A1.FileDateTime,
			A1.IssueNumber,
			FileRow_Count = ISNULL(FileRow.FileRow_Count, 0),
			FileCol_Count = ISNULL(FileCol.FileCol_Count, 0),
			FileNote_Count = ISNULL(FileNote.FileNote_Count, 0)
	FROM	(SELECT	df.FileId,
					df.FilePathId,
					df."FileName",
					FileDateTime = ds.SessionDateTime,
					ds.IssueNumber
			FROM	[@ServerName@].DBADefault.dbo.DBAFile df
					INNER JOIN [@ServerName@].DBADefault.dbo.DBASession ds
						ON ds.DBASessionId = df.DBASessionId
			WHERE	ds.SessionDateTime < CONVERT(datetime, ''@RetentionDate@'')) A1
			OUTER APPLY
				(SELECT FileRow_Count = COUNT(*)
				FROM	[@ServerName@].DBADefault.dbo.DBAFileRow dfr
				WHERE	dfr.FileId = A1.FileId) FileRow
			OUTER APPLY
				(SELECT FileCol_Count = COUNT(*)
				FROM	[@ServerName@].DBADefault.dbo.DBAFileColumn dfc
						INNER JOIN [@ServerName@].DBADefault.dbo.DBAFileRow dfr
							ON dfr.FileRowId = dfc.FileRowId
				WHERE	dfr.FileId = A1.FileId) FileCol
			OUTER APPLY
				(SELECT	FileNote_Count = COUNT(*)
				FROM	[@ServerName@].DBADefault.dbo.DBAFileNote dfn
				WHERE	dfn.FileId = A1.FileId) FileNote
	ORDER BY A1.FileId;'
	
SET @SQL_SELECT_Template2 = 
	N'INSERT	#DBAFile
			(ServerRowSeq,
			FileId,
			FilePathId,
			"FileName",
			FileDateTime,
			IssueNumber,
			FileRow_Count,
			FileCol_Count,
			FileNote_Count)
	SELECT	ServerRowSeq = @ServerRowSeq@,
			A1.FileId,
			A1.FilePathId,
			A1."FileName",
			A1.FileDateTime,
			A1.IssueNumber,
			FileRow_Count = ISNULL(FileRow.FileRow_Count, 0),
			FileCol_Count = ISNULL(FileCol.FileCol_Count, 0),
			FileNote_Count = ISNULL(FileNote.FileNote_Count, 0)
	FROM	(SELECT	df.FileId,
					df.FilePathId,
					df."FileName",
					FileDateTime = ds.SessionDateTime,
					ds.IssueNumber
			FROM	[@ServerName@].DBADefault.dbo.DBAFile df
					INNER JOIN [@ServerName@].DBADefault.dbo.DBASession ds
						ON ds.DBASessionId = df.DBASessionId
			WHERE	df.FileId < @FileIdMIN@) A1
			OUTER APPLY
				(SELECT FileRow_Count = COUNT(*)
				FROM	[@ServerName@].DBADefault.dbo.DBAFileRow dfr
				WHERE	dfr.FileId = A1.FileId) FileRow
			OUTER APPLY
				(SELECT FileCol_Count = COUNT(*)
				FROM	[@ServerName@].DBADefault.dbo.DBAFileColumn dfc
						INNER JOIN [@ServerName@].DBADefault.dbo.DBAFileRow dfr
							ON dfr.FileRowId = dfc.FileRowId
				WHERE	dfr.FileId = A1.FileId) FileCol
			OUTER APPLY
				(SELECT	FileNote_Count = COUNT(*)
				FROM	[@ServerName@].DBADefault.dbo.DBAFileNote dfn
				WHERE	dfn.FileId = A1.FileId) FileNote
	ORDER BY A1.FileId;'	
------------------------------
	
-- Scan each server, and find files to be deleted.	
SELECT	@RowSeq = 0,
		@RowSeq_Max = MAX(RowSeq)
FROM	#Server;
WHILE @RowSeq < @RowSeq_Max
BEGIN;

	-- Get next server to process.
	SELECT	@RowSeq = RowSeq,
			@ServerName = ServerName
	FROM	#Server
	WHERE	RowSeq = (@RowSeq + 1);
	
	BEGIN TRY
		SET	@SQL = 
				REPLACE(
					REPLACE(
						REPLACE(@SQL_SELECT_Template1,
								'@ServerRowSeq@',
								CONVERT(varchar(10), @RowSeq)),
						'@RetentionDate@',
						@RetentionDate),
					'@ServerName@',
					@ServerName);
				
		EXEC(@SQL);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @ServerName + ' - Select 1: ' + ERROR_MESSAGE();
				
		SELECT @SQL;
		
	END CATCH
	IF @Error <> 0
		GOTO Failed;

	SET @FileId_Min = 
			ISNULL((SELECT	MIN(FileId)
					FROM	#DBAFile
					WHERE	ServerRowSeq = @RowSeq),
					0);

	IF @FileId_Min > 0
	BEGIN;
		BEGIN TRY
			SET	@SQL = 
					REPLACE(
						REPLACE(
							REPLACE(@SQL_SELECT_Template2,
									'@ServerRowSeq@',
									CONVERT(varchar(10), @RowSeq)),
							'@ServerName@',
							@ServerName),
						'@FileIdMIN@',
						CONVERT(varchar(10), @FileId_Min));
				
			EXEC(@SQL);
		END TRY
		BEGIN CATCH
			SELECT	@Error = ERROR_NUMBER(),
					@ErrMsg = @ServerName + ' - Select 2: ' + ERROR_MESSAGE();
				
			SELECT @SQL;
		
		END CATCH
		IF @Error <> 0
			GOTO Failed;
	END;
		
END;

------------------------------

-- If nothing to do, send email and exit.
IF NOT EXISTS(
	SELECT	1
	FROM	#DBAFile)
BEGIN;
	BEGIN TRY
		SET @Email_Subject = 'Nothing to do - ' + @Email_Subject;
		
		EXEC @RC = DBADefault.dbo.s_DBAEmailINS
						@ToList = @Email_To,
						@CCList = @Email_CC,
						@Subject = @Email_Subject,
						@Body = @Email_Body;
		
		IF @RC <> 0
		BEGIN;
			SET @ErrMsg = N's_DBAEmail_Ins - Nothing to do - RC: ' + CONVERT(nvarchar(10), @RC);
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
PRINT	'Summary';
BEGIN TRY
	SELECT	"Rows" = ISNULL(CONVERT(varchar(10), NULLIF(A1.FileRow_Count, 0)), SPACE(0)),
			"Columns" = ISNULL(CONVERT(varchar(10), NULLIF(A1.FileCol_Count, 0)), SPACE(0)),
			"Notes" = ISNULL(CONVERT(varchar(10), NULLIF(A1.FileNote_Count, 0)), SPACE(0)),
			"Server" = ISNULL(srvr.ServerName, N'Total')
	FROM	(SELECT	Section = 1,
					ServerRowSeq,
					FileRow_Count = SUM(FileRow_Count),
					FileCol_Count = SUM(FileCol_Count),
					FileNote_Count = SUM(FileNote_Count)
			FROM	#DBAFile
			GROUP BY ServerRowSeq
			UNION ALL
			SELECT	Section = 2,
					ServerRowSeq = NULL,
					FileRow_Count = SUM(FileRow_Count),
					FileCol_Count = SUM(FileCol_Count),
					FileNote_Count = SUM(FileNote_Count)
			FROM	#DBAFile) A1
			LEFT JOIN #Server srvr
				ON srvr.RowSeq = A1.ServerRowSeq
	ORDER BY A1.Section, srvr.ServerName;
	PRINT	SPACE(0);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Summary: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

------------------------------

-- Queue file entry deletion. Entries will be deleted at a later time.
SELECT	@RowSeq = 0,
		@RowSeq_Max = MAX(RowSeq)
FROM	#Server;

WHILE @RowSeq < @RowSeq_Max
BEGIN;
	BEGIN TRY
		SELECT	@RowSeq = RowSeq,
				@FileServerName = ServerName
		FROM	#Server
		WHERE	RowSeq = (@RowSeq + 1);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'#Server - Select next server for file-deletion queueing: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		BREAK;
/*
	-- Temporary logic, until prod-sql uses file-deletion queueing.
	IF @FileServerName = N'prod-sql'
		CONTINUE;

	-- Skip if no entries for the server.
	IF NOT EXISTS(SELECT 1 FROM #DBAFile WHERE ServerRowSeq = @RowSeq)
		CONTINUE;
*/
	-- Queue the file(s) for deletion, and update the file-entries' Note column.
	IF EXISTS(SELECT 1 FROM #DBAFile WHERE ServerRowSeq = @RowSeq)
	BEGIN;
		BEGIN TRY
			SET @SQL = 
				N'INSERT [' + @FileServerName + N'].DBADefault.dbo.DBAFile_Cleanup (FileId)' +
				N' SELECT df.FileId FROM #DBAFile df WHERE df.ServerRowSeq = ' + CONVERT(nvarchar(10), @RowSeq) + 
				N' AND NOT EXISTS(SELECT 1 FROM [' + @FileServerName + N'].DBADefault.dbo.DBAFile_Cleanup dfc WHERE dfc.FileId = df.FileId);'					

			EXEC(@SQL);

			UPDATE	#DBAFile
			SET		Note = 'Deletion queued'
			WHERE	ServerRowSeq = @RowSeq;
		END TRY
		BEGIN CATCH
			--SELECT	@Error = ERROR_NUMBER(),
			--		@ErrMsg = N'Queue Deletion (' + @FileServerName + N'): ' + ERROR_MESSAGE();
				
			UPDATE	#DBAFile
			SET		Note = N'Deletion-queueing: ' + ERROR_MESsAGE()
			WHERE	ServerRowSeq = @RowSeq;

			SELECT @SQL;
		END CATCH
		IF @Error <> 0
			BREAK;
	END;
END;
IF @Error <> 0
	GOTO Failed;

------------------------------
/*
-- Delete file entries one-by-one.
SET @SQL_DELETE_Template = 
	N'DECLARE @ErrMsg nvarchar(128)

	BEGIN TRY
		DELETE	[@ServerName@].DBADefault.dbo.DBAFileNote
		WHERE	FileId = @FileId@
	END TRY
	BEGIN CATCH
		SELECT @ErrMsg = N''DBAFileNote - Delete: '' + ERROR_MESSAGE();
	END CATCH

	IF @ErrMsg IS NULL
	BEGIN;
		BEGIN TRY
			DELETE	[@ServerName@].DBADefault.dbo.DBAFileColumn
			WHERE	FileRowId IN
						(SELECT	FileRowId
						FROM	[@ServerName@].DBADefault.dbo.DBAFileRow
						WHERE	FileId = @FileId@);
		END TRY
		BEGIN CATCH
			SELECT @ErrMsg = N''DBAFileColumn - Delete: '' + ERROR_MESSAGE();
		END CATCH
	END;

	IF @ErrMsg IS NULL
	BEGIN;
		BEGIN TRY
			DELETE	[@ServerName@].DBADefault.dbo.DBAFileRow
			WHERE	FileId = @FileId@;
		END TRY
		BEGIN CATCH
			SELECT @ErrMsg = N''DBAFileRow - Delete: '' + ERROR_MESSAGE();
		END CATCH
	END;

	IF @ErrMsg IS NULL
	BEGIN;
		BEGIN TRY
			DELETE	[@ServerName@].DBADefault.dbo.DBAFile
			WHERE	FileId = @FileId@;
		END TRY
		BEGIN CATCH
			SELECT @ErrMsg = N''DBAFile - Delete: '' + ERROR_MESSAGE();
		END CATCH
	END;

	IF @ErrMsg IS NOT NULL
		RAISERROR(@ErrMsg, 18, 1);
	';

SELECT	@RowSeq = 0,
		@RowSeq_Max = MAX(RowSeq)
FROM	#DBAFile;
WHILE	@RowSeq < @RowSeq_Max
BEGIN;
	SELECT	@RowSeq = df.RowSeq,
			@ServerName = s.ServerName,
			@FileId = df.FileId
	FROM	#DBAFile df
			INNER JOIN #Server s
				ON s.RowSeq = df.ServerRowSeq
	WHERE	df.RowSeq = (@RowSeq + 1);

	-- Temporary logic, until deletion-queueing implemented for prod-sql.
	IF @ServerName <> 'prod-sql'
		CONTINUE;
	
	SET @SQL = 
			REPLACE(
				REPLACE(@SQL_DELETE_Template,
						'@FileId@',
						CONVERT(varchar(10), @FileId)),
				'@ServerName@',
				@ServerName);
				
	BEGIN TRY
		EXEC(@SQL);
			
		UPDATE	#DBAFile
		SET		Note = 'Deleted'
		WHERE	RowSeq = @RowSeq;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg =  @ServerName + N' - Delete FileId (' + CONVERT(nvarchar(10), @FileId) + '): ' + ERROR_MESSAGE();
				
		UPDATE #DBAFile
		SET		Note = ERROR_MESSAGE()
		WHERE	RowSeq = @RowSeq;
		
		SELECT @SQL;
	END CATCH
	IF @Error <> 0
		GOTO Failed;

END;

PRINT	'Table entries deleted.';
PRINT	SPACE(0);
*/
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
	--VALUES	(@ResultsFileId, 'DBA Database Maintenance'),
	--		(@ResultsFileId, @Title),
	--		(@ResultsFileId, 'Retention (Days): ' + CONVERT(varchar(10), @FileRetention_Days) + 
	--						 ' - Before ' + CONVERT(char(10), @RetentionDate, 121)),
	--		(@ResultsFileId, 'DB: ' + @DBName),
	--		(@ResultsFileId, 'Run: ' + CONVERT(char(23), @RunDateTime, 121)),
	--		(@ResultsFileId, SPACE(0)),
	--		(@ResultsFileId, 'Server,FileName,Creation Date/Time,IssueNumber,Rows,Columns,Notes');
	SELECT	FileId = @ResultsFileId,
			A1.Contents
	FROM	(SELECT	1 AS RowSeq,  'DBA Database Maintenance' AS Contents UNION ALL
			SELECT	2, '"' + REPLACE(@Title, '"', '""') + '"' UNION ALL
			SELECT	3, 'Retention (Days): ' + CONVERT(varchar(10), @FileRetention_Days) +  ' - Before ' + CONVERT(char(10), @RetentionDate, 121) UNION ALL
			SELECT	4, 'DB: ' + @DBName UNION ALL
			SELECT	5, 'Run: ' + CONVERT(char(23), @RunDateTime, 121) UNION ALL
			SELECT  6, SPACE(0) UNION ALL
			SELECT	7, 'Server,FileName,Creation Date/Time,IssueNumber,Rows,Columns,Notes') A1
	ORDER BY A1.RowSeq;
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
				WHEN ISNULL(A1.Persist_ServerDisplaySeq, 1) = 1
				THEN '"' + REPLACE(s.ServerName, '"', '""') + '"'
				ELSE SPACE(0)
				END + ',' +
				ISNULL(A1."FileName", SPACE(0)) + ',' +
				ISNULL(A1.FileDateTime, SPACE(0)) + ',' +
				ISNULL(A1.IssueNumber, SPACE(0)) + ',' +
				ISNULL(A1.FileRow_Count, SPACE(0)) + ',' +
				ISNULL(A1.FileCol_Count, SPACE(0)) + ',' +
				ISNULL(A1.FileNote_Count, SPACE(0)) + ',' +
				ISNULL(A1.Note, ISNULL('"' + REPLACE(s.Note, '"', '""') + '"', SPACE(0)))
	FROM	#Server s
			LEFT JOIN
				(SELECT	ServerRowSeq,
						Persist_ServerDisplaySeq = ROW_NUMBER() OVER(PARTITION BY ServerRowSeq ORDER BY FileId),
						"FileName" = '"' + REPLACE(df."FileName", '"', '""') + '"',
						FileDateTime = '"''' + CONVERT(varchar(23), FileDateTime, 121) + '"',
						IssueNumber = ISNULL(CONVERT(varchar(10), IssueNumber), SPACE(0)),
						FileRow_Count = ISNULL(CONVERT(varchar(10), NULLIF(df.FileRow_Count, 0)), SPACE(0)),
						FileCol_Count = ISNULL(CONVERT(varchar(10), NULLIF(df.FileCol_Count, 0)), SPACE(0)),
						FileNote_Count = ISNULL(CONVERT(varchar(10), NULLIF(df.FileNote_Count, 0)), SPACE(0)),
						Note = ISNULL('"' + REPLACE(df.Note, '"', '""') + '"', SPACE(0))
				FROM	#DBAFile df) A1
				ON A1.ServerRowSeq = s.RowSeq
	ORDER BY s.RowSeq, A1.Persist_ServerDisplaySeq;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'DBAFileRow - Write resport detail: ' + ERROR_MESSAGE();
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

	SET @Email_Body = '"' + @ResultsPath + '\' + @ResultsFileName + '"';
	
	EXEC @RC = DBADefault.dbo.s_DBAEmailINS
					@ToList = @Email_To,
					@CCList = @Email_CC,
					@Subject = @Email_Subject,
					@Body = @Email_Body;
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = N's_DBAEmail_Ins  RC: ' + CONVERT(nvarchar(10), @RC);
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





		