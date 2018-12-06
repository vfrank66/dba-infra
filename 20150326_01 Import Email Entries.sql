-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
IF DB_NAME() <> 'DBADefault'
	USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
SET XACT_ABORT ON;SET ARITHABORT ON;
IF OBJECT_ID('tempdb..#DynamicError') IS NOT NULL DROP TABLE #DynamicError
IF OBJECT_ID('tempdb..#Server') IS NOT NULL DROP TABLE #Server;
IF OBJECT_ID('tempdb..#Email') IS NOT NULL DROP TABLE #Email;
IF OBJECT_ID('tempdb..#SQL') IS NOT NULL DROP TABLE #SQL;
GO

-- This Script polls the servers specified in the #Server table, and imports any email(s) found in the DBADefault.dbo.DBAEmail_Temp table.
-- This approach was necessary, after bi-directional linked server support was removed.

DECLARE	@ScriptPrefix		varchar(14),
		@Server				nvarchar(128),
		@DBName				nvarchar(128),
		@RunDateTime		datetime,
		@DBASessionId		int;

SELECT	@ScriptPrefix = '20150326_01',
		@Server = CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('ServerName'))),
		@DBName = DB_NAME(),
		@RunDateTime = GETDATE();

DECLARE	@Title				varchar(1000);
SELECT	@Title = 'Import Email Entries';

PRINT	'DBA Database Maintenence';
PRINT	'"' + REPLACE(@Title, '"', '""') + '"';
PRINT	'DB: ' + @Server + '.' + @DBName;
PRINT	'Run: ' + CONVERT(varchar(23), @RunDateTime, 121);
PRINT	SPACE(0);

IF @Server <> 'localhost'
BEGIN;
	PRINT	'This must run from localhost.DBADefault';
	GOTO Failed;
END;

DECLARE	@RC				int,
		@Error			int,
		@ErrMsg			nvarchar(2048);

DECLARE	@SQL_Template		nvarchar(max),
		@SQL				nvarchar(max);

DECLARE	@ServerId			tinyint,
		@ServerId_Max		tinyint,
		@ServerName			nvarchar(128);

DECLARE	@ResultsPath		varchar(255),
		@ResultsFileName	varchar(255),
		@ResultsFileId		int;

SET	@ResultsPath		= '\\localhost\Import2000\Ad-hoc\Results\' + @ScriptPrefix;
SET @ResultsFileName	= @ScriptPrefix + '.' + @DBName + '.' + DBADefault.dbo.f_DBAFileDateStamp(@RunDateTime) + '.csv'

DECLARE
    @Email_To          varchar(4000),
    @Email_CC          varchar(4000),
    @Email_Subject     varchar(4000),
    @Email_Body        varchar(MAX),
    @Email_Attachments varchar(MAX),
    @Email_BodyFormat  nvarchar(10) ;

SELECT	@Email_To			= 'VFrank@DBAIL.org',
		@Email_CC			= SPACE(0),
		@Email_Subject		= '(' + @Server + '.' + @DBName + ') ' + @Title,
		@Email_Body			= '"' + @ResultsPath + '\' + @ResultsFileName + '"' ,
		@Email_BodyFormat = 'TEXT';


CREATE TABLE #DynamicError
		(RowSeq				int					NOT NULL IDENTITY(1, 1),
		ServerId			tinyint				NOT NULL,
		Item				nvarchar(2048)		NOT NULL,
		Error				int					NOT NULL,
		ErrMsg				nvarchar(2048)		NOT NULL
		PRIMARY KEY NONCLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

CREATE TABLE #Server
		(ServerId			tinyint		NOT NULL IDENTITY(1, 1),
		ServerName			nvarchar(128)
		PRIMARY KEY NONCLUSTERED
			(ServerId)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(ServerName)
			WITH FILLFACTOR = 100);

CREATE TABLE #Email
		(ServerId			tinyint			NOT NULL,
		RowSeq				int				NOT NULL,
		ToList				varchar(4000)	NOT NULL,
		CCList				varchar(4000)	NOT NULL,
		SubjectLine			varchar(4000)	NOT NULL,
		Body				varchar(max)	NOT NULL,
		AttachmentList		varchar(max)	NOT NULL,
		"Priority"			varchar(6)		NOT NULL,
		BodyFormat			nvarchar(20)	NULL,
		PRIMARY KEY NONCLUSTERED
			(ServerId, RowSeq)
			WITH FILLFACTOR = 100);

CREATE TABLE #SQL
		(RowSeq				int				NOT NULL IDENTITY(1, 1),
		Contents			nvarchar(max)	NOT NULL
		PRIMARY KEY NONCLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

SET	@SQL_Template = 
'IF EXISTS(SELECT 1 FROM [@ServerName@].DBADefault.dbo.DBAEmail_Temp)
BEGIN;
	DECLARE	@RowCount int;
	BEGIN TRY
		INSERT	#Email
				(ServerId,
				RowSeq,
				ToList,
				CCList,
				SubjectLine,
				Body,
				AttachmentList,
				Priority,
				BodyFormat)
		SELECT	ServerId = @ServerId@,
				RowSeq,
				ToList,
				CCList = CCList,
				SubjectLine,
				Body,
				AttachmentList = REPLACE(AttachmentList, ''"'', SPACE(0)),
				Priority,
				BodyFormat
		FROM	[@ServerName@].DBADefault.dbo.DBAEmail_Temp;
		SET @RowCount = @@ROWCOUNT;
	END TRY
	BEGIN CATCH
		INSERT	#DynamicError
				(ServerId, Item, Error, ErrMsg)
		VALUES	(@ServerId@, ''Extract'', ERROR_NUMBER(), ERROR_MESSAGE());

		SET @RowCount = 0;
	END CATCH

	IF @RowCount > 0
	BEGIN;
		BEGIN TRY
			INSERT	[localhost].DBADefault.dbo.DBAEmail_Indirect
					(ToList,
					CCList,
					SubjectLine,
					Body,
					AttachmentList,
					"Priority",
					BodyFormat)
			SELECT	det.ToList,
					det.CCList,
					det.SubjectLine,
					det.Body,
					det.AttachmentList,
					det."Priority",
					det.BodyFormat
			FROM	[@ServerName@].DBADefault.dbo.DBAEmail_Temp det
					INNER JOIN #Email e
						ON e.ServerId = @ServerId@
						AND e.RowSeq = det.RowSeq;
			SET @RowCount = @@ROWCOUNT;
		END TRY
		BEGIN CATCH
			INSERT	#DynamicError
					(ServerId, Item, Error, ErrMsg)
			VALUES	(@ServerID@, ''Insert'', ERROR_NUMBER(), ERROR_MESSAGE());

			SET @RowCount = 0;
		END CATCH
	END;
			
	IF @RowCount > 0
	BEGIN;
		BEGIN TRY
			DELETE	det
			FROM	[@ServerName@].DBADefault.dbo.DBAEmail_Temp det
					INNER JOIN #Email e
						ON e.ServerId = @ServerId@
						AND e.RowSeq = det.RowSeq;
		END TRY
		BEGIN CATCH
			INSERT	#DynamicError
					(ServerId, Item, Error, ErrMsg)
			VALUES	(@ServerId@, ''Delete'', ERROR_NUMBER(), ERROR_MESSAGE());

			SET @RowCount = 0;
		END CATCH
	END;
END;'
			
BEGIN TRY
	INSERT	#Server
			(ServerName)
	SELECT	A1.ServerName
	FROM	(SELECT 'Dev-SQL' AS ServerName 
			SELECT	'test-sql' UNION ALL
			SELECT	'prod-sql' UNION ALL
			SELECT	'localhost') A1
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#Server - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

SELECT	@ServerId = 0,
		@ServerId_Max = MAX(ServerId)
FROM	#Server;
WHILE @ServerId < @ServerId_Max
BEGIN;
	-- Get next server to poll.
	BEGIN TRY
		SELECT	@ServerId = ServerId,
				@ServerName = ServerName
		FROM	#Server
		WHERE	ServerId = (@ServerId + 1);

		-- Build and execute the SQL
		SET @SQL = 	REPLACE(REPLACE(@SQL_Template, '@ServerName@', @ServerName), '@ServerId@', CONVERT(varchar(3), @ServerId));

		INSERT #SQL (Contents)
		SELECT @SQL AS Contents;
		
		EXEC(@SQL)

		TRUNCATE TABLE #SQL;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Build/Exec SQL (' + @ServerName + '): ' + ERROR_MESSAGE();
		SELECT	@SQL;
	END CATCH
	IF @Error <> 0
		BREAK;
END;

-- If nothing to report, exit the script.
IF NOT EXISTS(SELECT 1 FROM #Email)
AND NOT EXISTS(SELECT 1 FROM #DynamicError)
BEGIN;
	PRINT	'Nothing to do!'
	SET @Email_Subject = 'Nothing to Report ' + @Email_Subject;
	GOTO Done;
END;

-- Get a DBASessionId.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBASessionINS
				@SessionDateTime = @RunDateTime, 
				@DBName = @DBName,
				@DBASessionId = @DBASessionId OUT;
	IF @RC = 0
		PRINT	'DBASessionId: ' + CONVERT(varchar(10), @DBASessionId)
	ELSE 
		SET @ErrMsg = N's_DBASessionINS RC: ' + CONVERT(nvarchar(10), @RC)
END TRY
BEGIN CATCH
	SET	@Error = ERROR_NUMBER();
	SET @ErrMsg = N's_DBASessionINS: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;

-- Create the results file.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBAFileINS
						@FilePath = @ResultsPath,
						@FileName = @ResultsFileName,
						@DBASessionId = @DBASessionId,
						@FileId = @ResultsFileId OUT;

	IF @RC = 0
		PRINT	'ResultsFileId (' + CONVERT(varchar(10), @ResultsFileId) + ') ' + @ResultsFileName;
	ELSE 
		SET @ErrMsg = N'DBAFileIns (Results) RC: ' + CONVERT(nvarchar(10), @RC)
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'DBAFileIns: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;

-- Write the file heading'
BEGIN TRY
	INSERT	DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	SELECT	FileId = @ResultsFileId,
			A1.Contents
	FROM	(SELECT	1 AS RowSeq, 'DBA Database Maintenence' AS Contents UNION ALL
			SELECT	2, '"' + REPLACE(@Title, '"', '""') + '"' UNION ALL
			SELECT	3, '"DB: ' + REPLACE(@Server + '.' + @DBName, '"', '""') + '"' UNION ALL
			SELECT	4, 'Run: ' + CONVERT(char(23), @RunDateTime, 121) UNION ALL
			SELECT	5, SPACE(0)) A1
	ORDER BY A1.RowSeq;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Write Results file heading: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;

-- Write the Dynamic Error listing.
IF EXISTS(SELECT 1 FROM #DynamicError)
BEGIN;
	BEGIN TRY
		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		SELECT	FileId = @ResultsFileId,
				A1.Contents
		FROM	(SELECT	1 AS RowSeq, 'Dynamic Error(s)' AS Contents UNION ALL
				SELECT	2, 'Server,Item, Msg') A1
		ORDER BY A1.RowSeq;

		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		SELECT	FileId = @ResultsFileId,
				Contents = 
					'"' + REPLACE(s.ServerName , '"', '""') + '"' + '.' +
					'"' + REPLACE(de.Item, '"', '""') + '"' + ',' +
					'"' + REPLACE(de.ErrMsg, '"', '""') + '"'
		FROM	#DynamicError de
				INNER JOIN #Server s
					ON s.ServerId = de.ServerId
		ORDER BY de.RowSeq;

		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		SELECT	FileId = @ResultsFileId,
				Contents = SPACE(0);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Write Dynamic Error listing: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;

-- List the transfered email(s).
IF EXISTS(SELECT 1 FROM #Email)
BEGIN;
	BEGIN TRY
		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		SELECT	FileId = @ResultsFileId,
				A1.Contents
		FROM	(SELECT	1 AS RowSeq, 'Transfered Email(s)' AS Contents UNION ALL
				SELECT	2, 'Server,Priority, To,CC,Subject,Body,BodyFormat,Attachment(s)') A1
		ORDER BY A1.RowSeq;

		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		SELECT	FileId = @ResultsFileId,
				Contents = 
					CASE
					WHEN A1.ServerDisplaySeq = 1
					THEN '"' + REPLACE(s.ServerName, '"', '""') + '"'
					ELSE SPACE(0)
					END + ',' +
					A1."Priority" + ',' +
					A1.ToList + ',' +
					A1.CCList + ',' +
					A1.SubjectLine + ',' +
					A1.Body + ',' +
					A1.BodyFormat + ',' +
					A1.AttachmentList
		FROM	(SELECT	Persist_ServerId = ServerId,
						ServerId,
						RowSeq = CONVERT(varchar(3), RowSeq),
						"Priority" = '"' + REPLACE("Priority", '"', '""') + '"',
						ToList = '"' + REPLACE(ToList, '"', '""') + '"',
						CCList = '"' + REPLACE(CCList, '"', '""') + '"',
						SubjectLine = '"' + REPLACE(SubjectLine, '"', '""') + '"',
						Body = 
							CASE
							WHEN LEN(Body) > 2000
							THEN '"' + REPLACE(REPLACE(LEFT(Body, 2000) + '...', CHAR(13) + CHAR(10), '||'), '"', '""') + '"'
							ELSE '"' + REPLACE(REPLACE(Body, CHAR(13) + CHAR(10), '||'), '"', '""') + '"'
							END,
						AttachmentList = '"' + REPLACE(AttachmentList, '"', '""') + '"',
						BodyFormat = '"' + REPLACE(BodyFormat, '"', '""') + '"',
						ServerDisplaySeq = ROW_NUMBER() OVER(PARTITION BY ServerId ORDER BY RowSeq)
				FROM	#Email) A1
						INNER JOIN #Server s
							ON s.ServerId = A1.ServerId
		ORDER BY A1.Persist_ServerId, A1.ServerDisplaySeq;

		INSERT	DBADefault.dbo.DBAFileRow
				(FileId, Contents)
		SELECT	FileId = @ResultsFileId,
				Contents = SPACE(0);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Write Email listing: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;

-- Export the results.
BEGIN TRY 
	SET @RC = 0;
	EXEC @RC = DBADefault.dbo.s_DBAFile_ExportSimple
				@FileId = @ResultsFileId,
				@ErrMsg = @ErrMsg OUT;
	IF @RC = 0
	AND LEN(ISNULL(@ErrMsg, SPACE(0))) = 0
	BEGIN;
		PRINT	'Results file exported.';
		PRINT	SPACE(0);
	END;
	ELSE
		SET @ErrMsg = N's_DBAFile_ExportSimple (Results) RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(SPACE(1) + @ErrMsg, SPACE(0));
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N's_DBAFile_ExportSimple (Results): ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;

-- Queue the email.
BEGIN TRY
	SET @RC = 0;
	select "@Email_Subject" = '"' + REPLACE(@Email_Subject, '"', '""') + '"', "@Server" = @Server;	--debug
	EXEC @RC = DBADefault.dbo.s_DBAEmailINS
					@ToList = @Email_To,
					@CCList = @Email_CC,
					@Subject = @Email_Subject,
					@Body = @Email_Body,
					@BodyFormat = @Email_BodyFormat;
	IF @RC = 0
		PRINT	'Email queued.';
	ELSE 
		SET @ErrMsg = N's_DBAEmailINS (Results) RC: ' + CONVERT(nvarchar(10), @RC);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N's_DBAEmailINS  (Results): ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
OR LEN(@ErrMsg) > 0
	GOTO Failed;
	
------------------------------
GOTO Done;
Failed:
	IF @Error <> 0	
		PRINT	'Error: ' + CONVERT(varchar(10), @Error);
	IF LEN(@ErrMsg) > 0
		PRINT	@ErrMsg;

	IF EXISTS(SELECT 1 FROM #SQL)
		SELECT	Contents
		FROM	#SQL;

	RAISERROR('Script failed!', 18, 1);
Done:
GO

