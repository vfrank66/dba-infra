--************************************************************
--
-- Run on localhost.'
--
--************************************************************
-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET CONCAT_NULL_YIELDS_NULL ON;SET QUOTED_IDENTIFIER ON;SET XACT_ABORT ON;
IF OBJECT_ID('tempdb..#Error') IS NOT NULL DROP TABLE #Error;

GO

DECLARE	@RC				int,
		@ErrMsg			nvarchar(2048),
		@ServerName		nvarchar(128),
		@DBName			nvarchar(128),
		@RunDateTime	datetime;
	
-- Values from 	
DECLARE	@DBASessionId				int,
		@SessionCreateDateTime		datetime,
		@ExtractCompleteDatetime	datetime,
		@CleanupCompleteDateTime	datetime,
		@LoggingCompleteDateTime	datetime;
		
SELECT	@RunDateTime = GETDATE(),
		@ServerName = ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('ServerName')),
		@DBName = DB_NAME();

PRINT	'DBA SQL Server Agent Logging';
PRINT	'Consolidate SQL Server Agent Job Log Entries';
PRINT	'DB: ' + CONVERT(varchar(128), @ServerName) + '.' + @DBName;
PRINT	'Run: ' + CONVERT(char(23), @RunDateTime, 121);
PRINT	SPACE(0);

DECLARE	@SQLServerId			int,
		@SQLServerId_Max		int,
		@SQLServerName			nvarchar(128),
		@InstanceName			nvarchar(128),
		@MonitorSQLServerAgent	bit,
		@UserServerName			nvarchar(128);
		
DECLARE	@SQL_Archive_Template	nvarchar(max),
		@SQL_Archive			nvarchar(max);
			
-- Create the #Error table;
IF OBJECT_ID('tempdb..#Error') IS NULL
BEGIN;
	BEGIN TRY
		CREATE TABLE #Error
				(RowSeq				int				NOT NULL IDENTITY(1, 1),
				Item				nvarchar(2048)	NOT NULL,
				Msg					nvarchar(2048)	NOT NULL
				PRIMARY KEY CLUSTERED
					(RowSeq)
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'#Error - Create: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		GOTO Failed;
END;

-- Get the current DBASessionId.
IF OBJECT_ID('DBADefault.dbo.z20140514_01_Logging') IS NULL
BEGIN;	
	SET @ErrMsg = 'z20140514_01_Logging does not exist in the DBADefault database!';
	GOTO Failed;
END;

------------------------------

BEGIN TRY
	SELECT	@DBASessionId = DBASessionId,
			@SessionCreateDateTime = SessionCreateDateTime,
			@ExtractCompleteDatetime = ExtractCompleteDatetime,
			@CleanupCompleteDateTime = CleanupCompleteDateTime,
			@LoggingCompleteDateTime = LoggingCompleteDateTime	
	FROM	DBADefault.dbo.z20140514_01_Logging;
END TRY
BEGIN CATCH
	SELECT	@ErrMsg = N'z20140514_01_Logging - Select: ' + ERROR_MESSAGE();
END CATCH
IF LEN(@ErrMsg) > 0
	GOTO Failed;

-- If the server-level extraction has already been completed, skip to the end of this script.
IF @ExtractCompleteDatetime IS NOT NULL
BEGIN;
	PRINT	'Extract already completed: ' + CONVERT(char(23), @ExtractCompleteDatetime, 121);
	GOTO Done;
END;

------------------------------

-- Check for existence of the Job archive table.
IF NOT EXISTS(
	SELECT	1
	FROM	"Prod_SQL01".DBAArchives.INFORMATION_SCHEMA.TABLES
	WHERE	TABLE_NAME = 'DBAJobHistory_Job')
BEGIN;
	SET @ErrMsg = 'Archive version of DBAJobHistory_Job must be created!';
	GOTO Failed;
END;
IF LEN(@ErrMsg) > 0
	GOTO Failed;
	
IF NOT EXISTS(
	SELECT	1
	FROM	"Prod_SQL01".DBAArchives.INFORMATION_SCHEMA.TABLES
	WHERE	TABLE_NAME = 'DBAJobHistory_Step')
BEGIN;
	SET @ErrMsg = 'Archive version of DBAJobHistory_Step must be created!';
	GOTO Failed;
END;
IF LEN(@ErrMsg) > 0
	GOTO Failed;
	

IF NOT EXISTS(
	SELECT	1
	FROM	"prod-sql".DBAArchives.INFORMATION_SCHEMA.TABLES
	WHERE	TABLE_NAME = 'DBAJobHistory_Step')
BEGIN;
BEGIN;
	SET @ErrMsg = 'Archive version of DBAJobHistory_Step must be created!';
	GOTO Failed;
END;
IF LEN(@ErrMsg) > 0
	GOTO Failed;

-- Construct the SQL Template to archive the _Job and _Step tables on a server.
BEGIN TRY
	SET @SQL_Archive_Template = 

DECLARE	@Failed bit;
SET @Failed = 0;
/* Archive new _Job entries */
BEGIN TRY
	INSERT	"prod-sql".DBAArchive.dbo.DBASQLServer_Job
			(SQLServerId, JobRowSeq, JobId, JobName)
	SELECT	SQLServerId = @SQLServerId@,
			JobRowSeq = j1.RowSeq,
			j1.JobId,
			j1.JobName,
			DBASessionId = @DBASessionId@
	FROM	"@UseServerName@".DBADefault.dbo.DBASQLServer_Job j1
	WHERE	j1.DBASessionId = 0
	AND		NOT EXISTS(
				SELECT	1
				FROM	"prod-sql".DBAArchives.dbo.DBASQLServer_Job j2
				WHERE	j2.SQLServerId = @SQLServerId@
				AND		j2.JobRowSeq = j1.RowSeq)
	ORDER BY j1.RowSeq;
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'DBASQLServer_Job - Archive from @UseServerName@', ERROR_MESSAGE());
	SET @Failed = 1;
END CATCH
IF 	@Failed = 0
BEGIN;
	/* Note newly archived _Job entries */
	BEGIN TRY
		UPDATE	"@UseServerName@".DBADefault.dbo.DBASQLServer_Job
		SET		DBASessionId = @DBASessionId@
		FROM	"@UseServerName@".DBADefault.dbo.DBASQLServer_Job j1
		WHERE	j1.DBASessionId = 0
		AND		EXISTS(
					SELECT	1
					FROM	"prod-sql".DBAArchives.dbo.DBASQLServer_Job j2
					WHERE	j2.SQLServerId = @SQLServerId@
					AND		j2.JobRowSeq = j1.RowSeq
					AND		j2.DBASessionId = @DBASessionId@);
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBASQLServer_Job - Archive from @UseServerName@', ERROR_MESSAGE());
		SET @Failed = 1;
	END CATCH
END;

/* Archive new _Step entries */
IF @Failed = 0
BEGIN;
	BEGIN TRY
		INSERT	"prod-sql".DBAArchives.dbo.DBASSQLServer_Step
				(JobRowSeq,
				

-- Establish the ordinal limits for scanning the DBASQLServer table.
BEGIN TRY
	SELECT	@SQLServerId = 0,
			@SQLServerId_Max = MAX(SQLServerId)
	FROM	DBADefault.dbo.DBASQLServer;
END TRY
BEGIN CATCH
	SELECT	@ErrMsg = 'Loop Instantiation:' + ERROR_MESSAGE();
END CATCH
IF LEN(@ErrMsg) > 0
	GOTO Failed;
	
WHILE @SQLServerId < @SQLServerId_Max
BEGIN;
	-- Find the next server's information.
	BEGIN TRY
		SELECT	@SQLServerId = SQLServerId,
				@SQLServerName = SQLServerName,
				@InstanceName = InstanceName,
				@MonitorSQLServerAgent = MonitorSQLServerAgent
		FROM	DBADefault.dbo.DBASQLServer
		WHERE	SQLServerId = 
					(SELECT	TOP (1)
							SQLServerId
					FROM	DBADefault.dbo.DBASQLServer
					WHERE	SQLServerId >= (@SQLServerId + 1)
					ORDER BY SQLServerId);
	END TRY
	BEGIN CATCH
		SELECT	@ErrMsg = N'DBASQLServer - Find next to process: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		GOTO Failed;
		
	-- If this server doesn't has SQL Server Agent monitoring enabled, skip the server.
	IF @MonitorSQLServerAgent = 0
		CONTINUE;
		
	
		
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

------------------------------
GOTO Done;
Failed:
	IF LEN(@ErrMsg) > 0
	BEGIN;
		PRINT	'Error: ' + @ErrMsg;
		RAISERROR(@ErrMsg, 18, 1);
	END;
		
	RAISERROR('Script failed!', 18, 1);
Done:
GO
	

	
