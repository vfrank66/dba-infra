-- Note: Run from localhost.DBADefault.
-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
SET XACT_ABORT ON;SET ARITHABORT ON;
IF OBJECT_ID('tempdb..#Error') IS NOT NULL DROP TABLE #Error;
IF OBJECT_ID('tempdb..#DynamicError') IS NOT NULL DROP TABLE #DynamicError;
IF OBJECT_ID('tempdb..#Server') IS NOT NULL DROP TABLE #Server;
IF OBJECT_ID('tempdb..#Log') IS NOT NULL DROP TABLE #Log;
IF OBJECT_ID('tempdb..#SQL') IS NOT NULL DROP TABLE #SQL;
GO

CREATE TABLE #Error
		(RowSeq			int					NOT NULL IDENTITY (1, 1),
		Item			nvarchar(2048)		NOT NULL,
		Msg				nvarchar(2048)		NOT NULL
		PRIMARY KEY NONCLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

CREATE TABLE #DynamicError
		(RowSeq			int					NOT NULL IDENTITY (1, 1),
		ServerName		nvarchar(128)		NOT NULL,
		DBName			nvarchar(128)		NOT NULL,
		Item			nvarchar(2048)		NOT NULL,
		Msg				nvarchar(2048)		NOT NULL
		PRIMARY KEY NONCLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);
GO

DECLARE	@ScriptPrefix	varchar(14) = '20150708_02',
		@ServerName		nvarchar(128) = CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('MachineName'))),
		@DBName			nvarchar(128) = DB_NAME(),
		@RunDateTime	datetime = GETDATE(),
		@DBASessionId	int;

DECLARE	@Title			varchar(1000) = 'DBADBA Dynamic Job Clean-up';

PRINT	'DBA Database Maintenance';
PRINT	'"' + REPLACE(@Title, '"', '""') + '"';
PRINT	'DB: ' + @ServerName + '.' + @DBName;
PRINT	'Run: ' + CONVERT(char(23), @RunDateTime, 121);
PRINT	SPACE(0);

DECLARE	@RC			int,
		@Error		int,
		@ErrMsg		nvarchar(128);

DECLARE	@ResultsPath		varchar(255),
		@ResultsFileName	varchar(255),
		@ResultsFileId		int;

SELECT	@ResultsPath		= '\\localhost\Import2000\Ad-hoc\Results\' + @ScriptPrefix,
		@ResultsFileName	= @ScriptPrefix + '.' + @ServerName + '.' + @DBName + '.Results.' +
							  DBADefault.dbo.f_DBAFileDateStamp(@RunDateTime) + '.csv';

DECLARE	@Email_To			varchar(4000),
		@Email_Subject		varchar(4000),
		@Email_Body			varchar(max);

SELECT	@Email_To			= 'VFrank@DBAIL.org',
		@Email_Subject		= '(All Servers) ' + @Title,
		@Email_Body			= SPACE(0);


-- Applicable server(s).
CREATE TABLE #Server
		(ServerId			tinyint			NOT NULL IDENTITY(1, 1),
		ServerName			nvarchar(128)	NOT NULL
		PRIMARY KEY NONCLUSTERED
			(ServerId)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(ServerName)
			WITH FILLFACTOR = 100);

-- Job step history.
CREATE TABLE #Log
		(ServerName			nvarchar(128)		NOT NULL,
		JobID				uniqueidentifier	NOT NULL,
		JobName				nvarchar(128)		NOT NULL,
		CreateDate			datetime			NOT NULL,
		StepID				int					NULL,
		StepName			nvarchar(128)		NULL,
		Command				nvarchar(max)		NOT NULL,
		RunStatus			int					NULL,
		RunDate				int					NULL,
		RunTime				int					NULL,
		RunDuration			int					NULL,
		"Message"			nvarchar(4000)		NULL,
		InstanceId			int					NULL
		UNIQUE NONCLUSTERED
			(JobID, InstanceID)
			WITH FILLFACTOR = 100);

CREATE TABLE #SQL
		(RowSeq				int					NOT NULL IDENTITY(1, 1),
		"SQL"				nvarchar(max)		NOT NULL);
				
DECLARE	@SQL					nvarchar(max),
		@SQL_Template_Select	nvarchar(max),
		@SQL_Template_Drop		nvarchar(max);

SELECT	@SQL = SPACE(0);

SELECT	@SQL_Template_Select =
'BEGIN TRY
	INSERT	#Log
			(ServerName, JobId, JobName, CreateDate, StepId, StepName, Command, RunStatus, RunDate, RunTime, RunDuration,"Message", InstanceId)
	SELECT	ServerName = ''@ServerName@'',
			JobId = sj.job_id,
			JobName = sj."name",
			CreateDate = sj.date_created,
			StepId = sjs.step_id,
			StepName = sjs.step_name,
			Command = ISNULL(sjs.Command, ''None''),
			RunStatus = sjh.run_status,
			RunDate = sjh.run_date,
			RunTime = sjh.run_time,
			RunDuration = sjh.run_duration,
			"Message" = sjh."Message",
			InstanceId = sjh.instance_id
	FROM	[@ServerName@].msdb.dbo.sysjobs sj
			LEFT JOIN [@ServerName@].msdb.dbo.sysjobsteps  sjs
				ON sjs.job_id = sj.job_id
				LEFT JOIN [@ServerName@].msdb.dbo.sysjobhistory sjh
					ON sjh.job_id = sjs.job_id
					AND sjs.step_id = sjs.step_id
	WHERE	sj."name" LIKE ''DBADBA%''
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, DBName, Item, Msg)
	VALUES(''@ServerName@'', ''msdb'', ''#Log - Insert'', ERROR_MESSAGE());
END CATCH'
--AND		sjh.step_name = '(Job outcome)'


SELECT @SQL_Template_Drop =
N'BEGIN TRY
	INSERT[@ServerName@].DBADefault.dbo.DBADropJob (JobID)
	SELECT	DISTINCT
			JobID
	FROM	#Log
	WHERE	ServerName = ''@ServerName@'';
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, DBName, Item, Msg)
	VALUES(''@ServerName@'', N''DBADefault'', ''DBADropJob - Insert'', ERROR_MESSAGE());
END CATCH;
';

/*
SELECT @SQL_Template_Drop =
'BEGIN TRY
	SET @Result = NULL;
	EXEC @RC = DBADefault.dbo.s_DBASingleStepJob
				@JobName			= ''DBADBA Drop Job @GUID@'',
				@JobDescription		= ''This Job drops an old DBADBA generated Job'',
				@StepName			= ''Drop Job'',
				@StepSubSystem		= N''CmdExec'',
				@DBName				= NULL,
				@DBUser				= N''sa'',
				@Command			= N''SQLCMD -s @ServerName@ -U sa -P @Password@ -d DBADefault -m-1 -b -Q "EXEC msdb.dbo.sp_delete_job @job_id = ''''@JobID@'''' " '',
				@Result				= @Result OUT;
	IF LEN(@Result) > 0
		INSERT #DynamicError (ServerName, DBName, Item, Msg)
		VALUES(''@ServerName@'', N''msdb'', N''s_DBAJobDEL - @JobID@'', @Result);
END TRY
BEGIN CATCH
	INSERT #DynamicError (ServerName, DBName, Item, Msg)
	VALUES(''@ServerName@'', N''msdb'', N''Drop Job - @JobID@'', ERROR_MESSAGE());
END CATCH';
*/
------------------------------

-- Load the #Server table.
BEGIN TRY
	INSERT #Server
			(ServerName)
	VALUES('prod-sql'), ('localhost'), ('test-sql');
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'#Server - Insert', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Build SQL commands for each specified server.
BEGIN TRY
	SELECT	@SQL = @SQL +
				CASE
				WHEN LEN(@SQL) > 0
				THEN CHAR(13) + CHAR(10)
				ELSE SPACE(0)
				END +
				REPLACE(@SQL_Template_Select, '@ServerName@', ServerName)
	FROM	#Server
	ORDER BY ServerId;

	INSERT #SQL("SQL")
	VALUES(@SQL);
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'Build SELECT SQL', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Execute the SQL commands.
BEGIN TRY
	EXEC (@SQL)
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'Run Extract', ERROR_MESSAGE());
	SELECT @SQL;
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
OR EXISTS(SELECT 1 FROM #DynamicError)
	GOTO Failed;

-- Ensure that jobs which are still executing are not purged.
-- Jobs which never get executed will be purged after existing for more than one (1) hour.
BEGIN TRY
	DELETE	#Log
	FROM	#Log l
			LEFT JOIN
				(SELECT	JobId
				FROM	#Log l2
				WHERE	"Message" LIKE 'The Job succeeded%'
							OR "Message" LIKE 'The Job failed%'
							OR ("Message" IS NULL
								AND DATEDIFF(HOUR, CreateDate, @RunDateTime) > 1)) A1
				ON A1.JobId = l.JobId
	WHERE	A1.JobId IS NULL;
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'Remove In-progress Job(s)', ERROR_MESSAGE());
END CATCH
IF EXiSTS(SELECT 1 FROM #Error)
	GOTO Failed;

IF NOT EXISTS(
	SELECT	1
	FROM	#Log)
BEGIN;
	BEGIN TRY
		SET @Email_Subject = 'Nothing to report - ' + @Email_Subject;
		EXEC @RC = DBADefault.dbo.s_DBAEmailINS
					@ToList = @Email_To,
					@Subject = @Email_Subject;
		IF @RC = 0						
			PRINT	'No activity email queued.';
		ELSE
			INSERT #Error (Item, Msg)
			VALUES(N's_DBAEmailINS (No Activity)', N'RC = ' + CONVERT(nvarchar(10), @RC));
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUEs(N's_DBAEmailINS (No Activity)', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;

	-- Nothing left to do, so exit.
	GOTO Done;
END;

------------------------------

-- Create a DBASession.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBASessionINS
					@SessionDateTime = @RunDateTime,
					@DBName = @DBName,
					@IssueNumber = NULL,
					@DBASessionId = @DBASessionId OUT;
	IF @RC = 0
		PRINT	'DBASessionId: ' + CONVERT(varchar(10), @DBASessionId);
	ELSE
		SET @ErrMsg = 's_DBASessionINS failed - RC: ' + CONVERT(varchar(10), @RC);
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N's_DBASessionIns', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

------------------------------
	
-- Create the Results file's table entry.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBAFileINS
					@FilePath = @ResultsPath,
					@FileName = @ResultsFileName,
					@DBASessionId = @DBASessionId,
					@FileId = @ResultsFileId OUT;
	IF @RC = 0
		PRINT	'Results file created: ' + CONVERT(varchar(10), @ResultsFileId) + SPACE(2) + @ResultsFileName
	ELSE
		SET @ErrMsg = N's_DBAFileINS (Results) failed - RC: ' + CONVERT(varchar(10), @RC);
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N's_DBAFileINS (Results)', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;
		
-- Store the file headings.
BEGIN TRY
	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES (@ResultsFileId, 'DBA Database Maintenance'),
			(@ResultsFileId, @Title + ' - Results'),
			(@ResultsFileId, 'DB: ' + @ServerName + '.' + @DBName),
			(@ResultsFileId, 'Run: ' + CONVERT(char(23), @RunDateTime, 121)),
			(@ResultsFileId, SPACE(0));
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'Results File headings', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

BEGIN TRY
	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	VALUES	(@ResultsFileId, 'Server,Job,Step,Run Date,Run Time,Status,Command,Message');

	INSERT DBADefault.dbo.DBAFileRow
			(FileId, Contents)
	SELECT	FileId = @ResultsFileId,
			Contents = 
				ISNULL(A2.ServerName, SPACE(0)) + ',' +
				ISNULL(A2.JobName, SPACE(0)) + ',' +
				ISNULL(A2.StepName, SPACE(0)) + ',' +
				ISNULL(A2.RunDate, SPACE(0)) + ',' +
				ISNULL(A2.RunTime, SPACE(0)) + ',' +
				ISNULL(A2.RunStatus, SPACE(0)) + ',' +
				ISNULL(A2.Command, SPACE(0)) + ',' +
				ISNULL(A2."Message", SPACE(0))
	FROM	(SELECT	ServerName = 
						CASE
						WHEN A1.ServerDisplaySeq = 1
						THEN DBADefault.dbo.f_CSVText(A1.ServerName)
						ELSE SPACE(0)
						END,
					JobName = 
						CASE
						WHEN A1.JobDisplaySeq = 1
						THEN DBADefault.dbo.f_CSVText(A1.JobName)
						ELSE SPACE(0)
						END,
					StepName = 
						CASE
						WHEN A1."Message" LIKE 'The job succeeded%'
						OR   A1."Message" LIKE 'The job failed%'
						THEN SPACE(0)
						ELSE ISNULL(DBADefault.dbo.f_CSVText(A1.StepName), SPACE(0))
						END,
					RunDate = 
						CASE
						WHEN A1."Message" LIKE 'The job succeeded%'
						OR   A1."Message" LIKE 'The job failed%'
						THEN SPACE(0)
						ELSE STUFF(STUFF(CONVERT(char(10), A1.RunDate), 7, 0, '-'), 5, 0, '-')
						END,
					RunTime = 						
						CASE
						WHEN A1."Message" LIKE 'The job succeeded%'
						OR   A1."Message" LIKE 'The job failed%'
						THEN SPACE(0)
						ELSE STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CONVERT(varchar(6), A1.RunTime), 6), 5, 0, ':'), 3, 0, ':')
						END,
					RunStatus = 
						ISNULL(
							CASE RunStatus
							WHEN 0 THEN 'Failed'
							WHEN 1 THEN 'Succeeded'
							WHEN 2 THEN 'Retry'
							WHEN 3 THEN 'Cancelled'
							ELSE 'Unknown (' + CONVERT(varchar(10), A1.RunStatus) + ')'
							END,
							'Unknown (' + ISNULL(CONVERT(varchar(10), A1.RunStatus), 'NULL') + ')'),
					Command = 
						CASE
						WHEN A1."Message" LIKE 'The job succeeded%'
						OR   A1."Message" LIKE 'The job failed%'
						THEN SPACE(0)
						ELSE DBADefault.dbo.f_CSVText(RTRIM(REPLACE(A1.Command, CHAR(13) + CHAR(10), '||')))
						END,
					"Message"= DBADefault.dbo.f_CSVText(A1."Message"),
					A1.OverallDisplaySeq
			FROM	(SELECT	ServerName,
							JobName,
							StepId,
							StepName,
							RunDate,
							RunTime,
							RunStatus,
							Command,
							"Message",
							OverallDisplaySeq = ROW_NUMBER() OVER(ORDER BY ServerName, RunDate, RunTime, InstanceId, StepId),
							ServerDisplaySeq = ROW_NUMBER() OVER(PARTITION BY ServerName ORDER BY RunDate, RunTime, InstanceId, StepId),
							JobDisplaySeq = ROW_NUMBER() OVER(PARTITION BY ServerName, RunDate, RunTime, InstanceId ORDER BY StepId)
					FROM	#Log) A1) A2
	ORDER BY A2.OverallDisplaySeq;
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'#Write report detail', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Export the results file.
BEGIN TRY
	EXEC @RC = DBADefault.dbo.s_DBAFile_ExportSimple
					@FileId = @ResultsFileId,
					@ErrMsg = @ErrMsg OUT;
						
	IF @RC = 0
	AND LEN(ISNULL(@ErrMsg, SPACE(0))) = 0
		PRINT	'Results file exported.';
	ELSE 
		INSERT #Error (Item, Msg)
		VALUES	(N's_DBAFile_ExportSimple (Results)', N'RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL('  Msg: ' + @ErrMsg, SPACE(0)));
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N's_DBAFile_ExportSimple (Results)', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Queue the email.
BEGIN TRY
	SET @Email_Body = '"' + @ResultsPath + '\' + @ResultsFileName + '"';

	EXEC @RC = DBADefault.dbo.s_DBAEmailINS
				@ToList = @Email_To,
				@Subject = @Email_Subject,
				@Body = @Email_Body;
	IF @RC = 0						
		PRINT	'Email queued.';
	ELSE
		INSERT #Error(Item, Msg)
		VALUES(N's_DBAEmailINS', N'RC = ' + CONVERT(nvarchar(10), @RC));
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N's_DBAEmailINS', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

------------------------------

-- Build SQL commands for each specified server.
BEGIN TRY
	SET @SQL = SPACE(0);
	SELECT	@SQL = @SQL +
				CASE
				WHEN LEN(@SQL) > 0
				THEN CHAR(13) + CHAR(10)
				ELSE SPACE(0)
				END +
				REPLACE(@SQL_Template_Drop, '@ServerName@', A1.ServerName)
	FROM	(SELECT DISTINCT
					ServerName
			FROM	#Log l) A1
	ORDER BY A1.ServerName;

	INSERT #SQL("SQL")
	VALUES(@SQL);
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'Build DBADropJob INSERT SQL', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Execute the SQL commands.
BEGIN TRY
	EXEC (@SQL)
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'DBADropJob - Insert', ERROR_MESSAGE());
	SELECT @SQL;
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
OR EXISTS(SELECT 1 FROM #DynamicError)
	GOTO Failed;

------------------------------

GOTO Done;
Failed:

	IF EXISTS(SELECT 1 FROM #DynamicError)
	BEGIN;
		PRINT	'Error(s) from Dynamically-generated SQL';
		SELECT	"Server" = DBADefault.dbo.f_CSVText(ServerName),
				DBName = DBADefault.dbo.f_CSVText(DBName),
				Item = DBADefault.dbo.f_CSVText(Item),
				Msg = DBADefault.dbo.f_CSVText(Msg)
		FROM	#DynamicError
		ORDER BY RowSeq;
		TRUNCATE TABLE #DynamicError;
	END;

	IF EXISTS(SELECT 1 FROM #Error)
	BEGIN;
		PRINT	'Error(s)';
		SELECT	Item = DBADefault.dbo.f_CSVText(Item),
				Msg = DBADefault.dbo.f_CSVText(Msg)
		FROM	#Error
		ORDER BY RowSeq;
		TRUNCATE TABLE #Error;
	END;
		
	RAISERROR('Script failed!', 18, 1);

Done:
GO

IF EXISTS(SELECT 1 FROM #Error)
OR EXISTS(SELECT 1 FROM #DynamicError)
BEGIN;
	IF EXISTS(SELECT 1 FROM #DynamicError)
	BEGIN;
		PRINT	'Error(s) from Dynamically-generated SQL';
		SELECT	"Server" = DBADefault.dbo.f_CSVText(ServerName),
				DBName = DBADefault.dbo.f_CSVText(DBName),
				Item = DBADefault.dbo.f_CSVText(Item),
				Msg = DBADefault.dbo.f_CSVText(Msg)
		FROM	#DynamicError
		ORDER BY RowSeq;
		--TRUNCATE TABLE #DynamicError;	--debug
	END;

	IF EXISTS(SELECT 1 FROM #Error)
	BEGIN;
		PRINT	'Error(s)';
		SELECT	Item = DBADefault.dbo.f_CSVText(Item),
				Msg = DBADefault.dbo.f_CSVText(Msg)
		FROM	#Error
		ORDER BY RowSeq;
		TRUNCATE TABLE #Error;
	END;

	RAISERROR('Script terminated after fall-through!', 18, 1);
END;
GO
