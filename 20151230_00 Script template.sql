-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.

-- Search for		-- <<--- Change this
-- for things to update for your script.

-- The #Instance table defines most things to be changed for a specific run, including whether transactions should be commited or rolled back.

-- Search for		-- <<--- Choose appropriate logic
-- for areas which have may apply to your script.


SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
SET XACT_ABORT ON;SET ARITHABORT ON;
GO

-- Drop '#' tables in tempdb.
-- Note: Space(s) between entries are ignored.
EXEC TRSDefault.dbo.s_DropTempDBTable '#Instance,#Error,#DynamicError,#DynamicProcessingLog,#ExportQueue,#Parameter';
GO

-- drop tables in non-tempdb database(s).
-- Note: Can also include tempdb tables, as 'tempdb..#TableName'.
-- Note: Space(s) between entries are ignored.
EXEC TRSDefault.dbo.s_DropTable 'tempdb..#Error,#tempdb..#DynamicError,#tempdb..#ExportQueue,TRSDefault.dbo.MyTableForExample';
GO

------------------------------
-- Define infrastructure table(s).
------------------------------

-- Holds info about the current execution of the script.
CREATE TABLE #Instance
		(ScriptPrefix		varchar(14)			NOT NULL,		-- The date/number portion of the script's name.
		ScriptInstanceGUID	uniqueidentifier	NOT NULL,		-- Unique for each execution of the script.
		ScriptServerName	nvarchar(128)		NOT NULL,		-- Server on which the script is run.
		ScriptDBName		nvarchar(128)		NOT NULL,		-- Database on which the script is run.
		RunDateTime			datetime2(7)		NOT NULL,		-- Start date/time for the script.
		UserName			nvarchar(128)		NOT NULL,		-- The login which is running the script.
		IssueList			varchar(1000)		NOT NULL,		-- Identifies the Issue(s) related to the script.
		RollbackTRAN		bit					NULL,			-- Controls whether any transaction is commited, or rolled back.
		DBASessionId		int					NULL);			-- Identifier the DBA session identifier, when finally assigned


-- This table is used to identify critieria for the current run of the script.
CREATE TABLE #Parameter
		(RowSeq				int					NOT NULL IDENTITY(1, 1),
		Keyword				varchar(100)		NOT NULL,
		KeywordSequence		int					NULL,
		ValueList			varchar(max)		NOT NULL
		PRIMARY KEY NONCLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

-- Holds info about error(s) during execution.
CREATE TABLE #Error
	(RowSeq				int				NOT NULL IDENTITY(1, 1),
	Item				nvarchar(2048)	NOT NULL,
	Msg					nvarchar(2048)	NOT NULL
	PRIMARY KEY CLUSTERED
		(RowSeq)
		WITH FILLFACTOR = 100);

GO

-- Define the current run's criteria and environment.
-- Note: Clone the info to the DBAProcessingLog table.
BEGIN TRY
	INSERT #Instance
			(ScriptPrefix, 
			ScriptInstanceGUID, 
			ScriptServerName, 
			ScriptDBName, 
			RunDateTime,
			UserName,
			issueList,
			RollbackTRAN)
		OUTPUT	INSERTED.RunDateTime AS DateTimeStamp,
				INSERTED.ScriptPrefix AS ScriptPrefix,
				INSERTED.ScriptInstanceGUID AS ScriptInstanceGUID,
				INSERTED.ScriptServerName AS ScriptServerName,
				INSERTED.ScriptDBName AS ScriptDBName,
				CONVERT(int, NULL) AS RowsAffected,
				N'Script started' AS Item,
				CASE
				WHEN INSERTED.RollbackTRAN IS NULL
				THEN N'No comprehensive transaction(s) expected'
				WHEN INSERTED.RollBackTRAN = 1
				THEN N'Transaction rollback specified'
				WHEN INSERTED.RollBackTRAN = 0
				THEN N'Transaction commit specified'
				ELSE SPACE(0)
				END AS Msg
		INTO	TRSDefault.dbo.DBAProcessingLog		-- Logging
	SELECT	ScriptPrefix = '20151230_00',																							-- <<--- Change this to match the beginning of the scrip file name
			ScriptInstanctGUID = NEWID(), 
			ScriptSeverName = CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('ServerName'))), 
			ScriptDBName = DB_NAME(), 
			RunDateTime = SYSDATETIME(),
			UserName = SUSER_SNAME(),
			IssueList = SPACE(0),																									-- <<--- Change this (zero, one, or more Issue numbers, seperated by comma)			
			RollbackTRAN = 1;																										-- <<--- Change this (set to 0 to persist transaction, 1 to rollback for testing, NULL if NO transaction
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES (N'#Instance - Insert', ERROR_MESSAGE());
END CATCH

-- Parameter(s) for the run.
-- Parameter(s) for the run. Please note that ALL parameters should be presented as text.
BEGIN TRY
	INSERT	#Parameter														-- <<--- Change this (specify parameter(s) for your run
			(Keyword, KeywordSequence, ValueList)
	SELECT	Parameter.Keyword, Parameter.KeywordSequence, Parameter.ValueList
	FROM	(VALUES	('EmailTo', NULL, 'VFrank')			-- One or more email addresses, seperated by semi-colon

			) Parameter (Keyword, KeywordSequence, ValueList)
	ORDER BY Parameter.Keyword, Parameter.KeywordSequence;
			
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'#Parameter - Insert', ERROR_MESSAGE());
END CATCH

GO

------------------------------
-- Note active transaction from prior run.
------------------------------
IF @@TRANCOUNT > 0															
BEGIN;
	INSERT #Error (Item, Msg)
	VALUES (N'Startup', N'A transaction is active from a prior run of this script!');
END;

------------------------------
-- Note if run under improper login.										-- <<--- Choose appropriate logic
------------------------------
/*																			
IF SUSER_SNAME() NOT LIKE N'TRSNT\%'										-- Windows authentication
BEGIN;
	INSERT #Error (Item, Msg)
	VALUES (N'Login', N'Script must run under Windows authentication!');
END;
*/

/*																			
IF SUSER_SNAME() <> N'sa'													-- 'sa' login
BEGIN;
	INSERT #Error (Item, Msg)
	VALUES (N'Login', N'Script must run under 'sa' login!');
END;
*/
GO

-- Holds info about error(s) from dynamic SQL during execution.
CREATE TABLE #DynamicError
		(RowSeq				int					NOT NULL IDENTITY(1, 1),
		ServerName			nvarchar(128)		NULL,
		DBName				nvarchar(128)		NULL,
		Item				nvarchar(2048)		NOT NULL,
		Msg					nvarchar(2048)		NOT NULL
	PRIMARY KEY CLUSTERED
		(RowSeq)
		WITH FILLFACTOR = 100);

CREATE TABLE #DynamicProcessingLog		-- duplicate of @ProcessingLog table; used for internal data-gathering during dynamic SQL execution.
		(RowSeq			int				NOT NULL IDENTITY(1, 1),
		DateTimeStamp	datetime2(7)	NOT NULL DEFAULT(SYSDATETIME()),
		ServerName		nvarchar(128)	NULL,
		DBName			nvarchar(128)	NULL,
		RowsAffected	bigint			NULL,
		Item			nvarchar(2048)	NOT NULL,
		Msg				nvarchar(2048)	NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

-- Holds info about files to be exported.
CREATE TABLE #ExportQueue
	(RowSeq				int				NOT NULL IDENTITY(1, 1),
	Legend				varchar(8000)	NOT NULL,
	FileId				int				NOT NULL
	PRIMARY KEY CLUSTERED
		(RowSeq)
		WITH FILLFACTOR = 100,
	UNIQUE NONCLUSTERED
		(FileId)
		WITH FILLFACTOR = 100);
GO
------------------------------
-- Define and assign control-level variables.
------------------------------

-- Create and populate script-level variables.
DECLARE	@ScriptPrefix		varchar(14),
		@ScriptInstanceGUID	uniqueidentifier,
		@ScriptServerName	nvarchar(128),
		@ScriptDBName		nvarchar(128),
		@RunDateTime		datetime2(7),
		@Today				date,
		@UserName			nvarchar(128),
		@IssueList			varchar(4000),
		@RollbackTran		bit;

-- Assign run-time variables from stored data.
BEGIN TRY
	SELECT	@ScriptPrefix = ScriptPrefix,
			@ScriptInstanceGUID = ScriptInstanceGUID,
			@ScriptServerName = ScriptServerName,
			@ScriptDBName = ScriptDBName,
			@RunDateTime = RunDateTime,
			@Today = RunDateTime,
			@UserName = UserName,
			@IssueList = LTRIM(RTRIM(IssueList)),
			@RollbackTran = RollbackTran
	FROM	#Instance;
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES (N'#Instance - Assign variables', ERROR_MESSAGE())
END CATCH

-- Identifying text used for file titles, email titles, etc.
DECLARE	@Title				varchar(4000) = 'Script Template - Your title here';							-- <<--- Change this

DECLARE	@NoActivityEmail_To	varchar(4000) = NULL,															-- <<--- Change this
		@Email_To			varchar(4000) = 'VFrank',										-- <<--- Change this
		@Email_CC			varchar(4000) = NULL,															-- <<--- Change this
		@Email_Subject		varchar(4000) = '(' + 
											CASE
											WHEN @ScriptDBName NOT IN (N'TRSProd', N'TRSTest', N'AppTrackingProd', N'AppTrackingTest')
											THEN @ScriptServerName + N'.'
											ELSE SPACE(0)
											END + @ScriptDBName + ') ' + @Title,
		@Email_Body			varchar(max) = SPACE(0),
		@Email_Attachments	varchar(max) = SPACE(0);

DECLARE	@IssueNumber		int = NULL,						-- Filled with first entry in @IssueList
		@DBASessionId		int = NULL,
		@RC					int = NULL,
		@Error				int = NULL,
		@ErrMsg				nvarchar(2048) = NULL,
		@RowCount			int = NULL,
		@ExpectedCount		int = NULL,
		@TranCount			int = NULL,						-- Used during Failed: label processing and for 'fall-through' processing
		@SQL				nvarchar(max) = NULL,
		@CRLF				char(2) = CHAR(13) + CHAR(10),
		@Comma				char(1) = ',',
		@SQuote				char(1) = '''',
		@DQuote				char(1) = '"',
		@CommaN				nchar(1) = N',',
		@SQuoteN			nchar(1) = N'''',
		@DQuoteN			nchar(1) = N'"';

------------------------------
-- For scripts with one or more Issue#'s, select the *first( one for the #IssueNumber variable, if not already assigned.
------------------------------

BEGIN TRY
	IF LEN(@IssueList) > 0
	AND @IssueNumber IS NULL
	BEGIN;
		SELECT @IssueNumber = CONVERT(int, Item)
		FROM	TRSDefault.dbo.f_DelimitedSplit8K (@IssueList, ',')
		WHERE	ItemNumber = 1;
	END;
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg) VALUES(N'Set @IssueNumber variable', ERROR_MESSAGE());
END CATCH

-- Note: If human-supplied @IssueNumber exists, ensure the @IssueList variable is populated.
BEGIN TRY
IF @IssueNumber > 0
AND LEN(ISNULL(@IssueList, SPACE(0))) = 0
	SET @IssueList = CONVERT(varchar(10), @IssueNumber);
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg) VALUES(N'Set @IssueList variable', ERROR_MESSAGE());
END CATCH

------------------------------

-- Print ProcessingLog headings.
BEGIN TRY
	-- List the title information.
	PRINT	TRSDefault.dbo.f_CSVText('TRS Research');													-- <<--- Change this
	PRINT	TRSDefault.dbo.f_CSVText(@Title);

	-- If Issue#'s are provided, list them. Ensure a comma and space seperate multiple Issue numbers.
	IF LEN(@IssueList) > 0
		PRINT TRSDefault.dbo.f_CSVText('Issue(s): ' + REPLACE(REPLACE(@IssueList, ',', ', '), ',  ', ', '));

	-- List the DB against which the script is running. If not 'TRSProd', 'TRSTest', 'AppTrackingProd', or 'AppTrackingTest', list the server name, as well.
	PRINT	N'DB: ' + 
				CASE
				WHEN @ScriptDBName NOT IN (N'TRSProd', 'NTRSTest', N'AppTrackingProd', N'AppTrackingTest')
				THEN @ScriptServerName + N'.'
				ELSE SPACE(0)
				END + @ScriptDBName;

	-- List the starting time.
	PRINT	'Run: ' + CONVERT(char(23), @RunDatetime, 121)

	-- List the user executing the script.
	PRINT	'User: ' + @UserName;

	-- Filler, before additional contents.
	PRINT	SPACE(0);
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES (N'Write ProcessingLog headings', ERROR_MESSAGE());
END CATCH

-- Exit if any error has occurred.
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

------------------------------
-- Define variables
------------------------------

--...

-- Generic results file.
DECLARE	@ResultsPath				varchar(255) = '\\localhost\Import2000\Ad-hoc\Results\' + @ScriptPrefix,
		@ResultsFileName_Template	varchar(255) = @ScriptPrefix + '.' + @ScriptDBName + '.' + '@VariableToReplace@' + '.'+ TRSDefault.dbo.f_DBAFileDateStamp(@RunDateTime) + '.csv',
		@ResultsFileName			varchar(255) = @ScriptPrefix + '.' + @ScriptDBName + '.' + TRSDefault.dbo.f_DBAFileDateStamp(@RunDateTime) + '.csv',
		@ResultsFileId				int;

------------------------------
-- Define external, persisted tables
------------------------------

--...

------------------------------
-- Define table variables
------------------------------
		
-- Holds info about the script's processing.
DECLARE @ProcessingLog TABLE		-- duplicate of DBAProcessingLog table; used for internal data-gathering.
		(RowSeq			int				NOT NULL IDENTITY(1, 1),
		DateTimeStamp	datetime2(7)	NOT NULL DEFAULT(SYSDATETIME()),
		ServerName		nvarchar(128)	NULL,
		DBName			nvarchar(128)	NULL,
		RowsAffected	bigint			NULL,
		Item			nvarchar(2048)	NOT NULL,
		Msg				nvarchar(2048)	NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);

--...

------------------------------
-- Define tables
------------------------------
		
--... 


------------------------------
-- Note beginning of actual processing.
INSERT @ProcessingLog (Item) VALUES('Processing started');		-- Logging
------------------------------

-- Extract parameters and assign to variables or working tables.
BEGIN;

	-- Extract the 'EmailTo' email addresses.
	BEGIN TRY
		SET @Email_To = 
				ISNULL((SELECT	ValueList
						FROM	#Parameter
						WHERE	Keyword = 'EmailTo'),
						NULL);
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'Set @EmailTo', ERROR_MESSAGE());
	END CATCH

	--...

	INSERT @ProcessingLog (Item) OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item) VALUES('Extract #Parameter value(s)');		-- Logging

	------------------------------
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
	------------------------------

	-- Check validity of extracted parameter(s).
	BEGIN TRY
		IF @Email_To IS NULL
			INSERT #Error(Item, Msg)
			VALUES(N'@Email_To', N'Parameter value not supplied');

	--...

		INSERT @ProcessingLog (Item) OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item) VALUES('Validate #Parameter value(s)');		-- Logging
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'Analyize assigned parameter(s)', ERROR_MESSAGE());
	END CATCH
END;
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

--...


------------------------------
-- Send 'No Activity' email, if condition(s) met.
------------------------------
IF 1 = 0																								-- <<--- Change this
	GOTO SendNoActivityEmail;


------------------------------
-- Start a transaction.
------------------------------
IF @RollbackTran IS NOT NULL
BEGIN;
	BEGIN TRY
		BEGIN TRAN;	
		INSERT @ProcessingLog (Item) 
			OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item) 
		VALUES(N'BEGIN TRANSACTION');	-- Logging
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES (N'BEGIN TRANSACTION', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed; 
END;

------------------------------
-- Create a DBASession.
------------------------------
IF @DBASessionId IS NULL
BEGIN;
	BEGIN TRY
		EXEC @RC = TRSDefault.dbo.s_DBASessionINS
						@SessionDateTime = @RunDateTime,
						@DBName = @ScriptDBName,
						@IssueNumber = @IssueNumber,
						@ScriptInstanceGUID = @ScriptInstanceGUID,
						@DBASessionId = @DBASessionId OUT;
		IF @RC = 0
		BEGIN;
			INSERT @ProcessingLog (Item, Msg) 
				OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg) 
			VALUES (SYSDATETIME(), N's_DBASessionINS', N'DBASessionId: ' + CONVERT(nvarchar(10), @DBASessionId));	-- Logging
			UPDATE #Instance SET DBASessionId = @DBASessionId;	-- Retain the DBA session ID
		END;
		ELSE
		BEGIN;
			INSERT #Error (Item, Msg)
			VALUES(N's_DBASessionINS', N'RC: ' + CONVERT(nvarchar(10), @RC));
		END;
	END TRY
	BEGIN CATCH
		INSERT #Error(Item, Msg)
		VALUES(N's_DBASessionINS',  ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

------------------------------
-- Store persisted tables, if not done yet.
------------------------------

--...


------------------------------
-- Write report(s)
------------------------------

-- Create the 'Results' file.
-- Store an entry in the #ExportQueue table.
BEGIN TRY
	SET @RC = 0;
	EXEC @RC = TRSDefault.dbo.s_DBAFileINS
				@FilePath = @ResultsPath,
				@FileName = @ResultsFileName,
				@DBASessionId = @DBASessionId,
				@FileId = @ResultsFileId OUT;
	IF @RC = 0
	BEGIN;
		INSERT @ProcessingLog (Item, Msg) 
			OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
		VALUES(N'Create file (Results)', N'FileId: ' + CONVERT(nvarchar(10), @ResultsFileId) + N' FileName: ' + @ResultsFileName);	-- Logging
		IF OBJECT_ID('tempdb..#ExportQueue') IS NOT NULL
			INSERT #ExportQueue (Legend, FileId) VALUES('Results', @ResultsFileId);
	END;
	ELSE
	BEGIN;
		INSERT #Error (Item, Msg)
		VALUES (N's_DBAFileINS (Results)', N'Results file creation failed', N'RC: ' + CONVERT(nvarchar(10), @RC));
	END;
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N's_DBAFileINS (Results)', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Write results file heading.
-- Note: Do NOT store NULL lines.
BEGIN TRY
	INSERT TRSDefault.dbo.DBAFileRow
			(FileId, Contents)
		OUTPUT INSERTED.Contents						-- Debugging, output report file headings to ProcessingLog
	SELECT	FileId = @ResultsFileId,
			Contents = TRSDefault.dbo.f_CSVText(A0.Contents)
	FROM	(VALUES	(1, 'TRS Research'),													-- <<--- Change this
					(2, TRSDefault.dbo.f_CSVText(@Title)),
					(3, TRSDefault.dbo.f_CSVText
							('Issue(s): ' + NULLIF(@IssueList, SPACE(0)))),						-- Note: Only if contains something
					(4, TRSDefault.dbo.f_CSVText
							('DB: ' + 
							CASE
							WHEN @ScriptDBName = N'TRSProd'
							THEN @ScriptDBName
							ELSE @ScriptServerName + N'.' + @ScriptDBName
							END)),
					(5, 'Run: ' + CONVERT(char(23), @RunDatetime, 121)),
					(6, TRSDefault.dbo.f_CSVText('User: ' + NULLIF(@UserName, SPACE(0)))),							-- Note: Only if contains something
					(7, SPACE(0))) A0 (Seq, Contents)
	WHERE	A0.Contents IS NOT NULL
	ORDER BY A0.Seq;

	INSERT @ProcessingLog (RowsAffected, Item, Msg) 
		OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
	VALUES(@@ROWCOUNT, N'FileId ' + CONVERT(nvarchar(10), @ResultsFileId), N'Write file headings');	-- Logging
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES(N'Write results file headings', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;

-- Write detail column headings, and detail
BEGIN TRY
	INSERT TRSDefault.dbo.DBAFileRow
			(FileId, Contents)
		OUTPUT INSERTED.Contents						-- Debugging, output report detail column headings to ProcessingLog
	VALUES	(@ResultsFileId, 'Column headings here');

	INSERT TRSDefault.dbo.DBAFileRow
			(FileId, Contents)
		OUTPUT INSERTED.Contents						-- Debugging, output report detail headings to ProcessingLog
	SELECT	FileId = @ResultsFileId,
			Contents = 'Detail contents here'
	--FROM	YourTable
	--ORDER BY YourKey(s)

	INSERT @ProcessingLog (RowsAffected, Item, Msg) 
		OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
	VALUES(@@ROWCOUNT, N'FileId ' + CONVERT(nvarchar(10), @ResultsFileId), N'Write report detail');	-- Logging
END TRY
BEGIN CATCH
	INSERT #Error (Item, Msg)
	VALUES (N'Write results detail', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error)
	GOTO Failed;
	
--... Write additional report(s).

------------------------------------------------------------
------------------------------------------------------------

------------------------------
-- Transaction completion processing.
------------------------------

-- If a rollback has been specified, perform it, then raise an error, so the script-runner will be SURE the changes were not persisted.
IF @@TRANCOUNT > 0
BEGIN;
	BEGIN TRY
		IF @RollbackTran = 1
		BEGIN;
			WHILE @@TRANCOUNT > 0
				ROLLBACK TRAN; 

			BEGIN TRY
				-- Persist any @ProcessingLog entries.
				INSERT TRSDefault.dbo.DBAProcessingLog		-- Logging
						(DateTimeStamp,
						ScriptPrefix,
						ScriptInstanceGUID,
						ServerName,
						DBName,
						RowsAffected,
						Item,
						Msg)
				SELECT	DateTimeStamp,
						@ScriptPrefix,
						@ScriptInstanceGUID,
						ServerName,
						DBName,
						RowsAffected,
						Item,
						Msg
				FROM	@ProcessingLog
				ORDER BY RowSeq;
			END TRY
			BEGIN CATCH
				INSERT #Error (Item, Msg)
				VALUES	(N'DBAProcessingLog - Insert (@ProcessingLog)', ERROR_MESSAGE());
			END CATCH
			IF EXISTS(SELECT 1 FROM #Error)
				GOTO Failed;

			-- Note completion in ProcessingLog.
			BEGIN TRY
				INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
						(DateTimeStamp,
						ScriptPrefix,
						ScriptInstanceGUID,
						ServerName,
						DBName,
						RowsAffected,
						Item,
						Msg)
					OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
				VALUES	(SYSDATETIME(),
						@ScriptPrefix,
						@ScriptInstanceGUID,
						@ScriptServerName,
						@ScriptDBName,
						CONVERT(int, NULL),
						N'ROLLBACK TRANSACTION (Script completed: Rolled back for testing)',
						N'Duration (seconds): ' + CONVERT(nvarchar(10), DATEDIFF(SECOND, @RunDateTime, SYSDATETIME())));
			END TRY
			BEGIN CATCH
				INSERT #Error (Item, Msg)
				VALUES	(N'DBAProcessingLog - Insert (Script completed)', ERROR_MESSAGE());

				INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
						(DateTimeStamp,
						ScriptPrefix,
						ScriptInstanceGUID,
						ServerName,
						DBName,
						RowsAffected,
						Item,
						Msg)
					OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
				VALUES	(SYSDATETIME(),
						@ScriptPrefix,
						@ScriptInstanceGUID,
						@ScriptServerName,
						@ScriptDBName,
						CONVERT(int, NULL),
						N'ROLLBACK TRANSACTION (Script completed: Rolled back for testing)',
						ERROR_MESSAGE());
			END CATCH
		END;
		ELSE
		BEGIN;
			COMMIT TRAN; 
			INSERT @ProcessingLog (Item, Msg) 
				OUTPUT TRSDefault.dbo.f_CSVText(INSERTED.Item) AS Item, TRSDefault.dbo.f_CSVText(INSERTED.Msg) AS Msg
			VALUES(N'COMMIT TRANSACTION', N'Duration (seconds): ' + CONVERT(nvarchar(10), DATEDIFF(SECOND, @RunDateTime, SYSDATETIME())));
		END;
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES	(N'Commit / Rollback Transaction', ERROR_MESSAGE());

		INSERT TRSDefault.dbo.DBAProcessingLog		-- Logging
				(DateTimeStamp,
				ScriptPrefix,
				ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg)
			OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
		VALUES	(SYSDATETIME(),
				@ScriptPrefix,
				@ScriptInstanceGUID,
				@ScriptServerName,
				@ScriptDBName,
				CONVERT(int, NULL),
				N'COMMIT TRANSACTION',
				ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;		

	-- Alert the script-runner to the rollback.
	IF @RollbackTran = 1
	BEGIN;
		RAISERROR('Transaction rolled back for testing', 18, 1); 
		GOTO DoneAfterRollback;		-- Note: This should NOT execute. The RAISERROR ahould end processing.
	END;
END;

------------------------------
-- Export files in #ExportQueue table.
------------------------------

-- Generate and execute the SQL to export all files in the table.
IF OBJECT_ID('tempdb..#ExportQueue') IS NOT NULL
BEGIN;
	BEGIN TRY
		SET @SQL = 'DECLARE @RC int, @ErrMsg nvarchar(2048); ';

		SELECT @SQL = @SQL + 
				CASE
				WHEN LEN(@SQL) > 0
				THEN CHAR(13) + CHAR(10)
				ELSE SPACE(0)
				END + 
				N'BEGIN TRY ' + CHAR(13) + CHAR(10) +
					N'SELECT @RC = 0, @ErrMsg = NULL; ' + CHAR(13) + CHAR(10) +
					N'EXEC @RC = TRSDefault.dbo.s_DBAFile_ExportSimple @FileId = ' + CONVERT(nvarchar(10), FileId) + N', @ErrMsg = @ErrMsg OUT; ' + CHAR(13) + CHAR(10) +
					N'IF @RC > 0 ' + CHAR(13) + CHAR(10) +
					N'OR LEN(@ErrMsg) > 0' + CHAR(13) + CHAR(10) + 
						N'INSERT #DynamicError (ServerName, DBName, Item, Msg) VALUES(N''' + @ScriptServerName + N''', N''' + @ScriptDBName + N''', N''s_DBAFile_ExportSimple FileId: ' + CONVERT(nvarchar(10), FileId) + N' RC: ' + CONVERT(nvarchar(10), @RC) + N''', ISNULL(@ErrMsg, SPACE(0))); ' + CHAR(13) + CHAR(10) +
					N'ELSE ' + CHAR(13) + CHAR(10) + 
						N'PRINT ''File ' + CONVERT(nvarchar(10), FileId) + N' exported.'';' + CHAR(13) + CHAR(10) + 
				N'END TRY ' + CHAR(13) + CHAR(10) +
				N'BEGIN CATCH ' + CHAR(13) + CHAR(10) +
					N'INSERT #DynamicError (ServerName, DBName, Item, Msg) VALUES(N''' + @ScriptServerName + N''', N''' + @ScriptDBName + ''', N''s_DBAFile_ExportSimple FileId: ' + CONVERT(nvarchar(10), FileId) + N' RC: ' + CONVERT(nvarchar(10), @RC) + N''', ERROR_MESSAGE()); ' + CHAR(13) + CHAR(10) +
				N'END CATCH'
		FROM	#ExportQueue
		ORDER BY RowSeq;

		EXEC(@SQL);

		-- Log export completion.
		INSERT @ProcessingLog (Item) OUTPUT INSERTED.Item VALUES('File export completed.');
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg) VALUES(N'#ExportQueue - Script export', ERROR_MESSAGE())
		SELECT @SQL;	-- list offending SQL statement(s)

		INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
				(DateTimeStamp,
				ScriptPrefix,
				ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg)
			OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
		VALUES	(SYSDATETIME(),
				@ScriptPrefix,
				@ScriptInstanceGUID,
				@ScriptServerName,
				@ScriptDBName,
				CONVERT(int, NULL),
				N'#ExportQueue - Script export',
				ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
	OR EXISTS(SELECT 1 FROM #DynamicError)
		GOTO Failed;
END;
		
-- Email the database Results report to the responsible parties.
BEGIN;
	SET @RC = 0;

	-- Pick at least one of the methods below, to pass exported files to their recipient(s).
	--/*
	-- Add file linkage(s) in the #ExportQueue table the to email body.
	IF OBJECT_ID('tempdb..#ExportQueue') IS NOT NULL
	BEGIN;
		BEGIN TRY
			SELECT	@Email_Body = ISNULL(@Email_Body, SPACE(0)) +
						CASE
						WHEN LEN(ISNULL(@Email_Body, SPACE(0))) > 0
						THEN REPLICATE(CHAR(13) + CHAR(10), 2)
						ELSE SPACE(0)
						END +
						CASE
						WHEN LEN(eq.Legend) > 0
						THEN eq.Legend + CHAR(13) + CHAR(10)
						ELSE SPACE(0)
						END + '"' + dfp.FilePath + '\' + df."FileName" + '"'
			FROM	#ExportQueue eq
					INNER JOIN TRSDefault.dbo.DBAFile df
						ON df.FileId = eq.FileId
						INNER JOIN TRSDefault.dbo.DBAFilePath dfp
							ON dfp.FilePathId = df.FilePathId
			ORDER BY eq.RowSeq;
			INSERT @ProcessingLog (Item) OUTPUT INSERTED.Item VALUES(N'Append file links to email body');
		END TRY
		BEGIN CATCH
			INSERT #Error (Item, Msg) VALUES(N'#ExportQueue -Append Email Body', ERROR_MESSAGE());
		END CATCH
		IF EXISTS(SELECT 1 FROM #Error)
			GOTO Failed;
	END;
	--*/
	--/*
	-- Attach the file(s) in the #ExportQueue table to the attachments list.
	IF OBJECT_ID('tempdb..#ExportQueue') IS NOT NULL
	BEGIN;
		BEGIN TRY
			SELECT @Email_Attachments = ISNULL(@Email_Attachments, SPACE(0)) +
						CASE
						WHEN LEN(ISNULL(@Email_Attachments, SPACE(0))) > 0
						THEN ';'
						ELSE SPACE(0)
						END + dfp.FilePath + '\' + df."FileName"
			FROM	#ExportQueue eq
					INNER JOIN TRSDefault.dbo.DBAFile df
						ON df.FileId = eq.FileId
						INNER JOIN TRSDefault.dbo.DBAFilePath dfp
							ON dfp.FilePathId = df.FilePathId
			ORDER BY eq.RowSeq;
			INSERT @ProcessingLog (Item) OUTPUT INSERTED.Item VALUES(N'Append email attachment(s)');
		END TRY
		BEGIN CATCH
			INSERT #Error (Item, Msg) VALUES(N'#ExportQueue - Append Email Attachments', ERROR_MESSAGE());
		END CATCH
		IF EXISTS(SELECT 1 FROM #Error)
			GOTO Failed;
	END;
	--*/
	BEGIN TRY
		EXEC @RC = TRSDefault.dbo.s_DBAEmailINS
						@ToList = @Email_To,
						@CCList = @Email_CC,
						@Subject = @Email_Subject,
						@Body = @Email_Body,
						@AttachmentList = @Email_Attachments,
						@Priority = 'NORMAL';
		IF @RC = 0
		BEGIN;
			INSERT @ProcessingLog (Item) OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item) VALUES('Results email queued.');
		END;
		ELSE 
		BEGIN;
			INSERT #Error (Item, Msg)
			VALUES(N's_DBAEmailINS', N'RC: ' + CONVERT(nvarchar(10), @RC));
		END;
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES (N's_DBAEmailINS', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;


------------------------------------------------------------
------------------------------------------------------------

------------------------------
-- Finalization processing, after this point.
------------------------------
GOTO Done;

------------------------------
-- Failure processing
------------------------------

Failed:

	-- Note count of active transaction(s), if any.
	SET @TranCount = @@TRANCOUNT;

	-- List generic error information.
	IF @Error <> 0
		PRINT 'Error: ' + CONVERT(varchar(10), @Error);
	IF LEN(@ErrMsg) > 0
		PRINT	'ErrMsg: ' + @ErrMsg;

	-- Assign run-time variables from stored data.
	IF @ScriptPrefix IS NULL
	BEGIN;
		BEGIN TRY
			SELECT	@ScriptPrefix = ScriptPrefix,
					@ScriptInstanceGUID = ScriptInstanceGUID,
					@ScriptServerName = ScriptServerName,
					@ScriptDBName = ScriptDBName,
					@RunDateTime = RunDateTime,
					@Today = RunDateTime,
					@UserName = UserName,
					@IssueList = IssueList,
					@RollbackTran = RollbackTran
			FROM	#Instance;
			print 'ScriptPrefix: ' + ISNULL(@ScriptPrefix, 'NULL');select * from #Instance;	--debug
		END TRY
		BEGIN CATCH
			INSERT #Error (Item, Msg)
			VALUES (N'#Instance - Assign variables', ERROR_MESSAGE())
		END CATCH
	END;

	-- Save any entry in the #DynamicProcessingLog to the table variable.
	IF OBJECT_ID('tempdb..#DynamicProcessingLog') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #DynamicProcessingLog)
		BEGIN;
			BEGIN TRY
				INSERT @ProcessingLog
						(DateTimeStamp,
						ServerName,
						DBName,
						RowsAffected,
						Item,
						Msg)
				SELECT	DateTimeStamp,
						ServerName,
						DBName,
						RowsAffected,
						Item,
						Msg
				FROM	#DynamicProcessingLog
				ORDER BY RowSeq;

				-- Clean up the #DynamicProcessingLog.
				TRUNCATE TABLE #DynamicProcessingLog;
			END TRY
			BEGIN CATCH
				INSERT #Error (Item, Msg)
				VALUeS (N'Failed: @ProcessingLog - Insert from #DynamicProcessingLog', ERROR_MESSAGE());

				-- Clean up the #DynamicProcessingLog.
				TRUNCATE TABLE #DynamicProcessingLog;
			END CATCH
		END;
	END;

	-- List the contents of the #DynamicError table, if it exists.
	IF OBJECT_ID('tempdb..#DynamicError') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #DynamicError)
		BEGIN;
			BEGIN TRY
				PRINT	'Dynamic SQL Error(s)';
				SELECT	"Server" = TRSDefault.dbo.f_CSVText(ServerName),
						"DB" = TRSDefault.dbo.f_CSVText(DBName),
						"Item" = TRSDefault.dbo.f_CSVText(Item),
						"Msg" = TRSDefault.dbo.f_CSVText(Msg)
				FROM	#DynamicError
				ORDER BY RowSeq;

				TRUNCATE TABLE #DynamicError;			-- Clear it out, so we don't pick it up again, in 'Fall-through' checking.
			END TRY
			BEGIN CATCH
				PRINT	TRSDfault.dbo.f_CSVText(N'List #DynamicError contents: ' + ERROR_MESSAGE());
			END CATCH
			INSERT @ProcessingLog (Item) VALUES(N'List #DynamicError');
		END;
	END;

	-- List the contents of the #Error table, if it exists.
	IF OBJECT_ID('tempdb..#Error') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #Error)
		BEGIN;
			BEGIN TRY
				PRINT	'Error(s)';
				SELECT	"Item" = TRSDefault.dbo.f_CSVText(Item),
						"Msg" = TRSDefault.dbo.f_CSVText(Msg)
				FROM	#Error
				ORDER BY RowSeq;

				TRUNCATE TABLE #Error;				-- Clear it out, so we don't pick it up again, in 'Fall-through' checking.
			END TRY
			BEGIN CATCH
				PRINT	TRSDfault.dbo.f_CSVText(N'List #Error contents: ' + ERROR_MESSAGE());
			END CATCH
			INSERT @ProcessingLog (Item) VALUES(N'List #Error');
		END;
	END;

	-- If one or more transactions are currently active, roll them back.
	-- If the @ProcessingLog table exists, and has entries, persist them.
	IF @TranCount > 0
	BEGIN;
		BEGIN TRY
			WHILE @@TRANCOUNT > 0
				ROLLBACK TRAN;
		END TRY
		BEGIN CATCH
			PRINT	TRSDefault.dbo.f_CSVText('Rollback Tran - FAILED: ' + ERROR_MESSAGE());
		END CATCH
	END;

	----------

	-- Persist any @ProcessingLog entries.
	BEGIN TRY
		INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
				(DateTimeStamp,
				ScriptPrefix,
				ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg)
		SELECT	DateTimeStamp,
				ScripiptPrefix = ISNULL(@ScriptPrefix, 'NULL'),
				ScriptInstanceGUID = ISNULL(@ScriptInstanceGUID, 'NULL'),
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg
		FROM	@ProcessingLog
		ORDER BY RowSeq;
	END TRY
	BEGIN CATCH
		PRINT TRSDefault.dbo.f_CSVText(N'DBAProcessingLog - Insert (@ProcessingLog - Script failed): ' + ERROR_MESSAGE());
	END CATCH

	-- Note failure in ProcessingLog.
	BEGIN TRY
		INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
				(DateTimeStamp,
				ScriptPrefix,
				ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg)
			OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
		VALUES	(SYSDATETIME(),
				@ScriptPrefix,
				@ScriptInstanceGUID,
				@ScriptServerName,
				@ScriptDBName,
				CONVERT(int, NULL),
				CASE @TranCount
				WHEN 0
				THEN N'Script completed (Failed): No transaction active'
				ELSE N'Script completed (Failed): Transaction active'
				END,
				N'Duration (seconds): ' + CONVERT(nvarchar(10), DATEDIFF(SECOND, @RunDateTime, SYSDATETIME())));
	END TRY
	BEGIN CATCH
		PRINT TRSDefault.dbo.f_CSVText(N'DBAProcessingLog - Insert (Script failed): ' + ERROR_MESSAGE());
	END CATCH

	----------

	IF @TranCount > 0
		RAISERROR('Transaction rolled back after error!', 18, 1)
	ELSE 
		RAISERROR('Script failed!', 18, 1);

	GOTO DoneAfterRollback;	-- Shouldn't be necessary, or executable.

------------------------------------------------------------
------------------------------------------------------------

------------------------------
-- Send 'No Activity' email, if condition(s) met.
------------------------------
SendNoActivityEmail:
	-- Email the database Results report to the responsible parties.
	BEGIN TRY
		SET @RC = 0;
		SET @Email_Subject = N'Nothing to report ' + @Email_Subject;

		EXEC @RC = TRSDefault.dbo.s_DBAEmailINS
						@ToList = @NoActivityEmail_To,
						@Subject = @Email_Subject,
						@Priority = 'NORMAL';
		IF @RC = 0
		BEGIN;
			INSERT @ProcessingLog (Item, Msg) OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item) VALUES(N's_DBAEmailINS', N'No Activity email queued.');		-- Logging
		END;
		ELSE 
		BEGIN;
			INSERT #Error (Item, Msg)
			VALUES(N's_DBAEmailINS - No Activity email', N'RC: ' + CONVERT(nvarchar(10), @RC));
		END
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N's_DBAEmailINS - No Activity email', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;

------------------------------
-- End-of-script processing.
------------------------------
Done:

	-- Persist the #ProcessingLog entries.
	BEGIN TRY
		INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
				(DateTimeStamp,
				ScriptPrefix,
				ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg)
		SELECT	DateTimeStamp,
				@ScriptPrefix,
				@ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg
		FROM	@ProcessingLog
		ORDER BY RowSeq;

		-- Clean up the ProcesingLog.
		DELETE @ProcessingLog;
	END TRY
	BEGIN CATCH
		PRINT TRSDefault.dbo.f_CSVText(N'DBAProcessingLog - Insert (Done): ' + ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;

	-- Note completion in ProcessingLog.
	BEGIN TRY
		INSERT TRSDefault.dbo.DBAProcessingLog	-- Logging
				(DateTimeStamp,
				ScriptPrefix,
				ScriptInstanceGUID,
				ServerName,
				DBName,
				RowsAffected,
				Item,
				Msg)
			OUTPUT TRSDefault.dbo.f_CSVDateTime(INSERTED.DateTimeStamp), TRSDefault.dbo.f_CSVText(INSERTED.Item), TRSDefault.dbo.f_CSVText(INSERTED.Msg)
		VALUES	(SYSDATETIME(),
				@ScriptPrefix,
				@ScriptInstanceGUID,
				@ScriptServerName,
				@ScriptDBName,
				CONVERT(int, NULL),
				N'Script completed (Succeeded)',
				N'Duration (seconds): ' + CONVERT(nvarchar(10), DATEDIFF(SECOND, @RunDateTime, SYSDATETIME())));
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'DBAProcessingLog - Insert (Script completed): ' + ERROR_MESSAGE();
		RAISERROR(@ErrMsg, 18, 1);
	END CATCH

DoneAfterRollback:
GO

------------------------------
-- Fall-through processing: Detect and report any fall-through.
------------------------------

	DECLARE	@TranCount	int = @@TRANCOUNT,
			@FallThru	bit = 
							CASE 
							WHEN @@TRANCOUNT > 0 
							THEN 1 
							WHEN OBJECT_ID('tempdb..#DynamidError') IS NOT NULL 
							THEN CASE
								 WHEN EXISTS(SELECT 1 FROM #DynamicError)
								 THEN 1
								 ELSE 0
								 END
							WHEN OBJECT_ID('tempdb..#Error') IS NOT NULL
							THEN CASE
								 WHEN EXISTS(SELECT 1 FROM #Error)
								 THEN 1
								 ELSE 0
								 END
							ELSE 0 
							END;
			

	-- List the contents of the #DynamicError table, if it exists, and has entries.
	IF OBJECT_ID('tempdb..#DynamicError') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #DynamicError)
		BEGIN;
			BEGIN TRY
				PRINT	'Dynamic Error(s) - after fall-through';
				SELECT	"Server" = TRSDefault.dbo.f_CSVText(ServerName),
						"DB" = TRSDefault.dbo.f_CSVText(DBName),
						"Item" = TRSDefault.dbo.f_CSVText(Item),
						"Msg" = TRSDefault.dbo.f_CSVText(Msg)
				FROM	#DynamicError
				ORDER BY RowSeq;

				-- Clean up the #DynamicError table.
				TRUNCATE TABLE #DynamicError;
			END TRY
			BEGIN CATCH
				PRINT	TRSDfault.dbo.f_CSVText(N'List #DynamicError contents: ' + ERROR_MESSAGE());
			END CATCH
		END;
	END;

	-- List the contents of the #Error table, if it exists, and has entries.
	IF OBJECT_ID('tempdb..#Error') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #Error)
		BEGIN;
			BEGIN TRY
				PRINT	'Error(s) - after fall-through';
				SELECT	"Item" = TRSDefault.dbo.f_CSVText(Item),
						"Msg" = TRSDefault.dbo.f_CSVText(Msg)
				FROM	#Error
				ORDER BY RowSeq;

				-- Clean up the #Error table.
				TRUNCATE TABLE #Error;
			END TRY
			BEGIN CATCH
				PRINT	TRSDfault.dbo.f_CSVText(N'List #Error contents: ' + ERROR_MESSAGE());
			END CATCH
		END;
	END;

	-- Roll back any transactions, if they exist.
	IF @TranCount > 0
	BEGIN;
		BEGIN TRY
			WHILE @@TRANCOUNT > 0
				ROLLBACK TRAN;
		END TRY
		BEGIN CATCH
			PRINT	TRSDefault.dbo.f_CSVText('Rollback Tran - FALL-THROUGH: ' + ERROR_MESSAGE());
		END CATCH
	END;

	-- Raise appropriate error, if needed.
	IF @TranCount > 0
		RAISERROR('Transaction rolled back after fall-through!', 18, 1);
	ELSE
	BEGIN;
		IF @FallThru = 1
			RAISERROR ('Script suffered a ''Fall-through'' problem!', 18, 1);
	END;
GO
------------------------------
-- End of Script
------------------------------
	
