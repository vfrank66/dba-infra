-- This script is intended to be run on localhost.DBADefault.
-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET CONCAT_NULL_YIELDS_NULL ON;SET QUOTED_IDENTIFIER ON;SET XACT_ABORT ON;
GO

DECLARE	@RC						int,
		@ErrMsg					nvarchar(2048),
		@ServerName				nvarchar(128),
		@DBName					nvarchar(128),
		@RunDateTime			datetime,
		@SessionCreateDateTime	datetime,
		@DBASessionId			int;

-- Assign run-time constants to variables.		
SELECT	@RunDateTime = GETDATE(),
		@ServerName = CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('ServerName'))),
		@DBName = DB_NAME();

PRINT	'DBA SQL Server Agent Logging';
PRINT	'Prepare for SQL Server Agent Logging Reporting Run';
PRINT	'DB: ' + CONVERT(varchar(128), @ServerName) + '.' + @DBName;
PRINT	'Run: ' + CONVERT(char(23), @RunDateTime, 121);
PRINT	SPACE(0);

DECLARE	@SQL_ExtractTemlate		nvarchar(max),
		@SQL_Extract			nvarchar(max);
		
-- Create the checkpoint table, if necessary.
IF OBJECT_ID('DBADefault.dbo.z20140514_01_Logging') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'z20140514_01_Logging - Create';
		CREATE TABLE DBADefault.dbo.z20140514_01_Logging
				(DBASessionId			int				NOT NULL,
				SessionCreateDateTime	datetime		NOT NULL,
				ExtractCompleteDatetime	datetime		NULL,
				CleanupCompleteDateTime	datetime		NULL,
				LoggingCompleteDateTime	datetime		NULL);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'z20140514_01_Logging - Create: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		GOTO Failed;
END;

-- Ensure this is running in the proper environment.
IF @ServerName <> 'localhost'
OR @DBName <> 'DBADefault'
BEGIN;
	SET @ErrMsg = N'Run against localhost.DBADefault';
	GOTO Failed;
END;

-- If starting a new run, empty thecheckpoint table.
IF EXISTS(
	SELECT	1
	FROM	DBADefault.dbo.z20140514_01_Logging
	WHERE	LoggingCompleteDateTime IS NOT NULL)
BEGIN;
	BEGIN TRY
		PRINT	'z20140514_01_Logging - Delete';
		DELETE	DBADefault.dbo.z20140514_01_Logging;
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'z20140514_01_Logging - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		GOTO Failed;
END;

------------------------------

-- If the previous execution did not complete successfully, retrieve the stored DBASessionId for continued use.
-- Also, retrieve and display the original starting date/time.
IF EXISTS(
	SELECT	1
	FROM	DBADefault.dbo.z20140514_01_Logging
	WHERE	LoggingCompleteDateTime IS NULL
	AND		DBASessionId IS NOT NULL)
BEGIN;
	BEGIN TRY
		SELECT	@DBASessionId = DBASessionId,
				@SessionCreateDateTime = SessionCreateDateTime
		FROM	DBADefault.dbo.z20140514_01_Logging;
		
		PRINT	'Retrieved SessionId:  ' + CONVERT(varchar(9), @DBASessionId);
		PRINT	'Session intially started: ' + CONVERT(char(23), @SessionCreatDateTime, 121);
		PRINT	SPACE(0);

	END  TRY
	BEGIN CATCH
		SET @ErrMsg = N'z20140514_01_Logging - Retrieve DBASessionId: ' + ERROR_MESSAGE();
	END CATCH
	iF LEN(@ErrMsg) > 0
		GOTO Failed;
END;

-- Get new SessionId, if necessary.
IF @DBASessionId IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBASession - Insert';
		EXEC @RC = DBADefault.dbo.s_DBASessionINS
					@SessionDateTime = @RunDateTime,
					@DBName = @DBName,
					@IssueNumber = NULL,
					@DBASessionId = @DBASessionId OUT;
		IF @RC = 0
		BEGIN;
			PRINT	'SessionId:  ' + CONVERT(varchar(9), @DBASessionId)
			PRINT	SPACE(0);
			
			-- Store the new DBASessionId.
			BEGIN TRY
				PRINT	'z20140514_01_Logging - Insert';
				INSERT	DBADefault.dbo.z20140514_01_Logging
						(DBASessionId, SessionCreateDateTime)
				VALUES	(@DBASessionId, @RunDateTime);
			END TRY
			BEGIN CATCH
				SET @ErrMsg = N'z20140514_01_Logging - Insert: ' + ERROR_MESSAGE();
			END CATCH			
		END;
		ELSE
		BEGIN
			SET @ErrMsg = N'DBASession - Insert - RC: ' + CONVERT(varchar(10), @RC);
		END;
	END TRY
	BEGIN CATCH
		SELECT	@ErrMsg = N'DBASession or z20140514_01_Logging - Insert: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		GOTO Failed;
END;

------------------------------

SET @SQL_ExtractTemplate = 












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
