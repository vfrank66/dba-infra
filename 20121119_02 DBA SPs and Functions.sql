USE DBADefault;
SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET ANSI_WARNINGS ON;
GO

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
IF OBJECT_ID('dbo.f_DBAFile_GetName') IS NOT NULL DROP FUNCTION dbo.f_DBAFile_GetName;
GO
CREATE FUNCTION dbo.f_DBAFile_GetName
(@FileId	int)
RETURNS varchar(255)
AS
BEGIN;
	RETURN((SELECT	"FileName"
			FROM	dbo.DBAFile
			WHERE	FileId = @FileId));
END;
GO			

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

IF OBJECT_ID('dbo.s_DBASingleStepJob') IS NOT NULL DROP PROC dbo.s_DBASingleStepJob;
GO
CREATE PROC dbo.s_DBASingleStepJob
@ServerName			nvarchar(128) = N'(local)',
@JobName			nvarchar(128),
@JobDescription		nvarchar(512),
@StepName			nvarchar(128),
@StepSubSystem		nvarchar(40),
@DBName				nvarchar(128) = NULL,
@DBUser				nvarchar(128)= N'sa',
@Command			nvarchar(max),

@Result				nvarchar(2048) OUT
AS

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;
	
	-- Pre-set the OUT parameter(s).
	SET @Result = SPACE(0);
	
	DECLARE @SPName		nvarchar(128) = N's_DBASingleStepJob',
			@RC			int = 0,
			@Error		int = 0,
			@ErrMsg		nvarchar(2048) = SPACE(0),
			@ServerName2	nvarchar(128),
			@JobId		uniqueidentifier,
			@Result2	nvarchar(2048) = SPACE(0);
			
	-- Info from sysjobhistory for the single step.			
	DECLARE	@InstanceId		int,
			@Message		nvarchar(1024),
			@RunStatus		int;
			
	-- Create the SQL Server Agent Job.
	SET @ServerName = ISNULL(@ServerName, N'(local)');
	
--PRINT	@SPName + N' Calling sp_add_job for Jobname: ' + @JobName;	--debug	
	BEGIN TRY
		EXEC @RC = msdb.dbo.sp_add_job 
						@job_name = @JobName,
						@enabled = 1, 
						@notify_level_eventlog = 2, 
						@notify_level_email = 0, 
						@notify_level_netsend = 0, 
						@notify_level_page = 0, 
						@delete_level = 0, 
						@description = @JobDescription,
						@category_name = N'[Uncategorized (Local)]', 
						@owner_login_name = N'sa', 
						@job_id = @JobId OUTPUT;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' sp_add_job: ' + ERROR_MESSAGE()
	END CATCH
	IF @RC <> 0
	OR @Error <> 0
	OR XACT_STATE() = -1
	OR LEN(@ErrMsg) > 0
	BEGIN;
		SET @Result = ISNULL(NULLIF(@ErrMsg, SPACE(0)), 'Error occurred while attempting to create a SQL Server agent Job: ' + @JobName + N'.');
		RETURN(NULLIF(ISNULL(@RC, 0), @Error));
	END;
	

--PRINT	@SPName + N' Calling sp_add_jobserver for Jobname: ' + @JobName;	--debug	
	-- Add JobServer for the Job.
	BEGIN TRY
		EXEC @RC = msdb.dbo.sp_add_jobserver 
								@job_name = @JobName, 
								@server_name = @ServerName2
		SELECT @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' sp_add_jobserver: ' + ERROR_MESSAGE()
	END CATCH
	IF @RC <> 0
	OR @Error <> 0
	OR XACT_STATE() = -1
	OR LEN(@ErrMsg) > 0
	BEGIN;
		SET @Result = ISNULL(NULLIF(@ErrMsg, SPACE(0)), N'Error occurred while attempting to assign a Job Server for SQL Server Agent Job Job: ' + @JobName + N'.');
		RETURN(COALESCE(NULLIF(@RC, 0), NULLIF(@Error, 0), -1));
	END;
	
--PRINT	@SPName + N' Calling sp_add_jobstep for Jobname: ' + @JobName;	--debug	
	-- Add JobServer for the Job.
	-- Add the Job step.
	BEGIN TRY
		--IF @DBName IS NULL
		--	SET @DBName = N'master';
		--IF @DBUser IS NULL
		--	SET @DBUser = N'sa';	
		EXEC @RC = msdb.dbo.sp_add_jobstep 
						@job_id = @JobId, 
						@step_name = @StepName, 
						@step_id = 1, 
						@cmdexec_success_code = 0, 
						@on_success_action = 1,						-- Quit with success
						@database_name = @DBName,					-- Database, if needed
						--@database_user_name = @DBUser,				-- Database user, if needed
						@on_success_step_id = 0, 
						@on_fail_action = 2,						-- Quit with failure
						@on_fail_step_id = 0, 
						@retry_attempts = 0, 
						@retry_interval = 1, 
						@os_run_priority = 0, 
						@subsystem = @StepSubSystem,				-- The specified sub-system.
						@command = @Command,						-- The specified command line or SQL Query
						@flags = 0;
			SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' sp_add_jobstep: ' + ERROR_MESSAGE()
	END CATCH
	IF @RC <> 0
	OR @Error <> 0
	OR XACT_STATE() = -1
	OR LEN(@ErrMsg) > 0
	BEGIN;
		SET @Result = ISNULL(NULLIF(@ErrMsg, SPACE(0)), N'Error occurred while attempting to create a Job Step for SQL Server Agent Job Job: ' + @JobName + N'.');
		RETURN(COALESCE(NULLIF(@RC, 0), NULLIF(@Error, 0), -1));
	END;
	
--PRINT	@SPName + N' Calling sp_start_job for Jobname: ' + @JobName;	--debug	
	-- Start the Job.
	BEGIN TRY
--PRINT	N'Starting Job: ' + @JobName	; --debug
		EXEC @RC = msdb.dbo.sp_start_job 
								@job_name = @JobName					
			SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' sp_start_job for JobName (' + @JobName + N'): ' + ERROR_MESSAGE()
	END CATCH
	IF @RC <> 0
	OR @Error <> 0
	OR XACT_STATE() = -1
	OR LEN(@ErrMsg) > 0
	BEGIN;
		SET @Result = ISNULL(NULLIF(@ErrMsg, SPACE(0)), 'An error occured while attemping to start a SQL Server Agent Job: ' + @JobName + N'.');
		RETURN(NULLIF(ISNULL(@RC, 0), @Error));
	END;

--PRINT	@SPName + N' waiting for Job to end.';	--debug
	-- Wait for the Job to end.
	BEGIN TRY
		WHILE(1 <> 0)
		BEGIN;
			-- Get the status entry for the Job Step.
			SELECT	@InstanceId = instance_id,
					@Message = "message",
					@RunStatus = run_status
			FROM	msdb.dbo.sysjobhistory
			WHERE	job_id = @JobId
			AND		step_id = 1;

			-- Exit loop as soon as step is complete.
			IF @InstanceId IS NOT NULL
			AND @RunStatus <> 4		-- In progress
				BREAK;
					
			-- Wait before re-attempting to access status entry.					
			WAITFOR DELAY '00:00:01';	-- 1 second
		END;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Wait for completion: ' + ERROR_MESSAGE()
	END CATCH
	IF @RC <> 0
	OR @Error <> 0
	OR XACT_STATE() = -1
	OR LEN(@ErrMsg) > 0
	BEGIN;
		SET @Result = ISNULL(NULLIF(@ErrMsg, SPACE(0)), N'Error occurred while running a SQL Server Agent Job: ' + @JobName + N'.');
		RETURN(COALESCE(NULLIF(@RC, 0), NULLIF(@Error, 0), -1));
	END;
	
	-- For non-successful completion, return the error, or a message.
	IF @RunStatus IN
			(0,			-- Failed
			4)			-- Cancelled
	BEGIN;
		SET @Result = 
				N'Job: ' + @JobName + N' - ' +
				ISNULL(NULLIF(@Message, SPACE(0)),
						CASE 
						WHEN @RunStatus = 0 
						THEN N' Job failed'
						ELSE N' Job was cancelled'
						END);
		RETURN(-1);
	END;
	
	-- Clean up the job.
--PRINT	@SPName + N' Calling sp_delete_job for Jobname: ' + @JobName;	--debug	
	BEGIN TRY
		EXEC @RC = msdb.dbo.sp_delete_job
								@job_id = @JobId;
		SELECT @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' sp_delete_job: ' + ERROR_MESSAGE()
	END CATCH
	IF @RC <> 0
	OR @Error <> 0
	OR XACT_STATE() = -1
	OR LEN(@ErrMsg) > 0
	BEGIN;
		SET @Result = ISNULL(NULLIF(@ErrMsg, SPACE(0)), N'Error occurred while deleting a SQL Server Agent Job: ' + @JobName + N'.');
		RETURN(COALESCE(NULLIF(@RC, 0), NULLIF(@Error, 0), -1));
	END;
		
	RETURN(0);
GO							

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

IF OBJECT_ID('dbo.s_DBAFile_ImportSimple') IS NOT NULL DROP PROC dbo.s_DBAFile_ImportSimple;
GO
CREATE PROC dbo.s_DBAFile_ImportSimple
@FileId		int,
@DBName		nvarchar(128) = NULL,

@ErrMsg		nvarchar(2048) OUT
AS
	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;

	-- Preset OUTPUT value(s).
	SELECT	@ErrMsg = NULL;		
	
	DECLARE @SPName			nvarchar(128) = N's_DBAFile_ImportSimple',
			@Error			int,
			@CmdTemplate	nvarchar(max),
			@JobName		nvarchar(128),
			@JobDescription	nvarchar(512),
			@StepName		nvarchar(128),
			@RC				int,
			@Cmd			nvarchar(max),
			@Result			nvarchar(1024),
			@DBASessionId	int,
			@FilePath		varchar(255),
			@FileName		varchar(255),
			@DBName2			nvarchar(128),
			@ServerName		nvarchar(128),
			@Password		nvarchar(128);
	
	SET @CmdTemplate = 
			N'SET QUOTED_IDENTIFIER ON;SET IMPLICIT_TRANSACTIONS OFF; ' +
			N'USE DBADefault; ' +
			N'CREATE TABLE #Import ' +
			N'(RowSeq		int		NOT NULL IDENTITY(1, 1), ' +
			N'Contents		varchar(8000) NOT NULL ' +
			N'PRIMARY KEY CLUSTERED (RowSeq) WITH FILLFACTOR = 100); ' +
			N'BULK INSERT #Import FROM "@FilePath@\@FileName@" ' +
			N'WITH (FORMATFILE= ''\\localhost\Import2000\Ad-hoc\Fmt\Generic.RowSeq.Contents.fmt.txt''); ' +
			N'INSERT dbo.DBAFileRow (FileId, Contents) ' + 
			N'SELECT FileId = @FileId@, Contents ' +
			N'FROM #Import ' + 
			N'ORDER BY RowSeq;'

	SET @JobName = N'DBADBA - Import - ' + CONVERT(nchar(36), NEWID());
	SET @StepName = N'Import File';
	
	-- Check passed parameter (@FileId) for validity.		
	IF @FileId IS NULL
	BEGIN;
		SET @ErrMsg = @SPName + N' @FileId paramter IS NULL';
		RETURN(-1);
	END;
	IF NOT EXISTS(
		SELECT 1
		FROM	DBADefault.dbo.DBAFile
		WHERE	FileId = @FileId)
	BEGIN;
		SET @ErrMsg = @SPName + N' No File found for FileId (' + CONVERT(nvarchar(10), @FileId) + ')';
		RETURN(-2);
	END;

	-- Retrieve info from the DBAFile entry.
	BEGIN TRY
		SELECT	@DBASessionId = ds.DBASessionId,
				@FilePath = dfp.FilePath,
				@FileName = df."FileName",
				@DBName2 = ISNULL(@DBName, ds.DBName)
		FROM	dbo.DBAFile df
				INNER JOIN dbo.DBAFilePath dfp
					ON dfp.FilePathId = df.FilePathId
				LEFT JOIN dbo.DBASession ds
					ON ds.DBASessionId = df.DBASessionId					
		WHERE	df.FileId = @FileId;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Retrieve info for FileId (' + CONVERT(nvarchar(10), @FileId) + N'): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -255));

	------------------------------
	-- Build the import command.
	SET @Cmd = @CmdTemplate;
	SET @Cmd = REPLACE(@Cmd, '@FilePath@', @FilePath);
	SET @Cmd = REPLACE(@Cmd, '@FileName@', @FileName);
	SET @Cmd = REPLACE(@Cmd, '@FileId@', @FileId);
--PRINT @Cmd; --debug	
	------------------------------		
--PRINT	@SPNAme + N' Calling s_DBASingleStepJob for FileId(' + CONVERT(nvarchar(10), @FileId) + N')';	--debug
	BEGIN TRY	
		EXEC @RC = dbo.s_DBASingleStepJob
						@JobName			= @JobName,
						@JobDescription		= @JobDescription,
						@StepName			= @StepName,
						@StepSubSystem		= N'TSQL',
						@DBName				= @DBName2,
						@DBUser				= N'sa',
						@Command			= @Cmd,
						@Result				= @Result OUT;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH	
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' s_DBASingleStepJob: ' + ERROR_MESSAGE();
	END CATCH

	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' s_DBASingleStepJob RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(N' - ' + @Result, SPACE(0));
		RETURN(-3)
	END;							

	IF @Error <> 0
	OR XACT_STATE() = -1
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -256));

/*****	
	-- Create the import job.
	BEGIN TRY
		EXEC @RC = dbo.s_DBACreateSingleStepJob		
							@JobDescription	= @JobDescription,
							@StepName  = @StepName,
							@Subsystem = N'TSQL',
							@Command = @Cmd,
							@JobName = @JobName OUT;
		SET @Error = @@ERROR;							
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Create Job: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));

	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' s_DBACreateSingleStepJob RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-8);						
	END;							
	
	------------------------------
	-- Run the import job.
	BEGIN TRY
		EXEC @RC = dbo.s_DBARunJob
					@JobName = @JobName,
					@TimeoutDelay = 0;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = ERROR_MESSAGE();
	END CATCH;
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = @SPName + N' sub-Job (' + @JobName + N') RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-9);
	END;
	
	------------------------------
	-- Clean up the sub-Job.
	BEGIN TRY		
		EXEC @RC = msdb.dbo.sp_delete_job
						@job_name = @JobName;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' sp_delete_job (' + @JobName + N') RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-10);
	END;
*****/	
	------------------------------
	
	RETURN(ISNULL(@Error, 0));
GO

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

IF OBJECT_ID('dbo.s_DBAFile_ImportFile') IS NOT NULL DROP PROC dbo.s_DBAFile_ImportFile;
GO
CREATE PROC dbo.s_DBAFile_ImportFile
@DBASessionId	int = NULL,
@FilePath		varchar(255),		-- Do not include a trailing "\" character.
@FileName		varchar(255),
@DBName			nvarchar(128),
@FileId			int OUT,
@ErrMsg			nvarchar(2048) OUT
AS
-- This Stored Procedure creates a DBAFilePath entry, if necessary; a DBAFile entry; and 
-- zero or more DBAFileRow entries.
-- The import file is expected to be text.
-- The standard RowSeq (IDENTITY), Contents file format is used.
-- Any error encountered is returned to the caller.

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;
	
	-- Preset return value(s);
	SELECT	@FileId = NULL,
			@ErrMsg = NULL;
			
	DECLARE @SPName		nvarchar(128) = N's_DBAFile_ImportFile',
			@Error		int,
			@RC			int,
			@FilePathId	smallint,
			@ErrMsg2	nvarchar(2048);
	
	------------------------------
	-- Validate parameter(s).
	IF @FilePath IS NULL
	OR LEN(@FilePath) = 0
	BEGIN;
		SET @ErrMsg = @SPName + N' Missing @FilePath parameter value.';
		RETURN(-1);
	END;
	
	IF @FileName IS NULL
	OR LEN(@FileName) = 0
	BEGIN;
		SET @ErrMsg = @SPName + N' Missing @FileName parameter value.';
		RETURN(-2);
	END;
	
	------------------------------
	-- Retrieve the File Path Id, or create the entry, if necessary.				
	BEGIN TRY
		SELECT	@FilePathId = FilePathId
		FROM	dbo.DBAFilePath
		WHERE	FilePath = @FilePath;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Retrieve FilePath info: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(ISNULL(NULLIF(@Error, 0), XACT_STATE()));

	IF @FilePathId IS NULL
	BEGIN;
		BEGIN TRY
			INSERT dbo.DBAFilePath
					(FilePath)
			VALUES (@FilePath)
			SELECT	@Error = @@ERROR,
					@FilePathId = SCOPE_IDENTITY();
		END TRY
		BEGIN CATCH
			SELECT	@Error = ERROR_NUMBER(),
					@ErrMsg = @SPName + N' Create FilePath entry: ' + ERROR_MESSAGE();
		END CATCH
		IF @Error <> 0
		OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	END;
			
	-- If there is no DBASessionId supplied, determine if the file already exists.
	-- If found, delete it, since the DBASessionId is the 'uniqifier' for the DBAFile entry.
	IF @DBASessionId IS NULL
	BEGIN;
		BEGIN TRY
			SET @FileId = 
					ISNULL((SELECT	FileId
							FROM	DBAFile
							WHERE	FilePathId = @FilePathId
							AND		"FileName" = @FileName),
							NULL);
			IF @FileId IS NOT NULL
			BEGIN;
		
				EXEC @RC = dbo.s_DBADeleteFileEntry							
								@FileId = @FileId,
								@ErrMsg = @ErrMsg OUT
				SELECT @Error = @@ERROR;
				IF @Error <> 0
				OR @RC <> 0
					SET @ErrMsg = @SPName + 'N Unable to delete previous version of file'
				SET @FileId = NULL;
			END;
		END TRY
		BEGIN CATCH
			SELECT	@Error = @@ERROR,
					@ErrMsg = @SPName + N' Delete Prior File Version: ' + ERROR_MESSAGE();
		END CATCH
		IF @Error <> 0
		OR XACT_STATE() = -1		-- Problem during transaction
			RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	END;
			
	------------------------------					
	-- Create an entry for the File.			
	BEGIN TRY						
		INSERT dbo.DBAFile
				(FilePathId,
				"FileName",
				DBASessionId)
		VALUES(@FilePathId, @FileName, @DBASessionId);
		SELECT	@Error = @@ERROR,
				@FileId = SCOPE_IDENTITY();
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Create DBAFile entry: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));

	------------------------------
	-- Import the file's contents.
	BEGIN TRY
		EXEC @RC = dbo.s_DBAFile_ImportSimple
				@FileId	= @FileId,
				@DBName = @DBName,
				@ErrMsg = @ErrMsg2 OUT;
		SELECT @Error = @@ERROR;
	END TRY
	BEGIN CATCH;
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' s_DBAFile_ImportSimple FileId(' + CONVERT(nvarchar(10), @FileId) + N'): ' + ISNULL(NULLIF(@ErrMsg2, SPACE(0)), ERROR_MESSAGE());		
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = @SPName + N' s_DBAFile_ImportSimple FileId(' + CONVERT(nvarchar(10), @FileId) + N') RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-3);
	END;

	RETURN(ISNULL(@Error, 0));
GO

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


IF OBJECT_ID('dbo.s_DBAFile_ExportSimple') IS NOT NULL DROP PROC dbo.s_DBAFile_ExportSimple;
GO
CREATE PROC dbo.s_DBAFile_ExportSimple
@FileId			int,

@ErrMsg			nvarchar(2048) OUT
AS
-- This Stored Procedure exports the 'Contents' columns for the DBAFileRow entries of the spevieid FileId, in FileRowId sequence,
-- to the location specified by the FilePath and FileName entries for the FileId.
-- Any file headings, column headings, or formatting must be contained in the 'Contents' columns.
-- Any error encountered is returned to the caller.

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;

	-- Preset OUTPUT value(s).
	SELECT	@ErrMsg = NULL;		
	
	DECLARE @SPName			nvarchar(128) = N's_DBAFile_ExportSimple',
			@Error			int,
			@CmdTemplate	nvarchar(max),
			@JobName		nvarchar(128),
			@JobDescription	nvarchar(512),
			@StepName		nvarchar(128),
			@RC				int,
			@Cmd			nvarchar(max),
			@Result			nvarchar(1024),
			@DBASessionId	int,
			@DBName			nvarchar(128),
			@FilePath		varchar(255),
			@FileName		varchar(255),
			@ServerName		nvarchar(128),
			@Password		nvarchar(128);
	
	-- The is for standard BCP output command. @variable@ entries are replaced by their respective values.
	SET @CmdTemplate = 
			N'bcp "SELECT Contents FROM DBADefault.dbo.DBAFileRow WHERE FileId = @FileId@ ' +
			N'ORDER BY FileRowId" queryout "@OutputPath@\@OutputFileName@" -T -c -k -S @ServerName@ -U sa -P @Password@ ' +
			N'-o "@OutputPath@\@DBName@.@OutputFileName@.ExportLog.txt"';
			
	SELECT	@JobName = N'DBADBA - Export - ' + CONVERT(nchar(36), NEWID()),
			@JobDescription = N'DBA DBA Export',
			@StepName = N'Export FileId ' + CONVERT(nvarchar(10), @FileId);
		
	------------------------------
	
	-- Check passed parameter (@FileId) for validity.		
	IF @FileId IS NULL
	BEGIN;
		SET @ErrMsg = @SPName + N' @FileId paramter IS NULL';
		RETURN(-1);
	END;
	IF NOT EXISTS(
		SELECT 1
		FROM	DBADefault.dbo.DBAFile
		WHERE	FileId = @FileId)
	BEGIN;
		SET @ErrMsg = @SPName + N' No File found for FileId: ' + CONVERT(nvarchar(10), @FileId);
		RETURN(-2);
	END;

	------------------------------			
	
	-- Retrieve the DBAName, FilePath,  and FileName.
	BEGIN TRY
		SELECT	@DBASessionId = ds.DBASessionId,
				@DBName = ISNULL(ds.DBName, 'UnspecifiedDB'),
				@FilePath = dfp.FilePath,
				@FileName = df."FileName"
		FROM	dbo.DBAFile df
				INNER JOIN dbo.DBAFilePath dfp
					ON dfp.FilePathId = df.FilePathId
				INNER JOIN dbo.DBASession ds
					ON ds.DBASessionId = df.DBASessionId					
		WHERE	df.FileId = @FileId;
		SELECT @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Retrieve info for FileId (' + CONVERT(nvarchar(10), @FileId) + N'): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));

	-- Check @DBAName variable for valid value.
	IF @DBName IS NULL
	BEGIN;
		SET @ErrMsg = @SPName + N' Retrieve DBName for DBASession: ' + ISNULL(CONVERT(nvarchar(10), @DBASessionId), 'Unknown');
		RETURN (-3);
	END;

	-- Determine the ServerName.				
	BEGIN TRY			
		SET @ServerName	 = CONVERT(nvarchar(128), SERVERPROPERTY('ServerName'));
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Determine Server name: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	-- Check @ServerName variable for valid value.
	IF @ServerName IS NULL
	BEGIN;
		SET @ErrMsg = @SPName + N' Unable to determine ServerName';
		RETURN (-4);
	END;
	
	-- Determine the Password for the ServerName.
	BEGIN TRY
		SELECT	@Password = CONVERT(nvarchar(128), "Password")
		FROM	"localhost".DBADefault.dbo.TmpMicrofilmInv_Password
		WHERE	ServerName = @ServerName;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Determine Server password: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
		
	-- Check @Password variable for valid value.
	IF @Password IS NULL
	BEGIN;
		SET @ErrMsg = @SPName + N' Unable to determine Password for ServerName: ' + @ServerName;
		RETURN (-5);
	END;
	
	------------------------------
	-- Build the Command string.
	BEGIN TRY	
		SET @Cmd = @CmdTemplate;
		SET @Cmd = REPLACE(@Cmd, '@ServerName@', @ServerName);
		SET @Cmd = REPLACE(@Cmd, '@DBName@', @DBName);
		SET @Cmd = REPLACE(@Cmd, '@Password@', @Password);
		SET @Cmd = REPLACE(@Cmd, '@FileId@', CONVERT(nvarchar(10), @FileId));
		SET @Cmd = REPLACE(@Cmd, '@OutputPath@', @FilePath);
		SET @Cmd = REPLACE(@Cmd, '@OutputFileName@', @FileName);
		SET @Cmd = REPLACE(@Cmd, '@FileName@', @FileName);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Build Command string: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
		
--PRINT '@Cmd: ' + @Cmd;	--debug		
	------------------------------	
	BEGIN TRY
		EXEC @RC = dbo.s_DBASingleStepJob
						@JobName			= @JobName,
						@JobDescription		= @JobDescription,
						@StepName			= @StepName,
						@StepSubSystem		= N'CmdExec',
						@DBName				= NULL,
						@DBUser				= N'sa',
						@Command			= @Cmd,
						@Result				= @Result OUT;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH	
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' s_DBASingleStepJob: ' + ERROR_MESSAGE();
--PRINT '@Cmd: ' + @Cmd;	--debug/Logging
	END CATCH

	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' s_DBASingleStepJob RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(N' - ' + @Result, SPACE(0));
		RETURN(-3)
	END;							

	IF @Error <> 0
	OR XACT_STATE() = -1
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
/*****		
	-- Create the export job.
	BEGIN TRY
		EXEC @RC = dbo.s_DBACreateSingleStepJob		
							@JobDescription	= @JobDescription,
							@StepName  = @StepName,
							@Subsystem = N'CmdExec',
							@Command = @Cmd,
							@JobName = @JobName OUT;
		SET @Error = @@ERROR;							
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Create Job: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));

	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' s_DBACreateSingleStepJob RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-6);						
	END;							
	
PRINT	'Job Created: ' + @JobName;	--debug	
	------------------------------
	-- Run the export job.
	BEGIN TRY
		EXEC @RC = dbo.s_DBARunJob
					@JobName = @JobName,
					@TimeoutDelay = 0;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = ERROR_MESSAGE();
	END CATCH;
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = @SPName + N' sub-Job (' + @JobName + N') RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-7);
	END;
	
PRINT	'Job completed: ' + @JobName; --debug	
	------------------------------
	-- Clean up the sub-Job.
	BEGIN TRY		
		EXEC @RC = msdb.dbo.sp_delete_job
						@job_name = @JobName;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' sp_delete_job (' + @JobName + N') RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-8);
	END;
	------------------------------

PRINT	'Job deleted: ' + @JobName;		--debug	
*****/

	RETURN(ISNULL(@Error, 0));
GO

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

IF OBJECT_ID('dbo.s_DBAMoveFile') IS NOT NULL DROP PROC dbo.s_DBAMoveFile;
GO
CREATE PROCEDURE dbo.s_DBAMoveFile
@Source	varchar(1000),
@Dest	varchar(1000),

@ErrMsg	nvarchar(2048) OUT
AS
-- This Stored Procedure moves a file to the specified destination.

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;
		
	-- Preset OUTPUT value(s).
	SELECT	@ErrMsg = NULL;		

	DECLARE @SPName			nvarchar(128),
			@Error			int,
			@JobName		nvarchar(128),
			@JobDescription	nvarchar(512),
			@StepName		nvarchar(128),
			@RC				int,
			@Cmd			nvarchar(max),
			@Result			nvarchar(1024);
			
	SET		@SPName = N's_DBAMoveFile';
	
	SELECT	@JobName = N'DBADBA - Move - ' + CONVERT(nchar(36), NEWID()),
			@JobDescription = N'DBA DBA File Move',
			@StepName = N'Move File';
				
	IF @Source IS NULL
	OR LEN(@Source) = 0
	BEGIN;
		SET @ErrMsg = @SPName + N' missing @Source parameter';
		RETURN(-1);
	END;
	
	IF @Dest IS NULL
	OR LEN(@Dest) = 0
	BEGIN;
		SET @ErrMsg = @SPName + N' Missing @Dest parameter';
		RETURN(-2)
	END;

	-- Build the command string.			
	SET @Cmd = N'MOVE "' + CONVERT(nvarchar(1000), @Source) + N'" "' + CONVERT(nvarchar(1000), @Dest) + N'"';
			
	------------------------------
	BEGIN TRY
		EXEC @RC = dbo.s_DBASingleStepJob
						@JobName			= @JobName,
						@JobDescription		= @JobDescription,
						@StepName			= @StepName,
						@StepSubSystem		= N'CmdExec',
						@DBName				= NULL,
						@DBUser				= N'sa',
						@Command			= @Cmd,
						@Result				= @Result OUT;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH	
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' s_DBASingleStepJob: ' + ERROR_MESSAGE();
	END CATCH

	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' s_DBASingleStepJob RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(N' - ' + @Result, SPACE(0));
		RETURN(-3)
	END;							

	IF @Error <> 0
	OR XACT_STATE() = -1
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
							
/*****							
	-- Create the job.
	BEGIN TRY
		EXEC @RC = dbo.s_DBACreateSingleStepJob		
							@JobDescription	= @JobDescription,
							@StepName  = @StepName,
							@Subsystem = N'CmdExec',
							@Command = @Cmd,
							@JobName = @JobName OUT;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' s_DBACreateSingleStepJob: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
							
	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = @SPName + N' s_DBACreateSingleStepJob RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN(-3)
	END;							

	------------------------------	
	-- Run the job.
	BEGIN TRY
		EXEC @RC = dbo.s_DBARunJob
					@JobName = @JobName,
					@TimeoutDelay = 0;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Run sub-Job (' + @JobName + '): ' + ERROR_MESSAGE();
		SET @Error = @@ERROR;
	END CATCH;
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));

	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = @SPName + N' s_DBARunJob (' + @JobName + N') RC: ' + CONVERT(nvarchar(10), @RC);
		RETURN (-4);
	END;
	
	------------------------------
	
	-- Clean up the sub-Job.
	BEGIN TRY
		EXEC @RC = msdb.dbo.sp_delete_job
						@job_name = @JobName;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' sp_delete_job (' + @JobName + 'N"): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR XACT_STATE() = -1		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	IF @RC <> 0
	BEGIN;
		SET @ErrMsg = @SPName + N' sp_delete_job (' + @JobName + ') RC: ' + CONVERT(varchar(10), @RC);
		RETURN (-5);
	END;
*****/	
	------------------------------

	RETURN(ISNULL(@Error, 0));
GO

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

IF OBJECT_ID('dbo.s_DBADeleteFileEntry') IS NOT NULL DROP PROC dbo.s_DBADeleteFileEntry;
GO
CREATE PROCEDURE dbo.s_DBADeleteFileEntry
@FileId	int,

@ErrMsg	nvarchar(2048) OUT
AS
	-- This Stored Procedure removes a DBAFile entry, and all subsidiary data.

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;
		
	-- Preset OUTPUT value(s).
	SELECT	@ErrMsg = NULL;		

	DECLARE @SPName			nvarchar(128),
			@Error			int;
		
	SET		@SPName = N's_DBADeleteFileEntry';

	-- A Note may be directly linked to a file, or indirectly linked, via a DBAFileRow entry.
	BEGIN TRY
		DELETE	dbo.DBAFileNote
		WHERE	FileId = @FileId;
		
		DELETE	dbo.DBAFileNote
		FROM	dbo.DBAFileNote dfn
				INNER JOIN dbo.DBAFileRow dfr
					ON dfr.FileRowId = dfn.FileRowId
		WHERE	dfr.FileId = @FileId;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N': ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(@Error);
	
	-- Remove parsed columns for the file.	
	BEGIN TRY
		DELETE	dbo.DBAFileColumn
		FROM	dbo.DBAFileColumn dfc
				INNER JOIN dbo.DBAFileRow dfr
					ON dfr.FileRowId = dfc.FileRowId
		WHERE	dfr.FileId = @FileId;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N': ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(@Error);
		
	-- Remove rows for the file.
	BEGIN TRY
		DELETE	dbo.DBAFileRow
		WHERE	FileId = @FileId;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N': ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(@Error);

	-- Remove the file entry.
	BEGIN TRY
		DELETE	dbo.DBAFile
		WHERE	FileId = @FileId;
		SET @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N': ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(@Error);

	RETURN(0);
GO
		
	