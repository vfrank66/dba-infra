USE DBADefault;
GO
ALTER PROC dbo.s_DBAFile_ExportSimple
@FileId			int,

@ErrMsg			nvarchar(2048) OUT
AS
-- This Stored Procedure exports the 'Contents' columns for the DBAFileRow entries of the specifeid FileId, in FileRowId sequence,
-- to the location specified by the FilePath and FileName entries for the FileId.
-- Any file headings, column headings, or formatting must be contained in the 'Contents' columns.
-- Any error encountered is returned to the caller.

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;

	-- Preset OUTPUT value(s).
	SELECT	@ErrMsg = NULL;		
	
	DECLARE @SPName			nvarchar(128),
			@Error			int,
			@CmdTemplate	nvarchar(max),
			@JobName		nvarchar(128),
			@JobDescription	nvarchar(512),
			@StepName		nvarchar(128),
			@RC				int,
			@Cmd			nvarchar(max),
			@Result			nvarchar(1024),
			@FilePath		varchar(255),
			@FileName		varchar(255),

			@JobName_CreateDir	nvarchar(128),
			@StepName_CreateDir nvarchar(128),
			@Cmd_CreateDir		nvarchar(max);

	SELECT	@SPName = N's_DBAFile_ExportSimple';
	
	-- The is for standard BCP output command. @variable@ entries are replaced by their respective values.
	SET @CmdTemplate = 
	--		N'bcp "SELECT Contents FROM DBADefault.dbo.DBAFileRow WHERE FileId = @FileId@ ' +
	--		N'ORDER BY FileRowId" queryout "@OutputPath@\@OutputFileName@" -T -c -k -S @ServerName@ -U sa -P @Password@ ' +
	--		N'-o "@OutputPath@\@DBName@.@OutputFileName@.ExportLog.txt"';
			N'SQLCMD -E -d DBADefault -w 65535 -W -m-1 -h-1 -b -Q "SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_WARNINGS OFF;SET ANSI_PADDING ON;SET XACT_ABORT OFF;SELECT Contents FROM dbo.DBAFileRow WHERE FileId = @FileId@ ORDER BY FileRowId" ' +
			N' -o "@OutputPath@\@OutputFileName@" '
			
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
		SELECT	@FilePath = dfp.FilePath,
				@FileName = df."FileName"
		FROM	dbo.DBAFile df
				INNER JOIN dbo.DBAFilePath dfp
					ON dfp.FilePathId = df.FilePathId
		WHERE	df.FileId = @FileId;
		SELECT @Error = @@ERROR;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Retrieve info for FileId (' + CONVERT(nvarchar(10), @FileId) + N'): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
		
	------------------------------
	-- Ensure Folder exists. Create sub-folders, as necessary.
/*****
	BEGIN TRY
	--print 'ready to create ''Create Directory'' job';	--debug
		SELECT	@JobName_CreateDir = N'DBADBA - CreateDir - ' + CONVERT(nchar(36), NEWID()),
				@StepName_CreateDir = N'Create Directory',
				@Cmd_CreateDir = N'SQLCMD -E -d tempdb -Q "EXEC master.sys.xp_create_subdir ''' +  @FilePath + ''';" ';

		EXEC @RC = dbo.s_DBASingleStepJob
						@JobName			= @JobName_CreateDir,
						@JobDescription		= N' ',
						@StepName			= @StepName_CreateDir,
						@StepSubSystem		= N'CMDEXEC',		--N'TSQL',
						@DBName				= NULL,
						@DBUser				= N'sa',
						@Command			= @Cmd_CreateDir,
						@Result				= @Result OUT;
		SET @Error = @@ERROR;
	--print 'return from ''Create Directory'' job: RC: ' + CONVERT(varchar(10), @RC) + '  @Error: ' + CONVERT(varchar(10), @Error);	--debug

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
*****/
	BEGIN TRY
		EXEC master.sys.xp_create_subdir @FilePath
	END TRY

	BEGIN CATCH	
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' xp_create_subdir (' + @FilePath + N'): ' + ERROR_MESSAGE();
	END CATCH


	--IF @Error <> 0
	--OR (@@TRANCOUNT > 0
	--	AND XACT_STATE() = -1)		-- Problem during transaction
	--	RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	--BEGIN TRY
	--	EXEC master.sys.xp_create_subdir @FilePath
	--END TRY
	
	--BEGIN CATCH
	--	SELECT	@Error = ERROR_NUMBER(),
	--			@ErrMsg = @SPName + N' Ensure Folder exists: ' + ERROR_MESSAGE();
	--END CATCH
	
	IF @Error <> 0
	OR (@@TRANCOUNT > 0
		AND XACT_STATE() = -1)		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
	
	------------------------------
	-- Build the Command string.
	BEGIN TRY	
	SET @Cmd = @CmdTemplate;
		--SET @Cmd = REPLACE(@Cmd, '@ServerName@', @ServerName);
		--SET @Cmd = REPLACE(@Cmd, '@DBName@', @DBName);
		--SET @Cmd = REPLACE(@Cmd, '@Password@', @Password);
		SET @Cmd = REPLACE(@Cmd, '@FileId@', CONVERT(nvarchar(10), @FileId));
		SET @Cmd = REPLACE(@Cmd, '@OutputPath@', @FilePath);
		SET @Cmd = REPLACE(@Cmd, '@OutputFileName@', @FileName);
		--SET @Cmd = REPLACE(@Cmd, '@FileName@', @FileName);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = @SPName + N' Build Command string: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR (@@TRANCOUNT > 0
		AND XACT_STATE() = -1)		-- Problem during transaction
		RETURN(COALESCE(NULLIF(@Error, 0), NULLIF(XACT_STATE(), 1), -1));
		
--PRINT '@Cmd: ' + @Cmd;	--debug		
	------------------------------	
	BEGIN TRY
		--print 'ready to create ''' + @JobName + ''' job for file export';	--debug
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
	--print 'return from ''' + @JobName + ''' job: RC: ' + CONVERT(varchar(10), @RC) + '  @Error: ' + CONVERT(varchar(10), @Error);	--debug

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
	OR (@@TRANCOUNT > 0
		AND XACT_STATE() = -1)		-- Problem during transaction
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
	OR (@@TRANCOUNT > 0
		AND XACT_STATE() = -1)		-- Problem during transaction
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
	OR (@@TRANCOUNT > 0
		AND XACT_STATE() = -1)		-- Problem during transaction
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

