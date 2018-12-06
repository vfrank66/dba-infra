USE [DBADefault]
GO

/****** Object:  StoredProcedure [dbo].[s_DBAFile_ImportSimple2]    Script Date: 1/21/2016 9:12:27 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER PROC [dbo].[s_DBAFile_ImportSimple]
@FileId		int,
@DBName		nvarchar(128) = NULL,

@ErrMsg		nvarchar(2048) OUT
AS
	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;

	-- Preset OUTPUT value(s).
	SELECT	@ErrMsg = NULL;		
	
	DECLARE-- @SPName			nvarchar(128),
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
			@DBName2		nvarchar(128),
			@ServerName		nvarchar(128),
			@Password		nvarchar(128);

	-- These variables are used when an input file name is too long.
	-- A copy of the file will be created, using a GUID as the filename.
	-- The shorter-named file will be imported, then deleted.
	DECLARE	@GUID_FileName	varchar(36),
			@Result2		nvarchar(1024),
			@AltFileCreated	bit = 0,
			@RC2			int = 0,
			@Error2			int = 0,
			@ErrMsg2		nvarchar(2048);

	DECLARE	@SQuote			nvarchar(1) = N'''';


	--SET	@SPName  = OBJECT_NAME(@@PROCID);
	
	SET @CmdTemplate = 
			N'EXEC DBADefault.dbo.s_DBAFile_BulkInsert
							@FileId = @FileId@,
							@FilePath = ''@FilePath@'',
							@FileName = ''@FileName@'';';
	
	-- Check passed parameter (@FileId) for validity.		
	IF @FileId IS NULL
	BEGIN;
		SET @ErrMsg = OBJECT_NAME(@@PROCID) + N' @FileId paramter IS NULL';
		RETURN(-1);
	END;

	IF NOT EXISTS(
		SELECT 1
		FROM	DBADefault.dbo.DBAFile
		WHERE	FileId = @FileId)
	BEGIN;
		SET @ErrMsg = OBJECT_NAME(@@PROCID) + N' No File found for FileId (' + CONVERT(nvarchar(10), @FileId) + ')';
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
				@ErrMsg = OBJECT_NAME(@@PROCID) + N' Retrieve info for FileId (' + CONVERT(nvarchar(10), @FileId) + N'): ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(@Error);

	------------------------------		

	BEGIN TRY	
		SET @RC = 0;
		SET @JobName = N'DBADBA - Import - ' + CONVERT(nchar(36), NEWID());
		SET @StepName = N'Import File';
		SET @Cmd = 
				REPLACE
					(REPLACE
						(REPLACE(
							@CmdTemplate,
							'@FilePath@',
							@FilePath),
						'@FileName@',
						REPLACE(@FileName, @SQuote, @SQuote + @SQuote)),
					'@FileId@',
					@FileId);

		EXEC @RC = dbo.s_DBASingleStepJob
						@JobName			= @JobName,
						@JobDescription		= @JobDescription,
						@StepName			= @StepName,
						@StepSubSystem		= N'TSQL',
						@DBName				= @DBName2,
						@DBUser				= N'sa',
						@Command			= @Cmd,
						@Result				= @Result OUT;


	END TRY
	BEGIN CATCH	
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N's_DBASingleStepJob: ' + ERROR_MESSAGE();
	END CATCH

	IF @Result LIKE '%too long%'
	BEGIN;
		PRINT	'Filename too long; attempting alternate import.'		--Logging
		-- Copy the file to a shorter name, import the shorter-named file, then delete the shorter-named file.
		BEGIN TRY
			SET @GUID_FileName = CONVERT(char(36), NEWID());
			SET @JobName = N'DBADBA - Rename - ' + @GUID_FileName;
			SET @Cmd = N'COPY "' + @FilePath + N'\' + @FileName + N'" "' + @FilePath + N'\' + @GUID_FileName + N'"';
	
			EXEC @RC2 = dbo.s_DBASingleStepJob
							@JobName			= @JobName,
							@JobDescription		= N'Copy long filename to short filename',
							@StepName			= N'Copy File',
							@StepSubSystem		= N'CMDEXEC',
							@DBName				= @DBName2,
							@DBUser				= N'sa',
							@Command			= @Cmd,
							@Result				= @Result2 OUT;
			-- Note file creation.
			SET @AltFileCreated = 1;
		END TRY
		BEGIN CATCH
			SELECT	@Error2 = ERROR_NUMBER(),
					@ErrMsg2 = N's_DBASingleStepJob (Rename): ' + ERROR_MESSAGE();
					PRINT @ErrMsg2;		--debug
		END CATCH

		IF @Error2 = 0
		AND @RC2 = 0
		BEGIN;
			-- Import the alternate filename.
			BEGIN TRY
				SET @JobName = N'DBADBA - Import - ' + @GUID_FileName;
				SET @Cmd = 
						REPLACE
							(REPLACE
								(REPLACE(
									@CmdTemplate,
									'@FilePath@',
									@FilePath),
								'@FileName@',
								REPLACE(@GUID_FileName, @SQuote, @SQuote + @SQuote)),
							'@FileId@',
							@FileId);

				EXEC @RC2 = dbo.s_DBASingleStepJob
								@JobName			= @JobName,
								@JobDescription		= @JobDescription,
								@StepName			= N'Import file',
								@StepSubSystem		= N'TSQL',
								@DBName				= @DBName2,
								@DBUser				= N'sa',
								@Command			= @Cmd,
								@Result				= @Result2 OUT;
			END TRY
			BEGIN CATCH
				SELECT	@Error2 = ERROR_NUMBER(),
						@ErrMsg2 = N's_DBASingleStepJob (Import Alternate): ' + ERROR_MESSAGE();
					PRINT @ErrMsg2;		--debug
			END CATCH
		END;

		IF @AltFileCreated = 1
		BEGIN;
			-- Delete the alternate filename.
			BEGIN TRY
				SET @JobName = N'DBADBA - Delete - ' + @GUID_FileName;
				SET @Cmd = N'DEL "' + @FilePath + '\' + @GUID_FileName + '"';

				EXEC @RC2 = dbo.s_DBASingleStepJob
								@JobName			= @JobName,
								@JobDescription		= N'Delete altername filename',
								@StepName			= N'Delete File',
								@StepSubSystem		= N'CMDEXEC',
								@DBName				= @DBName2,
								@DBUser				= N'sa',
								@Command			= @Cmd,
								@Result				= @Result2 OUT;
			END TRY
			BEGIN CATCH
				SELECT	@Error2 = ERROR_NUMBER(),
						@ErrMsg2 = N's_DBASingleStepJob (Rename): ' + ERROR_MESSAGE();
					PRINT @ErrMsg2;		--debug
			END CATCH

			IF @Error2 = 0
			AND @RC2 = 0
			AND LEN(ISNULL(@Result2, SPACE(0))) = 0
				SELECT	@RC = 0,
						@Error = 0,
						@ErrMsg = NULL;
		END;
	END;

	IF @RC <> 0
	BEGIN;
		SELECT	@ErrMsg = N's_DBASingleStepJob RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(N' - ' + @Result, SPACE(0));
		RETURN(-3);
	END;							

	------------------------------
	
	RETURN(ISNULL(@Error, 0));



GO


