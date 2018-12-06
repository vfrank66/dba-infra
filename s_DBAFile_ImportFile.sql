USE DBADefault;
GO

ALTER PROC [dbo].[s_DBAFile_ImportFile]
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

	SET NOCOUNT ON;SET ROWCOUNT 0;SET XACT_ABORT ON;SET ANSI_PADDING ON; SET ANSI_WARNINGS ON;SET IMPLICIT_TRANSACTIONS OFF;
	
	-- Preset return value(s);
	SELECT	@FileId = NULL,
			@ErrMsg = NULL;
			
	DECLARE-- @SPName		nvarchar(128) = N's_DBAFile_ImportFile',
			@Error		int,
			@RC			int,
			@FilePathId	smallint,
			@ErrMsg2	nvarchar(2048);
	
	------------------------------

	-- Validate parameter(s).
	IF @FilePath IS NULL
	OR LEN(@FilePath) = 0
	BEGIN;
		SET @ErrMsg = OBJECT_NAME(@@PROCID) + N': Missing @FilePath parameter value.';
		RETURN(-1);
	END;

	-- Ensure Path does NOT end with a back-slash.
	IF LEN(@FilePath) > 0
	AND RIGHT(RTRIM(@FilePath), 1) = '\'
		SET @FilePath = LEFT(RTRIM(@FilePath), (LEN(RTRIM(@FilePath)) - 1));
	
	IF @FileName IS NULL
	OR LEN(@FileName) = 0
	BEGIN;
		SET @ErrMsg = OBJECT_NAME(@@PROCID) + N': Missing @FileName parameter value.';
		RETURN(-2);
	END;
	
	------------------------------

	-- Retrieve the File Path Id, or create the entry, if necessary.				
	BEGIN TRY
		SELECT	@FilePathId = FilePathId
		FROM	dbo.DBAFilePath
		WHERE	FilePath = @FilePath;

		IF @FilePathId IS NULL
			BEGIN TRY
				INSERT dbo.DBAFilePath
						(FilePath)
				VALUES (@FilePath)
				SELECT	@Error = @@ERROR,
						@FilePathId = SCOPE_IDENTITY();
			END TRY
			BEGIN CATCH
				SELECT	@Error = ERROR_NUMBER(),
						@ErrMsg = N'Create FilePath entry: ' + ERROR_MESSAGE();
			END CATCH
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Retrieve FilePath info: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		RETURN(@Error);
			
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
				SET @RC = 0;
				SET @ErrMsg2 = NULL;

				EXEC @RC = dbo.s_DBADeleteFileEntry							
								@FileId = @FileId,
								@ErrMsg = @ErrMsg2 OUT
				IF @RC <> 0
				OR LEN(@ErrMsg2) > 0
					SET @ErrMsg = N's_DBADeleteFileEntry RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL('; ' + NULLIF(@ErrMsg2, SPACE(0)), SPACE(0));

				SET @FileId = NULL;
			END;
		END TRY
		BEGIN CATCH
			SELECT	@Error = @@ERROR,
					@ErrMsg = N's_DBADeleteFileEntry: ' + ERROR_MESSAGE();
		END CATCH
		IF @Error <> 0
		OR LEN(@ErrMsg) > 0
			RETURN(ISNULL(NULLIF(@Error, 0), -11));
	END;
			
	------------------------------

	-- Create an entry for the File.			
	BEGIN TRY					
		SET @RC = 0;
		SET @FileId = NULL;

		EXEC @RC = dbo.s_DBAFileINS
					@FilePathId = @FilePathId,
					@FileName = @FileName,
					@DBASessionId = @DBASessionId,
					@FileId = @FileId OUT;
		IF @RC <> 0
			SET @ErrMsg =  N's_DBAFileINS RC: ' + CONVERT(nvarchar(10), @RC);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N's_DBAFileINS: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		RETURN(ISNULL(NULLIF(@Error, 0), -12));

	------------------------------

	-- Import the file's contents.
	BEGIN TRY
		SET @ErrMsg2 = NULL;

		EXEC @RC = dbo.s_DBAFile_ImportSimple
				@FileId	= @FileId,
				@DBName = @DBName,
				@ErrMsg = @ErrMsg2 OUT;
		IF @RC <> 0
			SET @ErrMsg =  N's_DBAFile_ImportSimple RC: ' + CONVERT(nvarchar(10), @RC) + ISNULL(N'; ' + NULLIF(@ErrMsg2, SPACE(0)), SPACE(0));
	END TRY
	BEGIN CATCH;
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg =  ERROR_MESSAGE();		
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		RETURN(ISNULL(NULLIF(@Error, 0), -3));

	RETURN(ISNULL(@Error, 0));



