SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET ANSI_NULLS ON;
USE DBADefault;
GO
IF OBJECT_ID('dbo.s_DBAFileINS_Path') IS NOT NULL DROP PROCEDURE dbo.s_DBAFileINS_Path;
GO

CREATE PROCEDURE dbo.s_DBAFileINS_Path
@FilePath		varchar(255),
@FileName		varchar(255),
@FileSet		tinyint = 0,
@UserState		tinyint = 0,
@DBASessionId	int = NULL,

@FileId			int OUT

AS

BEGIN;

	SET NOCOUNT ON;SET ROWCOUNT 0;

	-- Preset output parameter.
	SET @FileId = NULL;
	
	DECLARE	@RC				int,
			@FilePathId		int;
	
	EXEC @RC = dbo.s_DBAFilePathGET
					@FilePath,
					@FilePathId OUT;
	IF @RC <> 0
	BEGIN;
		RETURN(@RC);
	END;
	
	BEGIN TRY
		INSERT	dbo.DBAFile
				(FilePathId, "FileName", FileSet, UserState, DBASessionId)
		VALUES	(@FilePathId, LTRIM(RTRIM(@FileName)), @FileSet, @UserState, @DBASessionId)

		SET @FileId = SCOPE_IDENTITY();
		
		RETURN(0);
	END TRY
	
	BEGIN CATCH
		RETURN(ERROR_NUMBER());
	END CATCH

END;
