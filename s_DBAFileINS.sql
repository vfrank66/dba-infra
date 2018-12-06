SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET ANSI_NULLS ON;
USE DBADefault;
GO
IF OBJECT_ID('dbo.s_DBAFileINS') IS NOT NULL DROP PROCEDURE dbo.s_DBAFileINS;
GO

CREATE PROCEDURE dbo.s_DBAFileINS
@FilePathId		smallint,
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
	
	BEGIN TRY
		INSERT	dbo.DBAFile
				(FilePathId, "FileName", FileSet, UserState, DBASessionId)
		VALUES	(@FilePathid, LTRIM(RTRIM(@FileName)), @FileSet, @UserState, @DBASessionId)

		SET @FileId = SCOPE_IDENTITY();
		
		RETURN(0);
	END TRY
	
	BEGIN CATCH
		RETURN(ERROR_NUMBER());
	END CATCH

END;
