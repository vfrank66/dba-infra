SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET ANSI_NULLS ON;
USE DBADefault;
GO
IF OBJECT_ID('dbo.s_DBAFilePathGET') IS NOT NULL DROP PROCEDURE dbo.s_DBAFilePathGET;
GO
CREATE PROCEDURE dbo.s_DBAFilePathGET
@FilePath	varchar(255),

@FilePathId	smallint OUT
AS
BEGIN;
	SET NOCOUNT ON;SET ROWCOUNT 0;
	
	-- Preset output parameter.
	SET @FilePathId = NULL;
	
	-- Retrieve the existing FilePathId for the specified FilePath value,
	-- or create a new entry, and return its FilePathId value.
	BEGIN TRY
		SELECT	@FilePathId = FilePathId
		FROM	dbo.DBAFilePath
		WHERE	FilePath = 
					CASE
					WHEN RIGHT(LTRIM(RTRIM(@FilePath)), 1) = '\'
					THEN LEFT(LTRIM(@FilePath), (LEN(LTRIM(@FilePath)) - 1))
					ELSE LTRIM(RTRIM(@FilePath))
					END
		
		IF @FilePathId IS NULL
		BEGIN;
			INSERT	dbo.DBAFilePath
					(FilePath)
			SELECT	FilePath = 
						CASE
						WHEN RIGHT(LTRIM(RTRIM(@FilePath)), 1) = '\'
						THEN LEFT(LTRIM(@FilePath), (LEN(LTRIM(@FilePath)) - 1))
						ELSE LTRIM(RTRIM(@FilePath))
						END

			SET @FilePathId = SCOPE_IDENTITY();
		END;
		
		RETURN(0);
	END TRY
	
	BEGIN CATCH
		RETURN(ERROR_NUMBER());
	END CATCH
	
END;

		

