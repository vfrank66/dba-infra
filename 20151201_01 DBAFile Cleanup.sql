SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
SET XACT_ABORT ON;SET ARITHABORT ON;
GO


DECLARE	@ScriptPrefix		varchar(14) = '20151201_01',
		@ServerName			nvarchar(128) = CONVERT(nvarchar(128), ISNULL(SERVERPROPERTY('ServerName'), SERVERPROPERTY('InstanceName'))),
		@DBName				nvarchar(128) = DB_NAME(),
		@RunDateTime		datetime = GETDATE(),
		@DBASessionId		int;

DECLARE	@Title	varchar(1000) = 'DBA - DBAFile Cleanup';

PRINT	'DBA Database Maintenance';
PRINT	'"' + REPLACE(@Title, '"', '""') + '"';
PRINT	'DB: ' + DB_NAME();
PRINT	'Run: ' + CONVERT(char(23), GETDATE(), 121);
PRINT	SPACE(0);

DECLARE	@RC					int,
		@Error				int,
		@ErrMsg				nvarchar(2048);

-- File to be processed.
DECLARE	@FileId				int;

-- Process each file
WHILE 1 <> 0
BEGIN;
	-- Get next fie to be processed.
	BEGIN TRY
		SET @FileId = 
				ISNULL((SELECT TOP (1)
								FileId
						FROM	DBADefault.dbo.DBAFile_Cleanup
						ORDER BY RowSeq),
						0);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'Loop - Get next FileId: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		BREAK;

	-- Exit if nothing to do.
	IF @FileId = 0
		BREAK;

	-- Delete note(s).
	BEGIN TRY
		DELETE	DBADefault.dbo.DBAFileNote
		WHERE	FileId = @FileId
		PRINT 'DBAFileNote (' + CONVERT(varchar(10), @FileId) + ') deleted: ' + CONVERT(varchar(10), @@ROWCOUNT);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'DBAFileNote (' + CONVERT(nvarchar(10), @FileId) + N') - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		BREAK;

	-- Delete column(s).
	BEGIN TRY
		DELETE	dfc
		FROM	DBADefault.dbo.DBAFileColumn dfc 
				INNER JOIN DBADefault.dbo.DBAFileRow dfr
					ON dfr.FileRowId = dfc.FileRowId
		WHERE dfr.FileId = @FileId;
		PRINT 'DBAFileColumn (' + CONVERT(varchar(10), @FileId) + ') deleted: ' + CONVERT(varchar(10), @@ROWCOUNT);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'DBAFileColumn (' + CONVERT(nvarchar(10), @FileId) + N') - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		BREAK;

	-- Delete row(s).
	BEGIN TRY
		DELETE	DBADefault.dbo.DBAFileRow
		WHERE	FileId = @FileId
		PRINT 'DBAFileRow (' + CONVERT(varchar(10), @FileId) + ') deleted: ' + CONVERT(varchar(10), @@ROWCOUNT);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'DBAFileRow (' + CONVERT(nvarchar(10), @FileId) + N') - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		BREAK;

	-- Delete file.
	BEGIN TRY
		DELETE	DBADefault.dbo.DBAFile
		WHERE	FileId = @FileId
		PRINT 'DBAFile (' + CONVERT(varchar(10), @FileId) + ') deleted: ' + CONVERT(varchar(10), @@ROWCOUNT);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'DBAFile (' + CONVERT(nvarchar(10), @FileId) + N') - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		BREAK;

	-- Remove entry.
	BEGIN TRY
		DELETE	DBADefault.dbo.DBAFile_Cleanup
		WHERE	FileId = @FileId;
		PRINT 'DBAFile_Cleanup (' + CONVERT(varchar(10), @FileId) + ') deleted';
		PRINT	SPACE(0);
	END TRY
	BEGIN CATCH
		SET @ErrMsg = N'DBAFile_Cleanup (' + CONVERT(nvarchar(10), @FileId) + N') - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF LEN(@ErrMsg) > 0
		BREAK;
END;
IF LEN(@ErrMsg) > 0
	GOTO Failed;

------------------------------
GOTO Done;
Failed:
		PRINT	'Error: ' + CONVERT(varchar(10), @Error);
	IF LEN(@ErrMsg) > 0
		PRINT	'"' + REPLACE(@ErrMsg, '"', '""') + '"';

	RAISERROR('Script failed!', 18, 1);
Done:
GO




