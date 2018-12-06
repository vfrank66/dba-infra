-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
SET XACT_ABORT ON;SET ARITHABORT ON;
USE DBADefault;
GO
IF OBJECT_ID('tempdb..#DBASession') IS NOT NULL DROP TABLE #DBASession;
IF OBJECT_ID('tempdb..#Table') IS NOT NULL DROP TABLE #Table;
IF OBJECT_ID('tempdb..#Action') IS NOT NULL DROP TABLE #Action;
GO
DECLARE	@DBASessionId	int = 99999;

PRINT	'DBA Database Maintenance'
PRINT	'Remove Files For Specified DBASessions';
PRINT	'DB: ' + CONVERT(varchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('MachineName'))) + '.' + DB_NAME();
PRINT	'Run: ' + CONVERT(char(23), GETDATE(), 121);
PRINT	SPACE(0);


DECLARE	@Error			int,
		@ErrMsg			nvarchar(2048),
		@RowCount		int,
		@ExpectedCount	int;

DECLARE	@TableId_DBAFile		tinyint = 1,
		@TableId_DBAFileRow		tinyint = 2,
		@TableId_DBAFileColumn	tinyint = 3,
		@Tableid_DBAFileNote	tinyint = 4;

CREATE TABLE #DBASession
		(DBASessionId	int				NOT NULL
		PRIMARY KEY CLUSTERED
			(DBASessionId)
			WITH FILLFACTOR = 100);

CREATE TABLE #Table
		(TableId		tinyint			NOT NULL,
		TableName		varchar(128)	NOT NULL
		PRIMARY KEY CLUSTERED
			(TableId)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(TableName)
			WITH FILLFACTOR = 100);

CREATE TABLE #Action
		(TableId		tinyint			NOT NULL,
		DBASessionId	int				NOT NULL,
		RowId1			int				NOT NULL,
		RowId2			smallint		NULL
		UNIQUE NONCLUSTERED
			(TableId, RowId1, RowId2)
			WITH FILLFACTOR = 10);

----------

BEGIN TRY
	INSERT	#DBASession	(DBASessionId)
	VALUES	(1887);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#DBASession - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

BEGIN TRY
	INSERT	#Table (TableId, TableName)
	VALUES	(@TableId_DBAFile, 'DBAFile'), (@TableId_DBAFileRow, 'DBAFileRow'), (@TableId_DBAFileColumn, 'DBAFileColumn'), (@TableId_DBAFileNote, 'DBAFileNote');
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#Table - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

BEGIN TRY
	INSERT #Action
			(TableId,
			DBASessionId,
			RowId1,
			RowId2)
	SELECT TableId = @TableId_DBAFile,
			ds.DBASessionId,
			RowId1 = Fileid,
			RowId2 = CONVERT(int, NULL)
	FROM	dbo.DBAFile df
			INNER JOIN #DBASession ds
				ON ds.DBASessionId = df.DBASessionId
	UNION ALL
	SELECT	TableId = @TableId_DBAFileRow,
			ds.DBASessionId,
			RowId1 = dfr.FileRowId,
			RowId2 = CONVERT(int, NULL)
	FROM	dbo.DBAFile df
			INNER JOIN #DBASession ds
				ON ds.DBASessionId = df.DBASessionId
			INNER JOIN dbo.DBAFileRow dfr
				ON dfr.FileId = df.FileId
	UNION ALL
	SELECT	TableId = @TableId_DBAFileColumn,
			ds.DBASessionId,
			RowId1 = dfc.FileRowId,
			RowId2 = dfc.ColSeq
	FROM	dbo.DBAFile df
			INNER JOIN #DBASession ds
				ON ds.DBASessionId = df.DBASessionId
			INNER JOIN dbo.DBAFileRow dfr
				ON dfr.FileId = df.FileId
				INNER JOIN dbo.DBAFileColumn dfc
					ON dfc.FileRowId = dfr.FileRowId
	UNION ALL
	SELECT	TableId = @TableId_DBAFileNote,
			ds.DBASessionId,
			RowId1 = dfn.RowSeq,
			RowId2 = NULL
	FROM	dbo.DBAFile df
			INNER JOIN #DBASession ds
				ON ds.DBASessionId = df.DBASessionId
			INNER JOIN dbo.DBAFileNote dfn
				ON dfn.FileId = df.FileId
	WHERE	dfn.FileRowId IS NULL
	UNION ALL
	SELECT	TableId = @TableId_DBAFileNote,
			ds.DBASessionId,
			RowId1 = dfn.RowSeq,
			RowId2 = NULL
	FROM	dbo.DBAFile df
			INNER JOIN #DBASession ds
				ON ds.DBASessionId = df.DBASessionId
			INNER JOIN dbo.DBAFileRow dfr
				ON dfr.FileId = df.FileId
				INNER JOIN dbo.DBAFileNote dfn
					ON dfn.FileRowId = dfr.FileRowId;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'#Action - Insert: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

IF NOT EXISTS(SELECT 1 FROM #Action)
BEGIN;
	PRINT	'Nothing to do!'
	GOTO Done;
END;

BEGIN TRY
	PRINT	'Summary';
	SELECT	"Rows" = ISNULL(CONVERT(varchar(10), A1.Occurs), SPACE(0)),
			"Table" = 
				CASE
				WHEN ISNULL(A1.TableDisplaySeq, 1) = 1
				THEN '"' + REPLACE(t.TableName, '"', '""') + '"'
				ELSE SPACE(0)
				END,
			DBASessionId = ISNULL(CONVERT(varchar(10), DBASessionId), SPACE(0))
	FROM	#Table t
			LEFT JOIN
				(SELECT	Occurs = COUNT(*),
						TableId,
						DBASessionId,
						TableDisplaySeq = ROW_NUMBER() OVER(PARTITION BY TableId ORDER BY DBASessionId)
				FROM	#Action
				GROUP BY TableId, DBASessionId) A1
				ON A1.TableId = t.TableId
	ORDER BY t.TableId, A1.TableDisplaySeq;
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = N'Write Summary: ' + ERROR_MESSAGE();
END CATCH
IF @Error <> 0
	GOTO Failed;

SET @ExpectedCount = 
		ISNULL((SELECT	COUNT(*)
				FROM	#Action
				WHERE	TableId = @TableId_DBAFileNote),
				0);
IF @ExpectedCount > 0
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileNote - Delete';
		DELETE	dbo.DBAFileNote
		FROM	dbo.DBAFileNote dfn
				INNER JOIN #Action a
					ON a.TableId = @TableId_DBAFileNote
					AND a.RowId1 = dfn.RowSeq;
		SET @RowCount = @@ROWCOUNT;
		PRINT	CONVERT(varchar(10), @RowCount) + ' of ' + CONVERT(varchar(10), @ExpectedCount) + ' rows deleted.';
		PRINT	SPACE(0);
		IF @RowCount <> @ExpectedCount
			SET @ErrMsg = N'DBAFilenote - Delete: Discrepancy between actual and expected row counts.';
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBAFileNote - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;

SET @ExpectedCount = 
		ISNULL((SELECT	COUNT(*)
				FROM	#Action
				WHERE	TableId = @TableId_DBAFileColumn),
				0);
IF @ExpectedCount > 0
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileColumn - Delete';
		DELETE	dbo.DBAFileColumn
		FROM	dbo.DBAFileColumn dfc
				INNER JOIN #Action a
					ON a.TableId = @TableId_DBAFileColumn
					AND a.RowId1 = dfc.FileRowId
					AND a.RowId2 = dfc.ColSeq
		SET @RowCount = @@ROWCOUNT;
		PRINT	CONVERT(varchar(10), @RowCount) + ' of ' + CONVERT(varchar(10), @ExpectedCount) + ' rows deleted.';
		PRINT	SPACE(0);
		IF @RowCount <> @ExpectedCount
			SET @ErrMsg = N'DBAFileColumn - Delete: Discrepancy between actual and expected row counts.';
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBAFileColumn - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;

SET @ExpectedCount = 
		ISNULL((SELECT	COUNT(*)
				FROM	#Action
				WHERE	TableId = @TableId_DBAFileRow),
				0);
IF @ExpectedCount > 0
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileRow - Delete';
		DELETE	dbo.DBAFileRow
		FROM	dbo.DBAFileRow dfr
				INNER JOIN #Action a
					ON a.TableId = @TableId_DBAFileRow
					AND a.RowId1 = dfr.FileRowId;
		SET @RowCount = @@ROWCOUNT;
		PRINT	CONVERT(varchar(10), @RowCount) + ' of ' + CONVERT(varchar(10), @ExpectedCount) + ' rows deleted.';
		PRINT	SPACE(0);
		IF @RowCount <> @ExpectedCount
			SET @ErrMsg = N'DBAFileRow - Delete: Discrepancy between actual and expected row counts.';
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBAFileRow - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;

SET @ExpectedCount = 
		ISNULL((SELECT	COUNT(*)
				FROM	#Action
				WHERE	TableId = @TableId_DBAFile),
				0);
IF @ExpectedCount > 0
BEGIN;
	BEGIN TRY
		PRINT	'DBAFile - Delete';
		DELETE	dbo.DBAFile
		FROM	dbo.DBAFile df
				INNER JOIN #Action a
					ON a.TableId = @TableId_DBAFile
					AND a.RowId1 = df.FileId;
		SET @RowCount = @@ROWCOUNT;
		PRINT	CONVERT(varchar(10), @RowCount) + ' of ' + CONVERT(varchar(10), @ExpectedCount) + ' rows deleted.';
		PRINT	SPACE(0);
		IF @RowCount <> @ExpectedCount
			SET @ErrMsg = N'DBAFile - Delete: Discrepancy between actual and expected row counts.';
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBAFile - Delete: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
	OR LEN(@ErrMsg) > 0
		GOTO Failed;
END;

----------
GOTO Done;
Failed:
	IF @Error <> 0
		PRINT	'Error: ' + CONVERT(varchar(10), @Error);
	IF LEN(@ErrMsg) > 0
		PRINT	@ErrMsg;
	RAISERROR('Script failed!', 18, 1);
Done:
GO
