USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET QUOTED_IDENTIFIER ON;
IF OBJECT_ID('tempdb..#Error') IS NOT NULL DROP TABLE #Error;
GO
CREATE TABLE #Error
		(RowSeq			int				NOT NULL IDENTITY(1, 1),
		Item			nvarchar(128)	NOT NULL,
		Msg				nvarchar(2048)	NOT NULL)
GO		
PRINT	'DBA Database Maintenance';
PRINT	'Create DBA Infrastructure';
PRINT	'DB: ' + CONVERT(varchar(128), SERVERPROPERTY('ServerName')) + '.' + CONVERT(varchar(128), DB_NAME());
PRINT	'Run: ' + CONVERT(char(23), GETDATE(), 121);
PRINT	SPACE(0);

IF OBJECT_ID('dbo.DBASession') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBASession - Create';
		CREATE TABLE dbo.DBASession
				(DBASessionId			int				NOT NULL IDENTITY(1, 1),
				SessionDateTime			datetime		NOT NULL,
				DBName					nvarchar(128)	NULL,
				IssueNumber				int				NULL,
				ApplicationId			smallint		NULL
				CONSTRAINT DBASessionPK
					PRIMARY KEY CLUSTERED
						(DBASessionId)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBASession - Create', ERROR_MESSAGE())
	END CATCH
END;

IF NOT EXISTS(
	SELECT 1
	FROM	INFORMATION_SCHEMA.COLUMNS
	WHERE	TABLE_NAME = N'DBASession'
	AND		COLUMN_NAME = N'DBName')
BEGIN;
	BEGIN TRY
		PRINT	'DBASession.DBName - Add';
		ALTER TABLE dbo.DBASession
			ADD DBName nvarchar(128) NULL;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBASession.DBName - Add', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF NOT EXISTS(
	SELECT 1
	FROM	INFORMATION_SCHEMA.COLUMNS
	WHERE	TABLE_NAME = N'DBASession'
	AND		COLUMN_NAME = N'ApplicationId')
BEGIN;
	BEGIN TRY
		PRINT	'DBASession.ApplicationId - Add';
		ALTER TABLE dbo.DBASession
			ADD ApplicationId smallint NULL;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBASession.ApplicationId - Add', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF NOT EXISTS(
	SELECT 1
	FROM	INFORMATION_SCHEMA.COLUMNS
	WHERE	TABLE_NAME = N'DBASession'
	AND		COLUMN_NAME = N'IssueNumber')
BEGIN;
	BEGIN TRY
		PRINT	'DBASession.IssueNumber - Add';
		ALTER TABLE dbo.DBASession
			ADD IssueNumber int NULL;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBASession.IssueNumber - Add', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

/*****
IF OBJECT_ID('dbo.DBAApplication') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAApplication';
		CREATE TABLE DBAApplication
				(ApplicationId		smallint		NOT NULL IDENTITY(1, 1),
				ApplicationName		varchar(60)		NOT NULL,
				CreationDate		date			NOT NULL DEFAULT(GETDATE()),
				EffectiveUntil		datetime		NULL
				CONSTRAINT DBAApplicationPK
					PRIMARY KEY CLUSTERED
						(ApplicationId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAApplicationUNC1
					UNIQUE NONCLUSTERED
						(ApplicationName)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAApplication - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF NOT EXISTS(SELECT 1 FROM dbo.DBAApplication)
BEGIN;
	BEGIN TRY
		PRINT	'DBAApplication - Insert';
		SET IDENTITY_INSERT dbo.DBAApplication ON
		INSERT dbo.DBAApplication
				(ApplicationId, ApplicationName)
		VALUES	(1, 'KeepSpouse Lookup');
		SET IDENTITY_INSERT dbo.DBAApplications OFF
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAApplication - Insert', ERROR_MESSAGE());
		SET IDENTITY_INSERT dbo.DBAApplications OFF
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;
*****/

IF OBJECT_ID('dbo.DBAActionCode') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAActionCode - Create';
		CREATE TABLE dbo.DBAActionCode
				(ActionId				tinyint		NOT NULL IDENTITY(1, 1),
				ActionCode				char(1)		NOT NULL,
				"Description"			varchar(10)	NOT NULL
		CONSTRAINT DBAActionCodePK
			PRIMARY KEY CLUSTERED
				(ActionId)
				WITH FILLFACTOR = 100,
		CONSTRAINT DBAActionCodeUNC1
			UNIQUE NONCLUSTERED
				(ActionCode)
				WITH FILLFACTOR = 100,
		CONSTRAINT DBAActionCodeUNC2
			UNIQUE NONCLUSTERED
				("Description")
				WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBAActionCode - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF NOT EXISTS(SELECT 1 FROM dbo.DBAActionCode)
BEGIN;
	BEGIN TRY
		PRINT	'DBAActionCode - Load'
		SET IDENTITY_INSERT dbo.DBAActionCode ON;
		INSERT	dbo.DBAActionCode
				(ActionId, ActionCode, "Description")
		VALUES	(1, SPACE(1), 'No Action'),
				(2, 'D', 'Delete'),
				(3, 'I', 'Insert'),
				(4, 'U', 'Update'),
				(5, 'S', 'Skip');
		SET IDENTITY_INSERT dbo.DBAActionCode OFF;
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBAActionCode - Load', ERROR_MESSAGE());
		SET IDENTITY_INSERT dbo.DBAActionCode OFF;
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

				


IF OBJECT_ID('dbo.DBABeforeAfterCode') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBABeforeAfterCode - Create';
		CREATE TABLE dbo.DBABeforeAfterCode
			(BeforeAfterId			tinyint			NOT NULL IDENTITY(1, 1),
			"Description"			varchar(10)		NOT NULL
			CONSTRAINT DBABeforeAfterPK
				PRIMARY KEY CLUSTERED
					(BeforeAfterId)
					WITH FILLFACTOR = 100,
			CONSTRAINT DBABeforeAfterUNC1
				UNIQUE NONCLUSTERED
					("Description")
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBABeforeAfterCode - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF NOT EXISTS(SELECT 1 FROM dbo.DBABeforeAfterCode)
BEGIN;
	BEGIN TRY
		PRINT	'DBABeforAfterCode - Load'
		SET IDENTITY_INSERT dbo.DBABeforeAfterCode ON;
		INSERT	dbo.DBABeforeAfterCode
				(BeforeAfterId,
				"Description")
		VALUES	(1, 'Before'),
				(2, 'After');
		SET IDENTITY_INSERT dbo.DBABeforeAfterCode OFF;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBABeforeAfter - Load', ERROR_MESSAGE());
		SET IDENTITY_INSERT dbo.DBABeforeAfterCode OFF;
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;
	
IF OBJECT_ID('dbo.DBASeverityCode') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBASeverityCode - Create'
		CREATE TABLE dbo.DBASeverityCode
				(SeverityId			tinyint		NOT NULL IDENTITY(1, 1),
				SeverityCode		char(2)		NOT NULL,
				"Description"		varchar(20)	NOT NULL
				CONSTRAINT DBASeverityCodePK
					PRIMARY KEY CLUSTERED
						(SeverityId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBASeverityCodeUNC1
					UNIQUE NONCLUSTERED
						(SeverityCode)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBASeverityCodeUNC2
					UNIQUE NONCLUSTERED
						("Description")
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBASeverityCode - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF NOT EXISTS(SELECT 1 FROM dbo.DBASeverityCode)
BEGIN;
	BEGIN TRY
		PRINT	'DBASeverityCode - Load';
		SET IDENTITY_INSERT dbo.DBASeverityCode ON
		INSERT	dbo.DBASeverityCode
				(SeverityId,
				SeverityCode,
				"Description")
		VALUES	(1, 'I ', 'Informational'),
				(2, 'W ', 'Warning'),
				(3, 'F ', 'Fatal'),
				(4, 'FR', 'Fatal (Row)'),
				(5, 'FF', 'Fatal (File)');
		SET IDENTITY_INSERT dbo.DBASeverityCode OFF
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBASeverityCode - Load', ERROR_MESSAGE())
		SET IDENTITY_INSERT dbo.DBASeverityCode OFF
	END CATCH						
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF OBJECT_ID('dbo.DBAFilePath') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAFilePath';
		CREATE TABLE dbo.DBAFilePath
				(FilePathId			smallint		NOT NULL IDENTITY(1, 1),
				FilePath			varchar(255)	NOT NULL
				CONSTRAINT DBAFilePathsPK
					PRIMARY KEY CLUSTERED
						(FilePathId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAFilePathsUNC1
					UNIQUE NONCLUSTERED
						(FilePath)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFilePath - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

/*****
IF OBJECT_ID('dbo.DBAFileOrganization') IS NULL
BEGIN;
	BEGIN TRY
		CREATE TABLE dbo.DBAFileOrganization
				(FileOrganizationId		tinyint		NOT NULL IDENTITY(1, 1),
				FileOrganizationName	varchar(60)	NOT NULL,
				CreationDate			date		NOT NULL DEFAULT(GETDATE()),
				EffectiveUntil			datetime	NULL
				CONSTRAINT DBAFileOrganizationPK
					PRIMARY KEY CLUSTERED
						(FileOrganizationId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAFileOrganizationUNC1
					UNIQUE NONCLUSTERED	
						(FileOrganizationName, EffectiveUntil)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFilePath - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF NOT EXISTS(SELECT 1 FROM dbo.DBAFileOrganization)
BEGIN;
	BEGIN TRY;
		PRINT	'DBAFileOrganization - Insert';
		SET IDENTITY_INSERT dbo.DBAFileOrganization ON;
		INSERT	dbo.DBAFileOrganization
				(FileOrganizationId, FileOrganizationName)
		VALUES	(1, 'Unknown'),
				(2, 'Fixed Length'),
				(3, 'Tab Delimited'),
				(4, 'Comma Delimited');
		SET IDENTITY_INSERT dbo.DBAFileOrganization OFF;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileOrganization - Insert', ERROR_MESSAGE());
		SET IDENTITY_INSERT dbo.DBAFileOrganization OFF;
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF OBJECT_ID('dbo.DBAFileLayout') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileLayout - Create';
		CREATE TABLE dbo.DBAFileLayout
				(FileLayoutId		smallint		NOT NULL IDENTITY(1, 1),
				FileLayoutName		varchar(30)		NOT NULL,
				CreationDate		date			NOT NULL DEFAULT(GETDATE()),
				EffectiveUntil		datetime		NULL,
				FileOrganizationId	tinyint			NOT NULL,
				ColumnSeperator		varchar(5)		NULL,
				BCPFormatName		varchar(60)		NULL,
				CONSTRAINT DBAFileLayoutPK
					PRIMARY KEY CLUSTERED
						(FileLayoutId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAFileLayoutUNC1
					UNIQUE NONCLUSTERED
					(FileLayoutName, EffectiveUntil)
					WITH FILLFACTOR = 100);	
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileLayout - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF NOT EXISTS(SELECT 1 FROM dbo.DBAFileLayout)
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileLayout - Insert';
		SET IDENTITY_INSERT dbo.DBAFileLayOut ON;
		INSERT	dbo.DBAFileLayout
				(FileLayoutId, FileLayoutName, FileOrganizationId, BCPFormatName)
		VALUES(1, 'Generic', 1, 'Generic.RowSeq.Contents.fmt.txt');
		SET IDENTITY_INSERT dbo.DBAFileLayOut OFF;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileLayout - Insert', ERROR_MESSAGE());
		SET IDENTITY_INSERT dbo.DBAFileLayOut OFF;
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF OBJECT_ID('dbo.DBAFileLayoutColumn') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileLayoutColumn - Create';
		CREATE TABLE dbo.DBAFileLayoutColumn
				(FileLayoutId		smallint		NOT NULL,
				ColumnSeq			tinyint			NOT NULL,
				ColumnName			varchar(60)		NOT NULL,
				Datatype			varchar(10)		NOT NULL,
				Precision			int				NOT NULL,
				Scale				tinyint			NOT NULL
				CONSTRAINT DBAFileLayoutColumnPK
					PRIMARY KEY CLUSTERED
						(FileLayoutId, ColumnSeq)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAFileLayoutColumnUNC1
					UNIQUE NONCLUSTERED
						(ColumnName, FileLayoutId)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileLayoutColumn - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;			
*****/
			
IF OBJECT_ID('dbo.DBAFile') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFile - Create';
		CREATE TABLE dbo.DBAFile
				(FileId				int				NOT NULL IDENTITY(1, 1),
				FilePathId			smallint		NOT NULL,
				"FileName"			varchar(255)	NOT NULL,
				FileSet				tinyint			NOT NULL DEFAULT(0),
				UserState			tinyint			NOT NULL DEFAULT(0),
				DBASessionId		int				NULL
				CONSTRAINT DBAFilePK
					PRIMARY KEY CLUSTERED
						(FileId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAFileUNC1
					UNIQUE NONCLUSTERED
						("FileName", FilePathId, DBASessionId)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFile - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;
	
IF NOT EXISTS(
		SELECT 1
		FROM	sys.indexes
		WHERE	"OBJECT_ID" = OBJECT_ID('DBAFile')
		AND		"name" = 'DBAFileIDX1')
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileIDX1 - Create'
		CREATE NONCLUSTERED INDEX DBAFileIDX1
			ON dbo.DBAFile
				(FilePathId)
				WITH FILLFACTOR = 100;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, MSg)
		VALUES(N'DBAFileIDX1 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;
	
IF NOT EXISTS(
		SELECT 1
		FROM	sys.indexes
		WHERE	"OBJECT_ID" = OBJECT_ID('DBAFile')
		AND		"name" = 'DBAFileIDX2')
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileIDX2 - Create'
		CREATE NONCLUSTERED INDEX DBAFileIDX2
			ON dbo.DBAFile
				(DBASessionId)
				WITH FILLFACTOR = 100;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUeS(N'DBAFileIDX2 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF OBJECT_ID('DBAFileFK1') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileFK1 - Create';
		ALTER TABLE dbo.DBAFile
			ADD CONSTRAINT DBAFileFK1
				FOREIGN KEY
					(FilePathId)
				REFERENCES dbo.DBAFilePath 
					(FilePathId);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileFK1 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF OBJECT_ID('DBAFileFK2') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileFK2 - Create';
		ALTER TABLE dbo.DBAFile
			ADD CONSTRAINT DBAFileFK2
				FOREIGN KEY
					(DBASessionId)
				REFERENCES dbo.DBASession 
					(DBASessionId);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileFK2 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;			
		
IF OBJECT_ID('dbo.DBAFileRow') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileRow - Create';
		CREATE TABLE dbo.DBAFileRow
				(FileRowId			int				NOT NULL IDENTITY(1, 1),
				FileId				int				NOT NULL,
				Contents			varchar(8000)	NOT NULL
				CONSTRAINT DBAFileRowPK
					PRIMARY KEY CLUSTERED
						(FileRowId)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileRow - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;
END;

IF NOT EXISTS(
		SELECT 1
		FROM	sys.indexes
		WHERE	"OBJECT_ID" = OBJECT_ID('DBAFileRow')
		AND		"name" = 'DBAFileRowIDX1')
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileRowIDX1 - Create';
		CREATE NONCLUSTERED INDEX DBAFileRowIDX1
			ON dbo.DBAFileRow
					(FileId)
					WITH FILLFACTOR = 100;
	END TRY
	BEGIN CATCH
		INSERT #Error (Item, Msg)
		VALUES(N'DBAFileRowIDX1 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF OBJECT_ID('DBAFileRowFK1') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileRowFK1 - Create';
		ALTER TABLE dbo.DBAFileRow
			ADD CONSTRAINT DBAFileRowFK1
				FOREIGN KEY
					(FileId)
				REFERENCES dbo.DBAFile (FileId);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileRowFK1 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF OBJECT_ID('dbo.DBAFileColumn') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileColumn - Create';
		CREATE TABLE dbo.DBAFileColumn
				(FileRowId			int				NOT NULL,
				ColSeq				smallint		NOT NULL,
				Contents			varchar(8000)	NOT NULL
				CONSTRAINT DBAFileColumnsPK
					PRIMARY KEY CLUSTERED
						(FileRowId, ColSeq)
						WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileColumn - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

IF OBJECT_ID('DBAFileColumnFK1') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileColumnFK1 - Create';
		ALTER TABLE dbo.DBAFileColumn
			ADD CONSTRAINT DBAFileColumnFK1
				FOREIGN KEY
					(FileRowId)
				REFERENCES dbo.DBAFileRow (FileRowId);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileRowFK1 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;			
	
IF OBJECT_ID('dbo.DBAFileNote') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAFileNote - Create';
		CREATE TABLE dbo.DBAFileNote
				(RowSeq			int				NOT NULL IDENTITY(1, 1),
				FileId			int				NOT NULL,
				FileRowId		int				NULL,
				ColSeq			smallint		NULL,
				Severity		char(2)			NULL,
				Contents		varchar(8000)	NOT NULL
				PRIMARY KEY CLUSTERED
					(RowSeq)
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileNote - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;			
			
IF OBJECT_ID('DBAFileNoteFK1') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAFileNoteFK1 on FileId to DBAFile';
		ALTER TABLE dbo.DBAFileNote
			ADD CONSTRAINT DBAFileNoteFK1
				FOREIGN KEY
					(FileId)
				REFERENCES dbo.DBAFile
					(FileId);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileNoteFK1 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;			
		
IF OBJECT_ID('DBAFileNoteFK2') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAFileNoteFK2 on FileRowId to DBAFileRow';
		ALTER TABLE dbo.DBAFileNote
			ADD CONSTRAINT DBAFileNoteFK2
				FOREIGN KEY
					(FileRowId)
				REFERENCES dbo.DBAFileRow
					(FileRowId);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileNoteFK2 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;													
		
IF OBJECT_ID('DBAFileNoteFK3') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAFileNoteFK3 on FileRowId, ColSeq to DBAFileColumn';
		ALTER TABLE dbo.DBAFileNote
			ADD CONSTRAINT DBAFileNoteFK3
				FOREIGN KEY
					(FileRowId, ColSeq)
				REFERENCES dbo.DBAFileColumn
					(FileRowId, ColSeq);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileNoteFK3 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;													
		
IF OBJECT_ID('DBAFileNoteFK4') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAFileNoteFK4 on Severity to DBASeverityCode';
		ALTER TABLE dbo.DBAFileNote
			ADD CONSTRAINT DBAFileNoteFK4
				FOREIGN KEY
					(Severity)
				REFERENCES dbo.DBASeverityCode
					(SeverityCode);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBAFileNoteFK4 - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;													

----------
GOTO Done;
Failed:
	PRINT	'Script failed';
	SELECT	Item,
			Msg
	FROM	#Error
	ORDER BY RowSeq;
	TRUNCATE TABLE #Error;
Done:
GO

BEGIN TRY
	SET IDENTITY_INSERT dbo.DBAActionCode OFF
END TRY
BEGIN CATCH
END CATCH

BEGIN TRY
	SET IDENTITY_INSERT dbo.DBABeforeAfterCode OFF
END TRY
BEGIN CATCH
END CATCH
					
BEGIN TRY
	SET IDENTITY_INSERT dbo.DBASeverityCode OFF
END TRY
BEGIN CATCH
END CATCH
					