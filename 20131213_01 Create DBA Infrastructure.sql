-- Run on the localhost Server
USE DBADefault;

SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET XACT_ABORT OFF;
SET CONCAT_NULL_YIELDS_NULL ON;

IF OBJECT_ID('tempdb..#Error') IS NOT NULL DROP TABLE #Error;
GO
DECLARE	@Error		int,
		@ErrMsg		nvarchar(2048);
		
SELECT	@Error = 0,
		@ErrMsg = SPACE(0);

BEGIN TRY
	CREATE TABLE #Error
			(RowSeq				int				NOT NULL IDENTITY(1, 1),
			Item				nvarchar(2048)	NOT NULL,
			Msg					nvarchar(2048)	NOT NULL
			PRIMARY KEY CLUSTERED
				(RowSeq)
				WITH FILLFACTOR = 100);
END TRY
BEGIN CATCH
	SELECT	@Error = ERROR_NUMBER(),
			@ErrMsg = ERROR_MESSAGE();
END CATCH
IF @Error <> 0
BEGIN;
	PRINT @ErrMsg;
	GOTO Failed;
END;
	
------------------------------
BEGIN TRY; PRINT 'Transaction started.'; PRINT SPACE(0);
------------------------------

------------------------------

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
		PRINT	'DBASession - Add DBName';
		ALTER TABLE dbo.DBASession
			ADD DBName nvarchar(128) NULL;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBASession - Add DBName', ERROR_MESSAGE());
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
		PRINT	'DBASession - Add ApplicationId';
		ALTER TABLE dbo.DBASession
			ADD ApplicationId smallint NULL;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBASession - Add ApplicationId', ERROR_MESSAGE());
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
		PRINT	'DBASession - Add IssueNumber';
		ALTER TABLE dbo.DBASession
			ADD IssueNumber int NULL;
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES(N'DBASession - Add IssueNumber', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) 
		GOTO Failed;
END;

------------------------------

IF OBJECT_ID('dbo.DBAEnvironment') IS NULL
BEGIN;
	BEGIN TRY
		CREATE TABLE dbo.DBAEnvironment
				(EnvironmentId			tinyint			NOT NULL,
				EnvironmentDescription	varchar(255)	NOT NULL,
				EnvironmentAbbreviation	varchar(10)		NOT NULL
		PRIMARY KEY CLUSTERED
			(EnvironmentId)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(EnvironmentDescription)
			WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH					
		INSERT #Error (Item, Msg)
		VALUES(N'DBAEnvironment - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;
	
IF OBJECT_ID('dbo.DBAEnvironment') IS NOT NULL
BEGIN;
	IF NOT EXISTS(SELECT 1 FROM dbo.DBAEnvironment)
	BEGIN;
		BEGIN TRY
			PRINT	'DBAEnvironment - Insert All';
			INSERT	dbo.DBAEnvironment
					(EnvironmentDescription, EnvironmentAbbreviation)
			VALUES	('Production', 'Prod'),
					('Testing', 'Test'),
					('Development', 'Dev');
		END TRY
		BEGIN CATCH					
			INSERT #Error (Item, Msg)
			VALUES(N'DBAEnvironment - Insert All', ERROR_MESSAGE());
		END CATCH
		IF EXISTS(SELECT 1 FROM #Error)
			GOTO Failed;
	END;
END;
		
------------------------------
	
IF OBJECT_ID('dbo.DBAServerType') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAServerType - Create';
		CREATE TABLE dbo.DBAServerType
			(ServerTypeId			tinyint				NOT NULL IDENTITY(1, 1),
			ServerTypeDescription	varchar(250)		NOT NULL,
			ServerTypeAbbreviation	varchar(10)			NOT NULL
			PRIMARY KEY CLUSTERED
				(ServerTypeId)
				WITH FILLFACTOR = 100,
			UNIQUE NONCLUSTERED
				(ServerTypeDescription)
				WITH FILLFACTOR = 100,
			UNIQUE NONCLUSTERED
				(SeverTypeAbbreviation)
				WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		INSERT	#Error (Item, Msg)
		VALUES	('DBAServerType - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

IF OBJECT_ID('DBAServerType') IS NOT NULL
BEGIN;
	IF NOT EXISTS(SELECT 1 FROM dbo.DBAServerType)
	BEGIN;
		BEGIN TRY
			PRINT	'DBAServerType - Insert All';
			INSERT dbo.DBAServerType
					(ServerTypeDescription, ServerTypeAbbreviation)
			VALUES	('SQL Server', 'SQL');
		END TRY
		BEGIN CATCH
			INSERT #Error (Item, Msg)
			VALUES(N'DBAServerType - Insert All', ERROR_MESSAGE());
		END CATCH
		IF EXISTS(SELECT 1 FROM #Error)
			GOTO Failed;
	END;
END;

------------------------------

IF OBJECT_ID('dbo.DBAServer') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBAServer - Create';
		CREATE TABLE dbo.DBAServer
				(ServerId			smallint			NOT NULL IDENTITY(1, 1),
				ServerName			nvarchar(128)		NOT NULL,
				ServerTypeId		tinyint				NOT NULL,
				EnvironmentId		tinyint				NOT NULL,
				EffectiveFrom		datetime			NOT NULL DEFAULT('1900-01-01'),
				EffectiveThru		datetime			NOT NULL DEFAULT('2999-12-31')
				PRIMARY KEY CLUSTERED
					(ServerId)
					WITH FILLFACTOR = 100,
				UNIQUE NONCLUSTERED
					(ServerName)
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH					
		INSERT #Error (Item, Msg)
		VALUES(N'DBAServer - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;

--... Add indices and FK's for ServerTypeId, EnvironmentId

IF OBJECT_ID('dbo.DBAServer') IS NOT NULL
BEGIN;
	IF NOT EXISTS(SELECT 1 FROM dbo.DBAServer)
	BEGIN;
		BEGIN TRY
			PRINT	'DBAServer - Insert All';
			INSERT	dbo.DBAServer
					(ServerName,
					ServerTypeId,
					EnvironmentId)
			SELECT	DBInfo.ServerName,
					dst.ServerTypeId,
					de.EnvironmentId
			FROM	(VALUES	('prod-sql', 'SQL Server', 'Prod'),
							('localhost', 'SQL Server', 'DBA Admin'),
							('test-sql', 'SQL Server', 'Test'),
							('dev-sql', 'SQL Server', 'Dev'),
					AS DBInfo(ServerName, ServerType, Environment)
					LEFT JOIN dbo.DBAServerType dst
						ON dst.ServerTypeAbbreviation = DBInfo.ServerType
					LEFT JOIN dbo.DBAEnvironment de
						ON de.EnvironmentAbbreviation = DBInfo.Environment
			ORDER BY de.EnvironmentId, dst.ServerTypeId, DBInfo.ServerName;
		END TRY
		BEGIN CATCH
			INSERT #Error (Item, Msg)
			VALUES(N'DBAServer - Insert All', ERROR_MESSAGE());
		END CATCH
		IF EXISTS(SELECT 1 FROM #Error)
			GOTO Failed;
	END;
END;
					
------------------------------

IF OBJECT_ID('dbo.DBADatabaseType') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBADatabaseType - Create';
		CREATE TABLE dbo.DBADatabaseType
				(DatabaseTypeId				tinyint			NOT NULL IDENTITY(1, 1),
				DatabaseTypeDescription		varchar(255)	NOT NULL,
				DatabaseTypeAbbreviation	varchar(10)		NOT NULL
		PRIMARY KEY CLUSTERED
			(DatabaseTypeId)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(DatabaseTypeDescription)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(DatabaseTypeAbbreviation)
			WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH					
		INSERT #Error (Item, Msg)
		VALUES(N'DBADatabaseType - Create', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error)
		GOTO Failed;
END;
			
IF OBJECT_ID('dbo.DBADatabaseType') IS NOT NULL
BEGIN;
	IF NOT EXISTS(SELECT 1 FROM dbo.DBADatabaseType)
	BEGIN;
		BEGIN TRY
			PRINT	'DBADatabaseType - Insert All';
			INSERT dbo.DBADatabaseType
					(DatabseTypeDescription, DatabaseTypeAbbreviation)
			VALUES	('STAR', 'STAR'),
					('Issue Tracking', 'AppTracking'),
					('DBA Administration', 'DBA'),
					('LaserFiche', 'LF');
		END TRY
		BEGIN CATCH					
			INSERT #Error (Item, Msg)
			VALUES(N'DBADatabaseType - Insert All', ERROR_MESSAGE());
		END CATCH
		IF EXISTS(SELECT 1 FROM #Error)
			GOTO Failed;
	END;
END;
	
------------------------------	
--...	
--IF OBJECT_ID('dbo.DBADatabase') IS NULL
--	BEGIN TRY
--		PRINT	'DBADatabase - Create';
--		CREATE TABLE dbo.DBADatabase
--			(DBId				smallint			NOT NULL IDENTITY(1, 1),
--			ServerId			smallint			NOT NULL,
--			DBName				nvarchar(128)		NOT NULL,
					








------------------------------
IF @@TRANCOUNT > 0
BEGIN;
	ROLLBACK TRAN; PRINT 'Transaction rolled back for testing.';
	--COMMIT TRAN; PRINT 'Transaction completed.';
	PRINT SPACE(0);
END;

GOTO Done;
Failed:
	IF OBJECT_ID('tempdb..#Error') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #Error)
		BEGIN;
			SELECT	Item = DBADefault.dbo.f_DQuoteString(Item),
					Msg = DBADefault.dbo.f_DQuoteString(Msg)
			FROM	#Error
			ORDER BY RowSeq;
			
			PRINT	SPACE(0);
			TRUNCATE TABLE #Error;
		END;
	END;
			
	IF @@TRANCOUNT > 0
	BEGIN;
		ROLLBACK TRAN; PRINT 'Transaction rolled back due to error.'; PRINT SPACE(0);
	END;
		
	IF @ErrMsg = SPACE(0)
		SET @ErrMsg = 'Script failed';
	
	RAISERROR (@ErrMsg, 18, 1);
	
Done:
GO

	IF OBJECT_ID('tempdb..#Error') IS NOT NULL
	BEGIN;
		IF EXISTS(SELECT 1 FROM #Error)
		BEGIN;
			SELECT	Item = DBADefault.dbo.f_DQuoteString(Item),
					Msg = DBADefault.dbo.f_DQuoteString(Msg)
			FROM	#Error
			ORDER BY RowSeq;
			
			PRINT	SPACE(0);
			TRUNCATE TABLE #Error;
		END;
	END;
	
	IF @@TRANCOUNT > 0
	BEGIN;
		ROLLBACK TRAN; PRINT 'Transaction rolled back after fall-through.'; PRINT SPACE(0);
			
		RAISERROR ('Script failed!', 18, 1);
	END

		