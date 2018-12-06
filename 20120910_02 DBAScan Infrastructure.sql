IF OBJECT_ID('tempdb..#Error') IS NOT NULL DROP TABLE #Error;
GO
CREATE TABLE #Error
		(OccurenceId			int				NOT NULL IDENTITY(1, 1),
		ObjectName				nvarchar(128)	NOT NULL,
		ErrorMessage			nvarchar(128)	NOT NULL);

BEGIN TRY
	USE DBADefault;
	SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET QUOTED_IDENTIFIER ON;
END TRY
BEGIN CATCH
	INSERT #Error(ObjectName, ErrorMessage)
	VALUES(N'Script initialization', ERROR_MESSAGE());
END CATCH
IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;	
	
IF OBJECT_ID('dbo.DBAScanSessionType') IS NULL
BEGIN
	BEGIN TRY
		PRINT	'Create DBAScanSessionType';
		CREATE TABLE dbo.DBAScanSessionType
				(ScanSessionTypeId			tinyint		NOT NULL,
				ScanSessionTypeDescription	varchar(60)	NOT NULL
				CONSTRAINT DBAScanSessionTypePK
					PRIMARY KEY CLUSTERED
						(ScanSessionTypeId)
						WITH FILLFACTOR = 100,
				CONSTRAINT DBAScanSessionTypeUNC1
					UNIQUE NONCLUSTERED
						(ScanSessionTypeDescription)
						WITH FILLFACTOR = 100);

		-- Initial contents.						
		INSERT dbo.DBAScanSessionType
				(ScanSessionTypeId, ScanSessionTypeDescription)
		VALUES	(1, 'Wait Stats'),
				(2, 'Latch Stats'),
				(3, 'Index Usage');

	END TRY
	BEGIN CATCH
		INSERT	#Error (ObjectName, ErrorMessage)
		VALUES(N'BAScanSessionType', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;
END;
					
IF OBJECT_ID('dbo.DBAScanSession') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAScanSession';
		CREATE TABLE dbo.DBAScanSession
				(ScanSessionId			int				NOT NULL IDENTITY(1, 1),
				ScanSessionTypeId		int				NOT NULL,
				TempDBCreationDateTime	datetime		NOT NULL,
				ServerName				nvarchar(128)	NOT NULL 
					CONSTRAINT DBAScanSessionDF_ServerName 
						DEFAULT SERVERPROPERTY('ServerName'),
				InstanceName			nvarchar(128)	NOT NULL 
					CONSTRAINT DBAScanSessionDF_InstanceName 
						DEFAULT SERVERPROPERTY('InstanceName'),				
				DatabaseName			nvarchar(128)	NULL,
				StartDateTime			datetime		NOT NULL 
					CONSTRAINT DBAScanSessionDF_StartDateTime 
						DEFAULT CURRENT_TIMESTAMP,
				DurationMilliseconds	bigint			NOT NULL 
					CONSTRAINT DBAScanSessionDF_DurationMilliseconds DEFAULT(-1));
				
		ALTER TABLE dbo.DBAScanSession
			ADD CONSTRAINT DBAScanSessionPK
				PRIMARY KEY CLUSTERED
					(ScanSessionId)
					WITH FILLFACTOR = 100;

	CREATE INDEX DBAScanSessionIDX1
		ON dbo.DBAScanSession
			(TempDBCreationDateTime, ScanSessionTypeId)
				INCLUDE (DatabaseName)
				WITH FILLFACTOR = 100;
	END TRY
					
	BEGIN CATCH
		INSERT	#Error (ObjectName, ErrorMessage)
		VALUES(N'DBAScanSession', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;
END;

------------------------------

IF OBJECT_ID('dbo.DBAScan_Wait_Exclusion') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAScan_Wait_Exclusion';
		CREATE TABLE dbo.DBAScan_Wait_Exclusion
				(WaitType		nvarchar(256)		NOT NULL
				PRIMARY KEY CLUSTERED
					(WaitType)
					WITH FILLFACTOR = 100;
					
	INSERT	dbo.DBAScan_Wait_Exclusion
			(WaitType)
	VALUES	('REQUEST_FOR_DEADLOCK_SEARCH'),
			('SQLTRACE_INCREMENTAL_FLUSH_SLEEP'),
			('SQLTRACE_BUFFER_FLUSH'),
			('LAZYWRITER_SLEEP'),
			('XE_TIMER_EVENT'),
			('XE_DISPATCHER_WAIT'),
			('FT_IFTS_SCHEDULER_IDLE_WAIT'),
			('LOGMGR_QUEUE'),
			('CHECKPOINT_QUEUE'),
			('BROKER_TO_FLUSH'),
			('BROKER_TASK_STOP'),
			('BROKER_EVENTHANDLER'),
			('SLEEP_TASK'),
			('WAITFOR'),
			('DBMIRROR_DBM_MUTEX'),
			('DBMIRROR_EVENTS_QUEUE'),
			('DBMIRRORING_CMD'),
			('DISPATCHER_QUEUE_SEMAPHORE'),
			('BROKER_RECEIVE_WAITFOR'),
			('CLR_AUTO_EVENT'),
			('DIRTY_PAGE_POLL'),
			('HADR_FILESTREAM_IOMGR_IOCOMPLETION'),
			('ONDEMAND_TASK_QUEUE'),
			('FT_IFTSHC_MUTEX');
	END TRY
					
	BEGIN CATCH
		INSERT	#Error (ObjectName, ErrorMessage)
		VALUES(N'DBAScan_Wait_Exclusion', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;
END;		

IF OBJECT_ID('dbo.DBAScan_WaitItem') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAScan_WaitItem';
		CREATE TABLE dbo.DBAScan_WaitItem
				(ScanSessionId		int					NOT NULL,
				WaitType			nvarchar(256)		NOT NULL,
				WaitTime_ms			bigint				NOT NULL,
				WaitingTasks		bigint				NOT NULL,
				PRIMARY KEY CLUSTERED
					(ScanSessionId, WaitType)
					WITH FILLFACTOR = 100);
	END TRY
					
	BEGIN CATCH
		INSERT	#Error (ObjectName, ErrorMessage)
		VALUES(N'DBAScan_WaitItem', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;
END;		

-----------------------------

IF OBJECT_ID('dbo.DBAScan_IndexUsageItem') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAScan_IndexUsageItem';
		CREATE TABLE dbo.DBAScan_IndexUsageItem
				(IndexUsageItemId		bigint			NOT NULL IDENTITY(1, 1),
				ScanSessionId			int				NOT NULL,
				ObjectName				nvarchar(128)	NOT NULL,
				IndexName				nvarchar(128)	NOT NULL,
				UserSeeks				bigint			NOT NULL,
				UserScans				bigint			NOT NULL,
				UserLookups				bigint			NOT NULL,
				UserUpdates				bigint			NOT NULL,
				LsatUserSeek			datetime		NULL,
				LastUserScan			datetime		NULL,
				LastUserLookup			datetime		NULL,
				LastUserUpdate			datetime		NULL,
				SystemSeeks				bigint			NOT NULL,
				SystemScans				bigint			NOT NULL,
				SystemLookups			bigint			NOT NULL,
				SystemUpdates			bigint			NOT NULL,
				LsatSystemSeek			datetime		NULL,
				LastSystemScan			datetime		NULL,
				LastSystemLookup		datetime		NULL,
				LastSystemUpdate		datetime		NULL,
				PRIMARY KEY CLUSTERED
					(IndexUsageItemId)
					WITH FILLFACTOR = 100);
					
		CREATE NONCLUSTERED INDEX DBAScan_IndexUsageItemIDX1
			ON dbo.DBAScan_IndexUsageItem	
				(ScanSessionId)
					INCLUDE(ObjectName, IndexName)
				WITH FILLFACTOR = 100;					
		END TRY
					
	BEGIN CATCH
		INSERT	#Error (ObjectName, ErrorMessage)
		VALUES(N'DBAScan_IndexUsageItem', ERROR_MESSAGE());
	END CATCH
	IF EXISTS(SELECT 1 FROM #Error) GOTO Failed;
END;

			
			
			
			
			
			
			
			
			
			
			
------------------------------
GOTO Done;
Failed:
	PRINT	'Script Failed.';
	SELECT	"ObjectName" = N'"' + REPLACE(TableName, N'"', N'""') + N'"',
			"ErrorMessage" = N'"' + REPLACE(ErrorMessage, N'"', N'""') + N'"'
	FROM	#Error
	ORDER BY OccurenceId;

	TRUNCATE TABLE #Error;
		
	IF @@TRANCOUNT > 0
		ROLLBACK TRAN;
Done:
GO
------------------------------
IF EXISTS(SELECT 1 FROM #Error)
BEGIN
	SELECT	"ObjectName" = N'"' + REPLACE(TableName, N'"', N'""') + N'"',
			"ErrorMessage" = N'"' + REPLACE(ErrorMessage, N'"', N'""') + N'"'
	FROM	#Error
	ORDER BY OccurenceId;

	TRUNCATE TABLE #Error;
END
	
IF @@TRANCOUNT > 0
	ROLLBACK TRAN;
GO
	
