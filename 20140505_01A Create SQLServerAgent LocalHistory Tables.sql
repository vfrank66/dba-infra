USE DBADefault
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET ANSI_WARNINGS ON;SET CONCAT_NULL_YIELDS_NULL ON;
GO
DECLARE	@Error			int,
		@ErrMsg			nvarchar(2048);

DECLARE	@ServerName		nvarchar(128);

SET @ServerName = CONVERT(varchar(128), ISNULL(SERVERPROPERTY('InstanceName'), SERVERPROPERTY('MachineName')));

IF OBJECT_ID('dbo.DBASQLServer_LocalHistory_Job') IS NULL
BEGIN;
	BEGIN TRY
		CREATE TABLE dbo.DBASQLServer_LocalHistory_Job
				(RowSeq				int						NOT NULL IDENTITY(1, 1),
				job_id				uniqueidentifier		NOT NULL,
				"name"				nvarchar(128)			NOT NULL
				PRIMARY KEY CLUSTERED
					(RowSeq)
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBASQLServer_LocalHistory_Job - Create: ' + ERROR_MESSAGE();
	END CATCH
END;

IF @Error <> 0
	GOTO Failed;
	
IF OBJECT_ID('dbo.DBASQLServer_LocalHistory_Step') IS NULL
BEGIN;
	BEGIN TRY
		CREATE TABLE dbo.DBASQLServer_LocalHistory_Step
				(RowSeq				int						NOT NULL IDENTITY(1, 1),
				job_id				nvarchar(128)			NOT NULL,
				step_id				int						NOT NULL,
				step_name			nvarchar(128)			NOT NULL,
				sql_message_id		int						NOT NULL,
				sql_serverity		int						NOT NULL,
				"message"			nvarchar(4000)			NULL,
				run_status			int						NOT NULL,
				run_date			int						NOT NULL,
				run_time			int						NOT NULL,
				run_duration		int						NOT NULL
				PRIMARY KEY CLUSTERED
					(RowSeq)
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBASQLServer_LocalHistory_Step - Create: ' + ERROR_MESSAGE();
	END CATCH
END;

IF @Error <> 0
	GOTO Failed;
	
IF @ServerName <> 'localhost'
	GOTO Done;
	
IF OBJECT_ID('DBASQLServer_Archive_Job') IS NULL
BEGIN;
	BEGIN TRY
		CREATE TABLE DBASQLServer_Archive_Job
				
