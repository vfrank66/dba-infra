USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET ANSI_NULLS ON;SET CONCAT_NULL_YIELDS_NULL ON;
GO
DECLARE	@Error	int,
		@ErrMsg	nvarchar(2048);
IF SERVERPROPERTY('ServerName') <> 'localhost'
BEGIN;
	PRINT	'This MUST run against localhost!';
	GOTO Failed;
END;		

IF OBJECT_ID('dbo.DBASQLServer') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'DBASQLServer - Create';
		CREATE TABLE dbo.DBASQLServer
				(SQLServerId			int						NOT NULL IDENTITY(1, 1),
				SQLServerName			nvarchar(128)			NOT NULL,
				InstanceName			nvarchar(128)			NULL,
				HostServerName			nvarchar(128)			NULL,
				MonitorSQLServerAgent	bit						NOT NULL DEFAULT(0),
				DateTimeCreated			datetime				NOT NULL DEFAULT(GETDATE())
				PRIMARY KEY CLUSTERED
					(SQLServerId)
					WITH FILLFACTOR = 100,
				UNIQUE NONCLUSTERED
					(SQLServerName, InstanceName)
					WITH FILLFACTOR = 100);
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBASQLServer - Create: ' + ERROR_MESSAGE()l;
	END CATCH
	IF @Error <> 0
		GOTO Failed;
END;










------------------------------
GOTO Done;
Failed:
	IF @Error <> 0
		PRINT	'Error: ' + @Error;
		
	IF LEN(@ErrMsg) > 0
		PRINT	N'Message: ' + @ErrMsg;
		
	RAISERROR('Script failed!';, 18, 1);
Done:
GO
					