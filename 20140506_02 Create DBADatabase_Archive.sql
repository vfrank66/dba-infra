SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET CONCAT_NULL_YIELDS_NULL ON;
		
Use DBAArchives;
		
IF OBJECT_ID('dbo.DBADatabase_Archive') IS NULL
BEGIN;
	CREATE TABLE  dbo.DBADatabase_Archive
		(RowSeq				int					NOT NULL IDENTITY(1, 1),
		DBASessionId		int					NOT NULL,
		ServerName			nvarchar(128)		NOT NULL,
		DBName				nvarchar(128)		NOT NULL,
		FileType			nvarchar(60)		NOT NULL,
		PhysicalName		nvarchar(260)		NOT NULL,
		SourceDB			nvarchar(128)		NULL,
		CreationDateTime	datetime			NOT NULL,
		RestoreDateTime		datetime			NULL,			
		RecoveryModel		nvarchar(60)		NOT NULL,
		DBState				nvarchar(60)		NOT NULL,
		DataPages			bigint				NOT NULL,
		LogPages			bigint				NOT NULL,
		FullTextPages		bigint				NOT NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100,
		UNIQUE NONCLUSTERED
			(DBName, ServerName, DBASessionId, PhysicalName)
			WITH FILLFACTOR = 100);
END;
	