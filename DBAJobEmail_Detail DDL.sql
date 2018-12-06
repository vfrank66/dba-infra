-- Run on localhost server only.

USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET ANSI_WARNINGS ON;

CREATE TABLE dbo.DBAJobEmail_Detail
		(RowSeq					int							NOT NULL IDENTITY(1, 1),
		JobId					uniqueidentifier			NOT NULL,
		RunDate					char(8)						NOT NULL,
		RunTime					varchar(8)					NOT NULL,
		ContainsSensitiveInfo	bit							NOT NULL,
		ServerName				nvarchar(128)				NOT NULL,
		DBName					nvarchar(128)				NOT NULL,
		FileLegend				varchar(1000)				NOT NULL,
		"FileName"				varchar(1000)				NOT NULL
		PRIMARY KEY CLUSTERED
			(RowSeq)
			WITH FILLFACTOR = 100);
		
CREATE INDEX DBAJobEmail_DetailIDX1
	ON dbo.DBAJobEmail_Detail
		(JobId, RunDate, RunTime);
		
GO		
CREATE PROCEDURE DBADefault.dbo.s_DBAJobEmail_DetailINS
JobId					uniqueidentifier,
RunDate					char(8),
RunTime					varchar(8),
ContainsSensitiveInfo	bit,
ServerName				nvarchar(128),
DBName					nvarchar(128),
FileLegend				varchar(1000),
"FileName"				varchar(1000)

AS

INSERT	"localhost".DBADefault.dbo.DBAJobEmail_Detail
		(JobId, RunDate, RunTime,
		ContainsSensitiveInfo	bit							NOT NULL,
		ServerName				nvarchar(128)				NOT NULL,
		DBName					nvarchar(128)				NOT NULL,
		FileLegend				varchar(1000)				NOT NULL,
		"FileName"				varchar(1000)				NOT NULL
			
		