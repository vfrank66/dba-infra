USE DBADefault;
GO

CREATE PROC dbo.s_DBAFile_BulkInsert
@FileId			int,
@FilePath		varchar(255),
@FileName		varchar(255)

AS

SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
SET XACT_ABORT ON;SET ARITHABORT ON;

DECLARE	@SQL		nvarchar(max);

CREATE TABLE #Import 
	(RowSeq			int		NOT NULL IDENTITY(1, 1), 
	Contents		varchar(8000) NOT NULL 
	PRIMARY KEY CLUSTERED (RowSeq) 
	WITH FILLFACTOR = 100); 
	
-- Build the BULK INSERT statement.	
SET @SQL = 
N'BULK INSERT #Import FROM "' + @FilePath + '\' + @FileName + N'" ' +
N'WITH (FORMATFILE= ''\\localhost\Import2000\Ad-hoc\Fmt\Generic.RowSeq.Contents.fmt.txt'');';

EXEC(@SQL);

INSERT dbo.DBAFileRow 
		(FileId, Contents) 
SELECT FileId = @FileId, Contents 
FROM #Import 
ORDER BY RowSeq;
GO


