-- Note: Before opening, change output to 'comma-delimited'; store results with .CSV file extension.
SET NOCOUNT ON;SeT ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET ANSI_WARNINGS ON;SET ANSI_NULLS ON;SET ANSI_PADDING ON;
SET XACT_ABORT ON;SET CONCAT_NULL_YIELDS_NULL ON;
IF OBJECT_ID('tempdb..#Server') IS NOT NULL DROP TABLE #Server;
IF OBJECT_ID('tempdb..#Job') IS NOT NULL DROP TABLE #Job;
IF OBJECT_ID('tempdb..#JobDetail') IS NOT NULL DROP TABLE #JobDetail;
GO

DECLARE	@DBName			nvarchar(128) = DB_NAME(),
		@RunDateTime	datetime = GETDATE(),
		@User			varchar(60) = CONVERT(varchar(60), SUSER_SNAME()),
		@UpdatedBy		smallint = 11,
		@IssueNumber	int = NULL,
		@DBASessionId	int;

DECLARE	@Heading1	varchar(100) = '"DBA Imaging Maintenance"',
		@Title		varchar(100) = 'Report and Remove Failed DBADBA SQL Server Agent Jobs',
		--@Heading3	varchar(100) = '"Issue Number: ' + CONVERT(varchar(10), @IssueNumber) + '"',
		@Heading3	varchar(100) = '"DB: ' + @DBName + '"',
		@Heading4	varchar(100) = '"Run: ' + CONVERT(char(23), @RunDateTime, 121) + '"',
		--@Heading5	varchar(100) = '"User: ' + @User + '"',
		@Heading5	varchar(100) = SPACE(0);

-- Display headings in ProcessingLog output.
PRINT	@Heading1;
PRINT	@Title;
--PRINT	@Heading3;
PRINT	@Heading3;
PRINT	@Heading4;
PRINT	@Heading5;
		