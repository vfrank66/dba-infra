SET NOCOUNT ON;SET ROWCOUNT 0;SET QUOTED_IDENTIFIER ON;SET CONCAT_NULL_YIELDS_NULL ON;SET XACT_ABORT ON;SET ARITHABORT ON;
SET ANSI_WARNINGS ON;SET ANSI_PADDING ON;SET ANSI_NULLS ON;
PRINT	'Test CSV Formatting Functions'
PRINT	'DB: ' + DB_NAME();
PRINT	'Run: ' + CONVERT(char(23), GETDATE(), 121);
PRINT	SPACE(0);

PRINT	'f_CSVText';
SELECT	InputCol = A1.TextCol,
		OutputCol = DBADefault.dbo.f_CSVText(A1.TextCol)
FROM	(VALUES(CONVERT(varchar(60), NULL)), ('Qwerty'), ('Qwer"ty'), ('The answer is "No".'), ('The answer, my friends, is blowin'' in the wind.')) A1(TextCol);
PRINT	SPACE(0);
GO
PRINT	'f_CSVDate';
SELECT	InputCol = CONVERT(char(23), A1.DateCol, 121),
		OutptCol = DBADefault.dbo.f_CSVDate(A1.DateCol)
FROM	(VALUES(CONVERT(datetime, NULL)), (CONVERT(datetime, '1900-01-01 12:13:00 AM')), (CONVERT(datetime, '1944-10-14 5:36 AM')), (CONVERT(datetime, '2014-10-16'))) A1(DateCol);
PRINT	SPACE(0);
GO
PRINT	'f_CSVDateTime';
SELECT	InputCol = CONVERT(char(23), A1.DateCol, 121),
		OutptCol = DBADefault.dbo.f_CSVDateTime(A1.DateCol)
FROM	(VALUES(CONVERT(datetime, NULL)), (CONVERT(datetime, '1900-01-01 12:13:00 AM')), (CONVERT(datetime, '1944-10-14 5:36 AM')), (CONVERT(datetime, '2014-10-16'))) A1(DateCol);
PRINT	SPACE(0);
GO
PRINT	'f_CSVMoney';
SELECT	InputCol = CONVERT(varchar(40), CONVERT(decimal(38, 4), A1.MoneyCol)),
		OutputCol = DBADefault.dbo.f_CSVMoney(A1.MoneyCol)
FROM	(VALUES(CONVERT(money, NULL)), (CONVERT(money, 100.9899)), (CONVERT(money, 100)), (CONVERT(money, 100.00)), (CONVERT(money, 100.10)), (CONVERT(money, 100.11))) A1(MoneyCol)
PRINT	SPACE(0);
GO
PRINT	'f_CSVFY'
SELECT	InputCol = CONVERT(varchar(10), A1.FYCol),
		OutputCol = DBADefault.dbo.f_CSVFY(A1.FYCol)
FROM	(VALUES(CONVERT(int, NULL)), (CONVERT(int, 20122013)), (CONVERT(int, 12345)), (CONVERT(int, 1)), (CONVERT(int, 20132014))) A1(FYCol);
PRINT	SPACE(0);
GO
		
