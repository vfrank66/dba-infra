USE DBADefault;
IF OBJECT_ID('dbo.f_DigitsOnly') IS NOT NULL DROP FUNCTION dbo.f_DigitsOnly;
GO
CREATE FUNCTION dbo.f_DigitsOnly
(@Source varchar(8000))
RETURNS varchar(8000)
AS
BEGIN;
	RETURN(REPLACE((SELECT	',' + A2.SrceChar
					FROM	(SELECT	t.Numb,
									SrceChar = SUBSTRING(A1.Srce, t.Numb, 1)			
							FROM	(SELECT	Srce = @Source) A1
									INNER JOIN
										(SELECT TOP (LEN(@Source))
												Numb = ROW_NUMBER() OVER(ORDER BY "object_id", column_id)
									FROM	sys.columns) t
										ON SUBSTRING(A1.Srce, t.Numb, 1) IN 
												('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) A2
											ORDER BY A2.Numb
					FOR XML PATH(''), 
					  TYPE).value('(./text())[1]', 'varchar(max)'), 
					',', 
					SPACE(0)));
END;
			