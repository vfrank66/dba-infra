USE DBADefault
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[f_CompareTIN]
(@TIN1	int,
@TIN2	int)
RETURNS	tinyint
WITH SCHEMABINDING
AS
/* This function compares the 9-digit character representation of two TIN values, and returns a count of the number of matching digits.

ASSUMPTION: The passed values contain no more than nine (9) significant digits.

NULL values are anticipated, and are initially converted to an empty string, before prepending zeroes to fill out the required 9-digit length.
*/

BEGIN;
	RETURN(
		(SELECT	SUM(CASE WHEN SUBSTRING(A2.TIN1, B1.Digit, 1) = SUBSTRING(A2.TIN2, B1.Digit, 1) THEN 1 ELSE 0 END)
		FROM	(SELECT	TIN1 = REPLICATE('0', (9 - DATALENGTH(A1.TIN1))) + TIN1,
						TIN2 = REPLICATE('0', (9 - DATALENGTH(A1.TIN2))) + TIN2
				FROM	(SELECT TIN1 = ISNULL(CONVERT(varchar(9), @TIN1), SPACE(0)),
								TIN2 = ISNULL(CONVERT(varchar(9), @TIN2), SPACE(0))) A1) A2
				CROSS JOIN (VALUES(1), (2), (3), (4), (5), (6), (7), (8), (9)) B1 (Digit)))
END;
GO			
