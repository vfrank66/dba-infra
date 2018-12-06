USE DBADefault
GO
SET ANSI_PADDING ON;SET ANSI_NULLS ON;SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;SET XACT_ABORT OFF;
GO

CREATE FUNCTION dbo.f_CSVText
(@Text varchar(max))
RETURNS varchar(max)
AS
BEGIN;
	-- This function returns a quoted text string, with existing quotes doubled as an 'escape' mechanism.
	RETURN((SELECT DQuote + REPLACE(@Text, DQuote, DQuote + DQuote) + DQuote
			FROM	(SELECT	'"' AS DQuote) A1));
END;
GO

CREATE FUNCTION dbo.f_CSVDateTime
(@DateTime	datetime)
RETURNS varchar(26)
AS
BEGIN;
	-- This function returns a quoted date/time string.
	-- If the time is 'midnight', the time portion is discarded.
	-- Due to the way Excel current displays date/time information,
	-- if a time portion is included, IT is what will display in the grid view,
	-- with the date portion visible ONLY when clicking on a specific cell.
	-- To prevent this behavior, a single quote is pre-pended to the string,
	-- which forces it to be evaluated as text, rather than as a date.
	RETURN((SELECT	DQuote +
					CASE
					WHEN LEN("DateTime") > 10
					THEN SQuote
					ELSE SPACE(0)
					END +
					"DateTime" +
					DQuote
	FROM	(SELECT	'"' AS DQuote,
					'''' AS SQuote,
					CONVERT(varchar(26), @DateTime, 121) AS "DateTime") A1))
END
GO

CREATE FUNCTION dbo.f_CSVFY
(@FY int)
RETURNS varchar(11)
AS
BEGIN;
	-- This function returns an edited fiscal year value, enclosed in double quotes.
	RETURN((SELECT	DQuote + STUFF(CONVERT(char(8), @FY), 5, 0, Dash) + DQuote
			FROM	(SELECT	'"'  AS DQuote,
							'-' AS Dash) A1));
END;
GO							

CREATE FUNCTION dbo.CSVMoney
(@Money	money)
RETURNS varchar(40)
AS
BEGIN;
	-- This function returns a money amount, with TWO decimal positions (decimal(38, 2)), using rounding.
	RETURN(SELECT CONVERT(varchar(40), CONVERT(decimal(38, 2), @Money)));
END;
GO

