USE DBADefault;
GO

IF OBJECT_ID('dbo.f_CSVText') IS NOT NULL DROP FUNCTION dbo.f_CSVText;
IF OBJECT_ID('dbo.f_CSVTextMax') IS NOT NULL DROP FUNCTION dbo.f_CSVTextMax;
IF OBJECT_ID('dbo.f_CSVTextN') IS NOT NULL DROP FUNCTION dbo.f_CSVTextN;
IF OBJECT_ID('dbo.f_CSVTextNMax') IS NOT NULL DROP FUNCTION dbo.f_CSVTextNMax;
IF OBJECT_ID('dbo.f_CSVDate') IS NOT NULL DROP FUNCTION dbo.f_CSVDate;
IF OBJECT_ID('dbo.f_CSVNumber') IS NOT NULL DROP FUNCTION dbo.f_CSVNumber;
IF OBJECT_ID('dbo.f_CSVDateTime') IS NOT NULL DROP FUNCTION dbo.f_CSVDateTime;
IF OBJECT_ID('dbo.f_CSVMoney') IS NOT NULL DROP FUNCTION dbo.f_CSVMoney;
IF OBJECT_ID('dbo.f_CSVFY') IS NOT NULL DROP FUNCTION dbo.f_CSVFY;
GO

CREATE FUNCTION dbo.f_CSVText
(@Text	varchar(8000))
RETURNS varchar(max)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed.
	SCHEMABINDING
AS
/* This function returns a text string properly formatted for inclusion in a comma-seperated file, line, row, or column.
	The string is limited to a maximum of 8000 characters.
   Pre-existing comma(s) cause the text string to be enclosed in leading and trailing double-quotes.
   Pre-existing double-quotes are "escaped" by repeating the double-quote character.
*/   
BEGIN;
	RETURN((SELECT	CASE
					WHEN CHARINDEX('"', @Text, 1) > 0
					THEN '"' + REPLACE(@Text, '"', '""') + '"'
					WHEN CHARINDEX(',', @Text, 1) > 0
					THEN '"' + @Text + '"'
					ELSE @Text
					END));
END;
GO

CREATE FUNCTION dbo.f_CSVTextMax
(@Text	varchar(max))
RETURNS varchar(max)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed.
	SCHEMABINDING
AS
/* This function returns a text string properly formatted for inclusion in a comma-seperated file, line, row, or column.
   The string may contain up to 2 billion characters.
   Pre-existing comma(s) cause the text string to be enclosed in leading and trailing double-quotes.
   Pre-existing double-quotes are "escaped" by repeating the double-quote character.
*/   
BEGIN;
	RETURN((SELECT	CASE
					WHEN CHARINDEX('"', @Text, 1) > 0
					THEN '"' + REPLACE(@Text, '"', '""') + '"'
					WHEN CHARINDEX(',', @Text, 1) > 0
					THEN '"' + @Text + '"'
					ELSE @Text
					END));
END;
GO

CREATE FUNCTION dbo.f_CSVTextN
(@Text	nvarchar(8000))
RETURNS varchar(max)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed.
	SCHEMABINDING
AS
/* This function returns a text string properly formatted for inclusion in a comma-seperated file, line, row, or column.
	The string is limited to a maximum of 8000 characters.
   Pre-existing comma(s) cause the text string to be enclosed in leading and trailing double-quotes.
   Pre-existing double-quotes are "escaped" by repeating the double-quote character.
*/   
BEGIN;
	RETURN((SELECT	CASE
					WHEN CHARINDEX('"', @Text, 1) > 0
					THEN '"' + REPLACE(@Text, '"', '""') + '"'
					WHEN CHARINDEX(',', @Text, 1) > 0
					THEN '"' + @Text + '"'
					ELSE @Text
					END));
END;
GO

CREATE FUNCTION dbo.f_CSVTextNMax
(@Text	nvarchar(max))
RETURNS nvarchar(max)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed.
	SCHEMABINDING
AS
/* This function returns a text string properly formatted for inclusion in a comma-seperated file, line, row, or column.
   The string may contain up to 2 billion characters.
   Pre-existing comma(s) cause the text string to be enclosed in leading and trailing double-quotes.
   Pre-existing double-quotes are "escaped" by repeating the double-quote character.
*/   
BEGIN;
	RETURN((SELECT	CASE
					WHEN CHARINDEX('"', @Text, 1) > 0
					THEN '"' + REPLACE(@Text, '"', '""') + '"'
					WHEN CHARINDEX(',', @Text, 1) > 0
					THEN '"' + @Text + '"'
					ELSE @Text
					END));
END;
GO


CREATE FUNCTION dbo.f_CSVDate
(@Date	date)
RETURNS char(10)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed
	SCHEMABINDING
AS
/* This function returns a text string containing a date, formatted AS CCYY-MM-DD.
   Please note that Excel will display this value as MM/DD/CCYY, for US-English.
   
   Please also note that, natively, Excel will display a date formatted as CCYY-MM-DD as MM/DD/CCYY in U.S. English.
*/   
BEGIN;
	RETURN((SELECT	CONVERT(char(10), @Date, 101)));
END;
GO


CREATE FUNCTION dbo.f_CSVDateTime
(@DateTime	datetime)
RETURNS varchar(24)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed
	SCHEMABINDING
AS
/* This function returns a text string containing a date and time (if other than midnight), formatted AS CCYY-MM-DD HH-MM-SS.TTT.
   Please note that Excel will display only the time portion of a date-time string, until the containing cell is selected.
   To prevent this mis-behavior, a leading single-quote character is added, to cause Excel to treat the value as a text string.
   Please also note that, natively, Excel will display a date formatted as CCYY-MM-DD as MM/DD/CCYY in U.S. English.
   
   If the time component is midnight (00:00:00.000), it is removed.
*/   

BEGIN;
	RETURN(SELECT	CASE
					WHEN CONVERT(datetime, CONVERT(date, @DateTime)) = @DateTime
					THEN CONVERT(char(10), @DateTime, 101)
					ELSE '''' + CONVERT(char(23), @DateTime, 121)
					END);
END;
GO

CREATE FUNCTION dbo.f_CSVNumber
(@Number		sql_variant = NULL,
@Decimals		tinyint = 0)
RETURNS varchar(40)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed
	SCHEMABINDING
/* This function returns a number with the specified number of decimal positions. Implicit rounding is used.
*/
AS
BEGIN;
	RETURN(SELECT	LTRIM(STR(CONVERT(float, @Number), 40, @Decimals)));
END;
GO

CREATE FUNCTION dbo.f_CSVMoney
(@Money	money)
RETURNS varchar(40)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed
	SCHEMABINDING
/* This function returns a text string containing a monetary value, with an optional decimal component consisting of TWO digits.
   Please note that Excel will natively not display trailing zero(es), and will not display a decimal point unless non-zero decimal characters exist.
   
   If the decimal component is zero, no decimal point or decimal characters will be returned.
   Trailing zero(s) to the right of the decimal point will not be returned.
*/   
AS
BEGIN;
	RETURN(SELECT	CONVERT(varchar(40), CONVERT(decimal(38, 2), @Money)));
END;
GO

CREATE FUNCTION dbo.f_CSVFY
(@FY		int)					
RETURNS varchar(9)
WITH RETURNS NULL ON NULL INPUT,		-- If the input is NULL, the function call will be bypassed
	SCHEMABINDING
/*	This function returns a formatted text string containing a fiscal year value, in the format CCYY-CCYY.
	If the input value is zero, or the character length is not eight (8), NULL will be returned.
*/   
AS
BEGIN;
	RETURN((SELECT	CASE
					WHEN @FY = 0
					OR   LEN(CONVERT(varchar(8), @FY)) <> 8
					THEN CONVERT(varchar(9), NULL)
					ELSE STUFF(CONVERT(char(8), @FY), 5, 0, '-')
				    END));
END;
GO

					