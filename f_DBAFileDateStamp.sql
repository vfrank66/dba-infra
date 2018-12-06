USE DBADefault;
IF OBJECT_ID('dbo.f_DBAFileDateStamp') IS NOT NULL DROP FUNCTION dbo.f_DBAFileDateStamp;
GO
CREATE FUNCTION dbo.f_DBAFileDateStamp
(@DateTime datetime)
RETURNS char(15)
AS
BEGIN;
	-- Return CCYYMMDD.HHMMSS string from supplied date/time.
	RETURN(CONVERT(char(8), @DateTime, 112) + '.' +							-- CCYYMMDD
			REPLACE(CONVERT(varchar(8), @DateTime, 108), ':', SPACE(0)))	--HH:MM:SS
END;