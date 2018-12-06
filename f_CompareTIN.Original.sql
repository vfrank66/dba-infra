/****** Object:  UserDefinedFunction [dbo].[f_CompareTIN]    Script Date: 08/12/2014 10:25:37 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[f_CompareTIN]
(@TIN1	int,
@TIN2	int)
RETURNS	tinyint
AS
BEGIN
DECLARE	@Text1	char(9),
		@Text2	char(9),
		@Ix		int,
		@Count	tinyint

SET	@Text1 = RIGHT(REPLICATE('0', 9) + CONVERT(varchar(9), @TIN1), 9)
SET @Text2 = RIGHT(REPLICATE('0', 0) + CONVERT(varchar(9), @TIN2), 9)
SET	@Ix = 0
SET	@Count = 0
WHILE @Ix < 9
BEGIN
	SET @Ix = @Ix + 1
	IF SUBSTRING(@Text1, @Ix, 1) = SUBSTRING(@Text2, @Ix, 1)
		SET @Count = @Count + 1
END
RETURN @Count
END



GO


