SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO

-- This SP is the primary contact point with external logic.
-- It will queue the supplied email information in the primary DBA email table, if possible.
-- In event of a failure, it will store the information in a temporary holding area, for later attempts at queuing.
ALTER PROCEDURE dbo.s_DBAEmailINS
@ToList					varchar(4000),				-- Required
@CCList					varchar(4000)= NULL,
@Subject				varchar(4000),				-- Required
@Body					varchar(max) = NULL,
@AttachmentList			varchar(max) = NULL,
@Priority				varchar(6) = 'Normal',
@BodyFormat				varchar(10) = 'TEXT'

AS
	-- This Script queues the email information in the combined DBA email table, originally located on localhost.
	-- The original logic has been revised to use a Synonym, to permit flexible location of the DBA email table.
	
	-- Any error will be returned to the caller.
	-- No Return Code is provided.
	
	SET NOCOUNT ON;	-- Disable return of interim rowcounts during processing.
	SET QUOTED_IDENTIFIER ON;

	DECLARE	@Error			int;
	SET @Error = 0

/* Disable, due to loss of linked server capability.
	-- Insert the new entry in the DBA email table.
	BEGIN TRY
		INSERT	dbo.DBAEmail		-- Synonym for actual table
				(ToList, 
				CCList, 
				SubjectLine, 
				Body, 
				AttachmentList, 
				"Priority")
		SELECT	ToList = @ToList,
				CCList =  ISNULL(@CCList, SPACE(0)), 
				SubjectLine = @Subject, 
				Body = ISNULL(@Body, SPACE(0)),
				AttachmentList = ISNULL(@AttachmentList, SPACE(0)), 
				"Priority" = @Priority;
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER();
	END CATCH
	IF @Error = 0
		RETURN(@Error);
*/
	-- Store email info in temporary area for later queuing attempt.
	BEGIN TRY
		INSERT	DBAEmail_Temp
				(ToList, 
				CCList, 
				SubjectLine, 
				Body, 
				AttachmentList, 
				"Priority",
				BodyFormat)
		SELECT	ToList = @ToList,
				CCList =  ISNULL(@CCList, SPACE(0)), 
				SubjectLine = @Subject, 
				Body = ISNULL(@Body, SPACE(0)),
				AttachmentList = ISNULL(@AttachmentList, SPACE(0)), 
				"Priority" = @Priority,
				BodyFormat = ISNULL(@BodyFormat, 'TEXT');

		SET @Error = 0;		-- Clear the error, since the email was successfully stored for later queuing.
	END TRY
	BEGIN CATCH
		SET @Error = ERROR_NUMBER();
	END CATCH

	RETURN(@Error);

GO

