USE DBADefault;
SET ANSI_NULLS ON;SET ANSI_PADDING ON;SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('dbo.s_DBAEmailINS') IS NOT NULL DROP PROCEDURE dbo.s_DBAEmailINS;
GO

CREATE SYNONYM dbo.DBAEmail FOR "localhost".DBADefault.dbo.DBAEmail_Indirect;
GO

-- This table temporarily stores email information, until it can be queued in the primary DBA email table.
CREATE TABLE dbo.DBAEmail_Temp
	(RowSeq				int						NOT NULL IDENTITY(1, 1),
	ToList				varchar(4000)			NOT NULL,
	CCList				varchar(4000)			NOT NULL,
	SubjectLine			varchar(4000)			NOT NULL,
	Body				varchar(max)			NOT NULL DEFAULT(SPACE(0)),
	AttachmentList		varchar(max)			NOT NULL DEFAULT(SPACE(0)),
	"Priority"			varchar(6)				NOT NULL DEFAULT('Normal'),
	DateStamp			datetime				NOT NULL DEFAULT (GETDATE())
 CONSTRAINT DBAEmail_TempPK 
	PRIMARY KEY CLUSTERED 
		(RowSeq)
		WITH FILLFACTOR = 100);

GO

-- This INSERT trigger is invoked when an entry is added to the DBAEmail_Temp table.
-- It enables a SQL Server Agent Job, which will attempt to queue the emails in the temporary table.
CREATE TRIGGER dbo.tr_DBAEmail_TempINSERT
	ON dbo.DBAEmail_Temp
	FOR INSERT
AS
	BEGIN TRY
		EXEC msdb.dbo.sp_update_job 
				@job_name = 'Daily, Recurring, 1 minute - Attempt to Queue DBA Email - Auto',
				@enabled = 1;
	END TRY
	BEGIN CATCH
		RAISERROR('Problem encountered while attempting to start the background email queuing process.', 18, 1);
	END CATCH			
GO

-- This SP is the primary contact point with external logic.
-- It will queue the supplied email information in the primary DBA email table, if possible.
-- In event of a failure, it will store the information in a temporary holding area, for later attempts at queuing.
CREATE PROCEDURE dbo.s_DBAEmailINS
@ToList					varchar(4000),				-- Required
@CCList					varchar(4000)= NULL,
@Subject				varchar(4000),				-- Required
@Body					varchar(max) = NULL,
@AttachmentList			varchar(max) = NULL,
@Priority				varchar(6) = 'Normal'

AS
	-- This Script queues the email information in the combined DBA email table, originally located on localhost.
	-- The original logic has been revised to use a Synonym, to permit flexible location of the DBA email table.
	
	-- Any error will be returned to the caller.
	-- No Return Code is provided.
	
	SET NOCOUNT ON;	-- Disable return of interim rowcounts during processing.
	SET QUOTED_IDENTIFIER ON;

	DECLARE	@Error			int;
	SET @Error = 0

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

	-- Store email info in temporary area for later queuing attempt.
	BEGIN TRY
		INSERT	DBAEmail_Temp
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

		SET @Error = 0;		-- Clear the error, since the email was successfully stored for later queuing.
	END TRY
	BEGIN CATCH
		-- Allow the original error to stand.
	END CATCH

	RETURN(@Error);

GO

CREATE PROCEDURE s_ReQueue_DBAEmail
AS
-- This stored Procedure attempts to queue email entries stored in a temporary area.
-- As each email is queued, it is removed from the temporary area.

SET NOCOUNT ON;	-- Disable interim communication with caller.

DECLARE	@Error			int,
		@RowSeq			int,
		@RowSeq_Prev	int;
SELECT	@Error = 0,
		@RowSeq = 0,
		@RowSeq_Prev = 0;

WHILE 1 <> 0
BEGIN;
	-- Save prior RowSeq value.
	SELECT	@RowSeq_Prev = @RowSeq

	-- Determine next RowSeq value to be processed.
	BEGIN TRY
		SET	@RowSeq = 
				ISNULL((SELECT	TOP (1)
								RowSeq
						FROM	dbo.DBAEmail_Temp
						WHERE	RowSeq > @RowSeq_Prev
						ORDER BY RowSeq),
						0);
	END TRY
	BEGIN CATCH
		SET @Error = ERROR_NUMBER();
	END CATCH

	IF @Error <> 0
	OR @RowSeq = @RowSeq_Prev
	OR @RowSeq IS NULL
		BREAK;

	-- Attempt to queue the email. If successful, remove it from the temporary table.
	BEGIN TRY
		INSERT dbo.DBAEmail		-- Synomym for DBA email table
				(ToList, CCList, SubjectLine, Body, AttachmentList, "Priority")
		SELECT	 ToList, CCList, SubjectLine, Body, AttachmentList, "Priority"
		FROM	dbo.DBAEmail_Temp
		WHERE	RowSeq = @RowSeq;

		DELETE	dbo.DBAEmail_Temp
		WHERE	RowSeq = @RowSeq;
	END TRY
	BEGIN CATCH
		SET @Error = ERROR_NUMBER();
	END CATCH
	
	IF @Error <> 0
		BREAK;
END;
