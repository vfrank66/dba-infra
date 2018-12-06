USE DBADefault;
SET NOCOUNT ON;SET ROWCOUNT 0;SET ANSI_PADDING ON;SET ANSI_WARNINGS ON;SET ANSI_NULLS ON;SET CONCAT_NULL_YIELDS_NULL ON;
GO
DECLARE	@Error	int,
		@ErrMsg	nvarchar(2048);
		
IF OBJECT_ID('dbo.DBAJobHistory_Job') IS  NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAJobHistory_Job';
		CREATE TABLE dbo.DBAJobHistory_Job
				(RowSeq				int						NOT NULL IDENTITY(1, 1),
				JobId				uniqueidentifier		NOT NULL,
				JobName				nvarchar(128)			NOT NULL,
				CreatedDateTime		datetime				NOT NULL DEFAULT(GETDATE()),
				DBASessionId		int						NOT NULL DEFAULT(0)
				PRIMARY KEY CLUSTERED
					(RowSeq)
					WITH FILLFACTOR = 100);
					
		CREATE NONCLUSTERED INDEX DBAJobHistory_JobNDX1
			ON dbo.DBAJobHistory_Job
					(JobId)
					WITH FILLFACTOR = 100;		

		CREATE NONCLUSTERED INDEX DBAJobHistory_JobNDX2
			ON dbo.DBAJobHistory_Job
					(DBASessionId)
					WITH FILLFACTOR = 100;		

	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBAJobHistory_Job - Create: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		GOTO Failed;
END;

IF OBJECT_ID('dbo.DBAJobHistory_Step') IS NULL
BEGIN;
	BEGIN TRY
		PRINT	'Create DBAJobHistory_Step';
		CREATE TABLE dbo.DBAJobHistory_Step
				(RowSeq				int						NOT NULL IDENTITY(1, 1),
				JobRowSeq			int						NOT NULL,
				JobId				uniqueidentifier		NOT NULL,
				StepId				int						NOT NULL,
				StepName			nvarchar(128)			NOT NULL,
				SQLMessageId		int						NULL,
				SQLSeverity			int						NOT NULL,
				"Message"			nvarchar(4000)			NOT NULL,
				RunStatus			int						NOT NULL,
				RunDate				int						NOT NULL,	-- CCYYMMDD format
				RunTime				int						NOT NULL,	-- HHMMSS format
				RunDuration			int						NOT NULL,	-- HHMMSS format
				RetriesAttempted	int						NOT NULL,
				CreatedDateTime		datetime				NOT NULL DEFAULT(GETDATE()),
				DBASessionId		int						NOT NULL DEFAULT(0)
				PRIMARY KEY CLUSTERED
					(RowSeq)
					WITH FILLFACTOR = 100);

		CREATE NONCLUSTERED INDEX DBAJobHistory_StepNDX1
			ON dbo.DBAJobHistory_Step
					(DBASessionId)
					WITH FILLFACTOR = 100;		
	END TRY
	BEGIN CATCH
		SELECT	@Error = ERROR_NUMBER(),
				@ErrMsg = N'DBAJobHistory_Step - Create: ' + ERROR_MESSAGE();
	END CATCH
	IF @Error <> 0
		GOTO Failed;
END;

------------------------------
GOTO Done;
Failed:
	IF @Error <> 0
		PRINT	'Error: ' + @Error;
		
	IF LEN(@ErrMsg) > 0
		PRINT	N'Message: ' + @ErrMsg;
		
	RAISERROR('Script failed!', 18, 1);
Done:
GO

USE msdb;
GO

CREATE TRIGGER dbo.TR_sysjobhistoryINS_After
	ON dbo.sysjobhistory
	FOR INSERT
AS
/* This Trigger captures SQL Server Agent Job Step information as it is logged into the sysjobhistory table.
*/

-- Store the JobId and JobName.
-- Note that there many be multiple entries for the JobId, if the JobName has changed.
BEGIN TRY
	INSERT	DBADefault.dbo.DBAJobHistory_Job
			(JobId,
			JobName)
	SELECT	JobId = sj.job_id,
			JobName = sj."name"
	FROM	INSERTED i
			INNER JOIN dbo.sysjobs sj
				ON sj.job_id = i.job_id
	WHERE	NOT EXISTS(
				SELECT	1
				FROM	DBADefault.dbo.DBAJobHistory_Job djhj
				WHERE	djhj.JobId = sj.job_id
				AND		djhj.JobName = sj."name");

	-- Store the Job Step history, with a link to the current job name entry in the DBAJobHistory_Job table.
	INSERT	DBADefault.dbo.DBAJobHistory_Step
			(JobRowSeq,
			JobId,
			StepId,
			StepName,
			SQLMessageId,
			"Message",
			RunStatus,
			RunDate,
			RunTime,
			RunDuration,
			RetriesAttempted)
	SELECT	JobRowSeq = djhj.RowSeq,
			JobId = i.job_id,
			StepId = i.step_id,
			StepName = i.step_name,
			SQLMessageId = i.sql_message_id,
			SQLSeverity = i.sql_severity,
			"Message" = i."message",
			RunStatus = i.run_status,
			RunDate = i.run_date,
			RunTime = i.run_time,
			RunDuration = i.run_duration
	FROM	INSERTED i
			INNER JOIN dbo.sysjobs sj
				ON sj.job_id = i.job_id
				INNER JOIN DBADefault.dbo.DBAJobHistory_Job djhj
					ON djhj.JobId = sj.job_id
					AND djhj.JobName = sj."name";					
END TRY
BEGIN CATCH
END CATCH
GO

		
	

