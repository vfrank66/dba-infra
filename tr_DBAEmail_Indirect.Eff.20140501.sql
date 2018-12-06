/****** Object:  Trigger [dbo].[tr_DBAEmail_Indirect]    Script Date: 05/01/2014 14:31:58 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER TRIGGER [dbo].[tr_DBAEmail_Indirect]
ON [dbo].[DBAEmail_Indirect]
FOR INSERT
AS
	-- Try to start the job; otherwise, enable it, so later emails can still be sent on a possible later run.
	BEGIN TRY
		EXEC msdb.dbo.sp_update_job 
				@job_name = 'Daily, Recurring, 1 minute - Ad-hoc Indirect Email Distribution',
				@enabled = 1;
	END TRY
	BEGIN CATCH
		RAISERROR('Problem encountered while attempting to start the background email process.', 18, 1);
	END CATCH			
	
	BEGIN TRY
		EXEC msdb.dbo.sp_start_job @job_name = 'Daily, Recurring, 1 minute - Ad-hoc Indirect Email Distribution'
	END TRY
	BEGIN CATCH
	END CATCH
GO


