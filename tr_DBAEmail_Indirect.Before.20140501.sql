/****** Object:  Trigger [dbo].[tr_DBAEmail_Indirect]    Script Date: 05/01/2014 14:38:52 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TRIGGER [dbo].[tr_DBAEmail_Indirect]
ON [dbo].[DBAEmail_Indirect]
FOR INSERT
AS

--EXEC msdb.dbo.sp_Start_Job @job_name = 'Ad-hoc Indirect Email Distribution'
EXEC msdb.dbo.sp_update_job 
		@job_name = 'Daily, Recurring, 1 minute - Ad-hoc Indirect Email Distribution',
		@enabled = 1


GO


