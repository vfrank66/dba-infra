USE DBADefault;
GO
CREATE PROC dbo.s_DBAJobDEL
@JobID	uniqueidentifier
AS

EXEC msdb.dbo.sp_delete_job @job_id = JobID;