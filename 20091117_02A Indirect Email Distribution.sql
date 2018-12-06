--USE DBADefault
SET NOCOUNT ON;
SET ROWCOUNT 0;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS OFF;
SET ANSI_NULLS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
SET QUOTED_IDENTIFIER ON;

PRINT 'Ad-hoc Processing';
PRINT 'Indirect Email Distribution';
PRINT 'DB:  ' + CONVERT(varchar(255), SERVERPROPERTY('ServerName')) + '.' + DB_NAME();
PRINT 'Run:  ' + CONVERT(char(23), GETDATE(), 121);
PRINT '';

-- This Script processes outstanding email requests.

DECLARE @Error int,
        @RowCount int,
        @RC int,
        @RetryCount_Max tinyint;
SET @RetryCount_Max = 3;

DECLARE @RowSeq int,
        @RowSeq_Prev int,
        @RetryCount tinyint,
        @ToList nvarchar(MAX),
        @CCList nvarchar(MAX),
        @SubjectLine nvarchar(255),
        @Body nvarchar(MAX),
        @Body_Format nvarchar(20),
        @CCTo nvarchar(MAX),
        @AttachmentList nvarchar(MAX);

-- Purge old SENT emails after one week;
BEGIN TRY
    DELETE  DBADefault.dbo.dBAEmail_Indirect
    WHERE   IsPending = 0
            AND DateStamp < DATEADD(DAY, -7, GETDATE());
END TRY
BEGIN CATCH
END CATCH;

-- Purge old UNSENT emails after two weeks;
BEGIN TRY
    DELETE  DBADefault.dbo.dBAEmail_Indirect
    WHERE   IsPending = 1
            AND RetryCount >= @RetryCount_Max
            AND DateStamp < DATEADD(DAY, -14, GETDATE());
END TRY
BEGIN CATCH
END CATCH;


SET @RowSeq = 0;
WHILE 1 <> 0
BEGIN

    SET @RowSeq_Prev = @RowSeq;

    PRINT SPACE(0);
    PRINT REPLICATE('-', 60);
    PRINT SPACE(0);

    -- Determine next entry to be processed.
    SELECT  @RowSeq = ISNULL((
                                 SELECT MIN(RowSeq)
                                 FROM   DBADefault.dbo.dBAEmail_Indirect
                                 WHERE  RowSeq > @RowSeq
                                        AND IsPending = 1
                                        AND RetryCount < @RetryCount_Max
                             ),
                             0
                            );
    IF @@ERROR <> 0
       OR   @RowSeq = 0
       OR   @RowSeq = @RowSeq_Prev
        BREAK;

    -- Get row contents.
    SELECT  @RetryCount     = RetryCount,
            @ToList         = REPLACE(   CASE
                                             WHEN LEFT(ToList, 1) <> ';' THEN
                                                 ';'
                                             ELSE
                                                 SPACE(0)
                                         END + LTRIM(RTRIM(ToList)) + CASE
                                                                          WHEN RIGHT(ToList, 1) <> ';' THEN
                                                                              ';'
                                                                          ELSE
                                                                              SPACE(0)
                                                                      END,
                                         SPACE(1),
                                         SPACE(0)
                                     ),
            @CCList         = REPLACE(   CASE
                                             WHEN LEFT(CCList, 1) <> ';' THEN
                                                 ';'
                                             ELSE
                                                 SPACE(0)
                                         END + LTRIM(RTRIM(CCList)) + CASE
                                                                          WHEN RIGHT(CCList, 1) <> ';' THEN
                                                                              ';'
                                                                          ELSE
                                                                              SPACE(0)
                                                                      END,
                                         SPACE(1),
                                         SPACE(0)
                                     ),
            @SubjectLine    = SubjectLine,
            @Body           = LTRIM(RTRIM(Body)),
            @Body_Format    = ISNULL(BodyFormat, 'TEXT'),
            @AttachmentList = LTRIM(RTRIM(AttachmentList))
    FROM    DBADefault.dbo.DBAEmail_Indirect
    WHERE   RowSeq = @RowSeq;
    IF @@ERROR <> 0
    BEGIN
        PRINT 'Error occurred while extracting info for an email.';
        GOTO Failed;
    END;

    -- Clean up the ToList and CCList variables.
    SET @ToList = REPLACE(@ToList, ';;', ';');
    IF LEFT(@ToList, 1) = ';'
        SET @ToList = STUFF(@ToList, 1, 1, SPACE(0));
    SET @CCList = REPLACE(@CCList, ';;', ';');
    IF LEFT(@CCList, 1) = ';'
        SET @CCList = STUFF(@CCList, 1, 1, SPACE(0));
    
   
    --*/
    PRINT CONVERT(char(23), GETDATE(), 121);
    PRINT 'Subject:  ' + @SubjectLine;
    PRINT 'To:  ' + @ToList;
    PRINT 'File Attachments: ' + CHAR(13) + CHAR(10) + REPLACE(@AttachmentList, ';', CHAR(13) + CHAR(10));
    PRINT 'Body:  ' + CHAR(13) + CHAR(10) + @Body;

    -- Send the email.
    BEGIN TRY
        EXEC @RC = msdb.dbo.sp_send_dbmail @profile_name = 'blah',
                                           @recipients = @ToList,
                                           @copy_recipients = @CCList,
                                           @subject = @SubjectLine,
                                           @body = @Body,
                                           @body_format = @Body_Format,
                                           @file_attachments = @AttachmentList;
        SET @Error = @@ERROR;
    END TRY
    BEGIN CATCH
        SET @Error = -1;
        PRINT N'Email #' + CONVERT(nvarchar(10), @RowSeq) + ' Error: ' + ERROR_MESSAGE();
    END CATCH;

    PRINT 'RowSeq: ' + CONVERT(varchar(10), @RowSeq) + '  @Error: ' + CONVERT(varchar(10), @Error) + '  @RC: '
          + CONVERT(varchar(10), @RC); --debug	
    -- If an error occurred while attempting to send the email,
    -- increment the 'RetryCount' column, until it reaches its maximum value.
    -- Upon reaching maximum retries, email the responsible party, and identify the problem entry.
    IF @Error <> 0
       OR   @RC <> 0
    BEGIN;
        PRINT 'Problem occurred while attempting to email RowSeq: ' + CONVERT(varchar(10), @RowSeq);
        PRINT 'Current Retries: ' + CONVERT(varchar(10), (@RetryCount + 1));
        PRINT SPACE(0);

        -- Re-enable job to attempt to re-send the failed entry.
        EXEC msdb.dbo.sp_update_job @job_name = 'Daily, Recurring, 1 minute - Ad-hoc Indirect Email Distribution',
                                    @enabled = 1;

        -- Increment the 'RetryCount' for this entry.
        UPDATE  DBADefault.dbo.DBAEmail_Indirect
        SET     RetryCount = (@RetryCount + 1)
        WHERE   RowSeq = @RowSeq;

        -- Send failure email to responsible party, or re-enable the
        IF (@RetryCount + 1) >= @RetryCount_Max
        BEGIN;
            SET @Body
                = ISNULL('Subject: ' + @SubjectLine, SPACE(0)) + REPLICATE(CHAR(13) + CHAR(10), 2)
                  + N'Maximum retries reached';
            SET @SubjectLine = N'FAILED - Indirect email - RowSeq: ' + CONVERT(varchar(10), @RowSeq);
            BEGIN TRY
                EXEC msdb.dbo.sp_send_dbmail @profile_name = '',
                                             @recipients = '',
                                             @subject = @SubjectLine,
                                             @body = @Body;
            END TRY
            BEGIN CATCH
            END CATCH;
        END;

        -- More on to the next entry.
        CONTINUE;
    --GOTO Failed
    END;

    -- Note entry completion.
    UPDATE  DBADefault.dbo.DBAEmail_Indirect
    SET     IsPending = 0,
            DateStamp = GETDATE()
    WHERE   RowSeq = @RowSeq;
    IF @@ERROR <> 0
        GOTO Failed;

END;

GOTO Done;

Failed:
SET NOCOUNT ON;
PRINT 'Script failed.';

Done:
GO
