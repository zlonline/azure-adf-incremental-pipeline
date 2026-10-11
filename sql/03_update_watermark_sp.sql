USE [Datatest]
GO

/****** Object:  StoredProcedure [dbo].[usp_update_pipeline_watermark]    Script Date: 10/11/2026 12:54:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER   PROCEDURE [dbo].[usp_update_pipeline_watermark]
    @pipeline_name VARCHAR(100),
    @new_watermark DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.pipeline_watermark
    SET watermark_value = @new_watermark
    WHERE pipeline_name = @pipeline_name;
END;
GO

