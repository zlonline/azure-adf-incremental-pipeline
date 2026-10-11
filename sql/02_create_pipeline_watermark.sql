USE [Datatest]
GO

/****** Object:  Table [dbo].[pipeline_watermark]    Script Date: 10/11/2026 12:52:38 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pipeline_watermark]') AND type in (N'U'))
DROP TABLE [dbo].[pipeline_watermark]
GO

/****** Object:  Table [dbo].[pipeline_watermark]    Script Date: 10/11/2026 12:52:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[pipeline_watermark](
	[pipeline_name] [varchar](100) NOT NULL,
	[watermark_value] [datetime2](7) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[pipeline_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO