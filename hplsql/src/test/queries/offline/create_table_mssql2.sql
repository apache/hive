USE [mic.gr]
GO

/****** Object:  Table [dbo].[downloads]    Script Date: 03/11/2016 11:46:35 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[downloads](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[fileName] [char](255) NOT NULL,
	[fileType] [char](10) NULL,
	[downloads] [int] NULL,
	[fromDate] [char](40) NULL,
	[untilDate] [char](40) NULL,
 CONSTRAINT [PK_downloads] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE [dbo].[downloads] ADD  CONSTRAINT [DF_downloads_downloads]  DEFAULT (0) FOR [downloads]
GO
