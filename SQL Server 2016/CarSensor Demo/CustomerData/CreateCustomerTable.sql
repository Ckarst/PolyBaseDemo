USE [DemoDW]
GO

/****** Object:  Table [dbo].[Insurance_Customer]    Script Date: 9/26/2015 1:33:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[InsuranceCustomers](
	[CustomerKey] [int] NOT NULL,
	[GeographyKey] [int] NULL,
	[FirstName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NULL,
	[BirthDate] [date] NULL,
	[MaritalStatus] [nchar](1) NULL,
	[Gender] [nvarchar](1) NULL,
	[TotalChildren] [tinyint] NULL,
	[YearlyIncome] [money] NULL,
	[AddressLine1] [nvarchar](120) NULL,
	[Phone] [nvarchar](20) NULL,
	[CommuteDistance] [nvarchar](15) NULL
) ON [PRIMARY]

GO


