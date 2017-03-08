Create Database DemoDW


USE DemoDW
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

-- Prestep: Configuring Hadoop flavor
exec sp_configure 'hadoop connectivity', 5
Reconfigure



/*
* STEP A: Create External Data Source
*		To specify LOCATION URI of Hadoop or Azure blob storage.
*		(Optional) Specify RESOURCE_MANAGER_LOCATION to enable push-down computation to Hadoop for faster query performance.
*		(Optional) Specify CREDENTIAL to authenticate against a Kerberos secured Hadoop cluster.
*/

CREATE EXTERNAL DATA SOURCE HadoopCDHCluster 
WITH (TYPE = Hadoop, LOCATION = 'hdfs://10.2.0.4:8020',
		Resource_Manager_Location = '10.2.0.4:8032')

SELECT * FROM sys.external_data_sources;



/*
* Step B: Create External File format 
*		To specify the layout of data stored in Hadoop or Azure blob storage that will be referenced by the external table.
*		Supported File Formats: Delimited Text, RCFile, ORC, Parquet
*/


CREATE EXTERNAL FILE FORMAT TextFile 
WITH (FORMAT_TYPE = DelimitedText, 
FORMAT_OPTIONS (FIELD_TERMINATOR = N'|'));

SELECT * FROM sys.external_file_formats;


-- IMPORT 
Create External Table Customer
(
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
)
WITH ( 
LOCATION = '/tmp/CustomerData/',  -- make sure this matches the file path you have in HUE!!!
		DATA_SOURCE = HadoopCDHCluster, 
		FILE_FORMAT = TextFile
)

Insert into InsuranceCustomers
 Select * FROM Customer


 Select * FROM InsuranceCustomers
 
