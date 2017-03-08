USE [DemoDW]
GO




------------------------------- (1) Polybase Building Blocks ------------------------------------------------------------------

/*
 Create External Table
*		To reference data stored in a Hadoop cluster or Azure blob storage.
*		Specify LOCATION: relative file path of the data from hdfs root
*		Specify DATA_SOURCE & FILE_FORMAT objects
*/

CREATE EXTERNAL TABLE [dbo].[SensorData]
(
	[SensorKey] [int] NOT NULL,
	[CustomerKey] [int] NOT NULL,
	[GeographyKey] [int] NULL,
	[Speed] [float] NOT NULL,
	[YearMeasured] [int] NOT NULL
)
WITH (LOCATION = '/tmp/SensorData/', 
		DATA_SOURCE = HadoopCDHCluster, 
		FILE_FORMAT = TextFile
);


SELECT * FROM sys.external_tables;
SELECT * FROM sys.tables where is_external = 1;

/*
* Step D: Create statistics on external & local tables for faster query performance
*/


CREATE STATISTICS Stats_For_SensorData on SensorData (CustomerKey,Speed);
CREATE STATISTICS Stats_For_InsuredCustomers 
on InsuranceCustomers (CustomerKey, Gender, MaritalStatus, YearlyIncome);



------------------------------- (2) Polybase Queries ------------------------------------------------------------------

-- STEP E: Who drives faster than 65 mph? 
--  Joining structured customer data stored in SQL Server 
--  with sensor data stored in external Hadoop cluster

SELECT DISTINCT InsuranceCustomers.FirstName, InsuranceCustomers.LastName, 
				SensorData.Speed
FROM InsuranceCustomers INNER JOIN SensorData  
ON InsuranceCustomers.CustomerKey = SensorData.CustomerKey 
WHERE SensorData.Speed > 65 
ORDER BY SensorData.Speed DESC


-- Push-down computation to Hadoop this time
SELECT DISTINCT InsuranceCustomers.FirstName, InsuranceCustomers.LastName, 
				SensorData.Speed
FROM InsuranceCustomers INNER JOIN SensorData  
ON InsuranceCustomers.CustomerKey = SensorData.CustomerKey 
WHERE SensorData.Speed > 65 
ORDER BY SensorData.Speed DESC
OPTION (FORCE EXTERNALPUSHDOWN);  

-- PRESENTER HINT: 
-- The above query runs for about 50 seconds. 
-- Navigate to Jobtracker in your browser to see details of the Map job submitted to the hadoop cluster.
-- Run MonitorWithDMVs script to drill into the execution of this query while it is running: show the distributed execution steps and the external hadoop operation.


-- STEP F: What is the yearly income and gender of fast drivers?

SELECT DISTINCT InsuranceCustomers.FirstName, InsuranceCustomers.LastName, 
                InsuranceCustomers.YearlyIncome, InsuranceCustomers.Gender
FROM InsuranceCustomers JOIN SensorData  
ON InsuranceCustomers.CustomerKey = SensorData.CustomerKey 
WHERE SensorData.Speed > 65 
ORDER BY InsuranceCustomers.YearlyIncome

-- PRESENTER HINT: 
-- "Wow, most of the fast drivers are males"



------------------------------- (3) Import Hadoop Data ------------------------------------------------------------------
 
 -- Import the sensor data for fastest 5% 
 SELECT DISTINCT TOP 5 PERCENT * INTO FastestDrivers
 FROM SensorData
 ORDER BY SensorData.Speed;



 ------------------------------- (4) Export to Hadoop  ------------------------------------------------------------------


-- PRESENTER HINT: 
-- By default, the next 2 queries export the data for 2006.
-- Navigate to the Archive folder on HDFS: http://10.193.26.179:50075/browseDirectory.jsp?dir=%2FArchive&namenodeInfoPort=50070&nnaddr=10.193.26.177:8020 
-- You will see all the years that have already been exported
-- Choose a year that has NOT been exported already and use that in the queries below


-- Archive old customer data to Hadoop
-- Create external table pointing to the export location
CREATE EXTERNAL TABLE [dbo].[FastestCustomers2006]
(
	[CustomerKey] [int] NOT NULL,
	[GeographyKey] [int] NULL,
	[FirstName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NULL,
	[Speed] [float] NULL,
	[YearMeasured] [int] NULL
)
WITH (LOCATION = '/tmp/Archive/2006/', 
		DATA_SOURCE = HadoopCDHCluster, 
		FILE_FORMAT = TextFile
);

-- Export the results of join between local and external data to hadoop
INSERT INTO dbo.FastestCustomers2006
SELECT T1.CustomerKey, T1.GeographyKey, T1.FirstName, T1.LastName,
	T2.Speed, T2.YearMeasured
FROM InsuranceCustomers T1 JOIN SensorData T2
ON T1.CustomerKey = T2.CustomerKey
WHERE T2.YearMeasured = 2006 and T2.Speed > 65;



SELECT * FROM dbo.FastestCustomers2006
ORDER BY Speed DESC;


 -- Clean up
 DROP TABLE dbo.FastestDrivers;
 DROP EXTERNAL TABLE dbo.FastestCustomers2006;
 DROP EXTERNAL TABLE dbo.SensorData;
 DROP EXTERNAL DATA SOURCE HadoopCDHCluster;
 DROP EXTERNAL FILE FORMAT TextFile;


