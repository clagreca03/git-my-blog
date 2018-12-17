/* Set up the demo */
CREATE DATABASE DemoDB; 
GO

USE DemoDB; 
GO


/* Create a table to store database file size metrics */
IF OBJECT_ID('dbo.DatabaseFileSize', 'U') IS NOT NULL
DROP TABLE dbo.DatabaseFileSize; 
CREATE TABLE dbo.DatabaseFileSize
(

	ID INT NOT NULL IDENTITY(1,1), 
	ServerName NVARCHAR(128) NOT NULL,
	DatabaseID SMALLINT	 NOT NULL,
	DatabaseName NVARCHAR(128) NOT NULL, 
	FileType NVARCHAR(60) NOT NULL,
	LogicalFileName SYSNAME NOT NULL,
	PhisicalFileName NVARCHAR(260) NOT NULL,
	FileSizeKB INT NOT NULL,
	FileSizeMB DECIMAL(14,4) NOT NULL, 
	SpaceUsedMB DECIMAL(14,4) NOT NULL, 
	FreeSpaceMB DECIMAL(14,4) NOT NULL,
	PollDate DATETIME NOT NULL,
	PRIMARY KEY CLUSTERED (ID)

);
GO




/* Capture the database file size metrics from the sys.database_files catalog view */
INSERT INTO dbo.DatabaseFileSize
(
    ServerName,
	DatabaseID,
    DatabaseName,
    FileType,
    LogicalFileName,
    PhisicalFileName,
    FileSizeKB,
    FileSizeMB,
    SpaceUsedMB,
    FreeSpaceMB,
    PollDate
)
SELECT 
	@@SERVERNAME AS ServerName, 
	DB_ID() AS DatabaseID,
	DB_NAME(DB_ID()) AS DatabaseName,
	database_files.type_desc AS FileType,
	database_files.name AS LogicalFileName, 
	database_files.physical_name AS PhysicalFileName, 
	database_files.size * 8 AS FileSize, -- Current size of file in 8KB pages
	CAST(database_files.size / 128.0 AS DECIMAL(14, 4)) AS FileSizeMB, -- Same as (size * 8 / 1024)
	CAST(FILEPROPERTY(database_files.name, 'SpaceUsed') / 128.0 AS DECIMAL(14, 4)) AS SpaceUsedMB,
	CAST((database_files.size / 128.0) - (FILEPROPERTY(database_files.name, 'SpaceUsed') / 128.0) AS DECIMAL(14, 4)) AS FreeSpaceMB, 
	GETDATE() AS PollDate
FROM sys.database_files
WHERE database_files.type_desc IN ('ROWS', 'LOG');




/* Cursor for iterating over all the databases and capturing the file size data */
-- Create List of Target Databases
DECLARE @DatabaseList TABLE
(
	DatabaseName VARCHAR(50)
);
INSERT INTO @DatabaseList (DatabaseName) 
SELECT [name]
FROM sys.databases

-- Initialize Variables
DECLARE @DatabaseName VARCHAR(50);
DECLARE @stmt NVARCHAR(MAX);

-- Initialize Cursor
DECLARE db_cursor CURSOR FOR  
SELECT DatabaseName 
FROM @DatabaseList

OPEN db_cursor   
FETCH NEXT FROM db_cursor INTO @DatabaseName   

-- Begin Loop
WHILE @@FETCH_STATUS = 0   
BEGIN 

	--Build and Execute Statement
	SET @stmt = 
		'
			USE ' + @DatabaseName + '

			INSERT INTO DemoDB.dbo.DatabaseFileSize
			(
				ServerName,
				DatabaseID,
				DatabaseName,
				FileType,
				LogicalFileName,
				PhisicalFileName,
				FileSizeKB,
				FileSizeMB,
				SpaceUsedMB,
				FreeSpaceMB,
				PollDate
			)
			SELECT 
				@@SERVERNAME AS ServerName, 
				DB_ID() AS DatabaseID,
				DB_NAME(DB_ID()) AS DatabaseName,
				database_files.type_desc AS FileType,
				database_files.name AS LogicalFileName, 
				database_files.physical_name AS PhysicalFileName, 
				database_files.size * 8 AS FileSize, -- Current size of file in 8KB pages
				CAST(database_files.size / 128.0 AS DECIMAL(14, 4)) AS FileSizeMB, -- Same as (size * 8 / 1024)
				CAST(FILEPROPERTY(database_files.name, ''SpaceUsed'') / 128.0 AS DECIMAL(14, 4)) AS SpaceUsedMB,
				CAST((database_files.size / 128.0) - (FILEPROPERTY(database_files.name, ''SpaceUsed'') / 128.0) AS DECIMAL(14, 4)) AS FreeSpaceMB, 
				GETDATE() AS PollDate
			FROM sys.database_files
			WHERE database_files.type_desc IN (''ROWS'', ''LOG'')

		';

	EXECUTE sp_executesql @stmt
	FETCH NEXT FROM db_cursor INTO @DatabaseName 

END

CLOSE db_cursor   
DEALLOCATE db_cursor




/* Example of a useful query with a measure for data file growth over time */
SELECT f.DatabaseName, f.LogicalFileName, f.FileType, f.FileSizeMB, f.SpaceUsedMB, f.FreeSpaceMB, f.PollDate, 
	(f.SpaceUsedMB) - LAG(f.SpaceUsedMB, 1) OVER(PARTITION BY f.DatabaseName, f.LogicalFileName ORDER by f.PollDate) AS Growth
FROM dbo.DatabaseFileSize f




/* Clean up the demo */
USE master; 
GO

DROP DATABASE DemoDB; 
GO
