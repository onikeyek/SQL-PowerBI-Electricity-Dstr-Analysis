-- Create the database
CREATE DATABASE ElectricityDistribution;
-- Use the database
USE ElectricityDistribution;

-- Create CUSTOMERS table
CREATE TABLE CUSTOMERS (
    CustomerID VARCHAR(50) PRIMARY KEY,  -- Unique ID for each customer
    TariffType VARCHAR(10),              -- Std or ToU
    ACORN_Code VARCHAR(20),              -- ACORN-A, ACORN-B, etc.
    ACORN_Group VARCHAR(50),             -- Affluent, Adversity, etc.
    DataFile VARCHAR(50)                 -- block_0, block_1, etc.
);

-- Create ACORN_CATEGORIES table
CREATE TABLE ACORN_CATEGORIES (
    ACORN_Code VARCHAR(20) PRIMARY KEY,   -- ACORN-A, ACORN-B, etc.
    ACORN_Group VARCHAR(50),              -- Affluent, Adversity, Comfortable
    Description VARCHAR(500)              -- Detailed description of this demographic
);


-- Create TARIFFS table
CREATE TABLE TARIFFS (
    TariffID INT PRIMARY KEY IDENTITY(1,1),  -- Auto-incrementing ID
    TariffType VARCHAR(10) UNIQUE,            -- Std or ToU
    TariffName VARCHAR(100),                  -- Descriptive name
    StandardRate DECIMAL(10,4),               -- Rate for Std tariff (per kWh)
    PeakRate DECIMAL(10,4),                   -- Peak rate for ToU (per kWh)
    OffPeakRate DECIMAL(10,4),                -- Off-peak rate for ToU (per kWh)
    Description VARCHAR(500)                  -- Details about the tariff
);

-- Create CONSUMPTION table
CREATE TABLE CONSUMPTION (
    ConsumptionID INT PRIMARY KEY IDENTITY(1,1),  -- Auto-generated ID
    CustomerID VARCHAR(50),                        -- Which customer
    ReadingDateTime DATETIME,                      -- When was this reading
    kWh_Consumed DECIMAL(10,4),                    -- How much electricity used
    
    -- Foreign key to link to CUSTOMERS table
    FOREIGN KEY (CustomerID) REFERENCES CUSTOMERS(CustomerID)
);

-- Create WEATHER table
CREATE TABLE WEATHER (
    WeatherID INT PRIMARY KEY IDENTITY(1,1),  -- Auto-generated ID
    ReadingDateTime DATETIME,                  -- When was this weather reading
    Temperature DECIMAL(5,2),                  -- Temperature in Celsius
    Visibility DECIMAL(5,2),                   -- Visibility in km
    Humidity DECIMAL(5,2),                     -- Humidity percentage
    WindSpeed DECIMAL(5,2),                    -- Wind speed
    Pressure DECIMAL(7,2)                      -- Atmospheric pressure
);




-- Insert ACORN Categories data
INSERT INTO ACORN_CATEGORIES (ACORN_Code, ACORN_Group, Description) VALUES
('ACORN-A', 'Affluent', 'Wealthy achievers - highest income areas'),
('ACORN-B', 'Affluent', 'Affluent greys - mature, wealthy households'),
('ACORN-C', 'Affluent', 'Flourishing families - prosperous young families'),
('ACORN-D', 'Affluent', 'Prosperous professionals - well-off professionals'),
('ACORN-E', 'Affluent', 'Educated urbanites - young educated city dwellers'),
('ACORN-F', 'Comfortable', 'Aspiring singles - singles in modern developments'),
('ACORN-G', 'Comfortable', 'Starting out - young people starting careers'),
('ACORN-H', 'Comfortable', 'Secure families - settled families in suburbs'),
('ACORN-I', 'Comfortable', 'Settled suburbia - established suburban areas'),
('ACORN-J', 'Comfortable', 'Prudent pensioners - retired people, moderate income'),
('ACORN-K', 'Adversity', 'Challenged Asian terraces - lower income Asian areas'),
('ACORN-L', 'Adversity', 'Inner city diversity - ethnically diverse urban areas'),
('ACORN-M', 'Adversity', 'Welfare borderline - people on benefits edge'),
('ACORN-N', 'Adversity', 'Municipal dependency - council housing, benefits'),
('ACORN-O', 'Adversity', 'Blue collar communities - traditional working class'),
('ACORN-P', 'Adversity', 'Struggling families - low income families'),
('ACORN-Q', 'Adversity', 'Difficult circumstances - most deprived areas');


-- Verify the import
SELECT * FROM ACORN_CATEGORIES;


-- Insert Tariff data
INSERT INTO TARIFFS (TariffType, TariffName, StandardRate, PeakRate, OffPeakRate, Description) VALUES
('Std', 'Standard Tariff', 0.1500, NULL, NULL, 'Fixed rate charged at all times - simple and predictable pricing'),
('ToU', 'Time of Use Tariff', NULL, 0.2000, 0.1000, 'Dynamic pricing - higher rates during peak hours (4pm-8pm), lower rates at night');
GO

-- Verify the import
SELECT * FROM TARIFFS;


-- Import CUSTOMERS from CSV
BULK INSERT CUSTOMERS
FROM 'C:\Users\Titanium\Downloads\archive\informations_households.csv'
WITH (
    FIRSTROW = 2,              -- Skip header row
    FIELDTERMINATOR = ',',     -- CSV delimiter
    ROWTERMINATOR = '\n',      -- New line
    TABLOCK
);


-- Verify the import
SELECT COUNT(*) AS TotalCustomers FROM CUSTOMERS;
SELECT TOP 10 * FROM CUSTOMERS;


-- Import WEATHER from CSV
BULK INSERT WEATHER
FROM 'C:\Users\Titanium\Downloads\archive\weather_hourly_darksky.csv'
WITH (
    FIRSTROW = 2,              -- Skip header row
    FIELDTERMINATOR = ',',     -- CSV delimiter
    ROWTERMINATOR = '\n',      -- New line
    TABLOCK
);


-- Verify the import
SELECT COUNT(*) AS TotalWeatherRecords FROM WEATHER;
SELECT TOP 10 * FROM WEATHER ORDER BY ReadingDateTime;




-- Create staging table matching CSV structure
CREATE TABLE WEATHER_STAGING (
    visibility DECIMAL(5,2),
    windBearing INT,
    temperature DECIMAL(5,2),
    time VARCHAR(50),
    dewPoint DECIMAL(5,2),
    pressure DECIMAL(7,2),
    apparentTemperature DECIMAL(5,2),
    windSpeed DECIMAL(5,2),
    precipType VARCHAR(20),
    icon VARCHAR(50),
    humidity DECIMAL(5,2),
    summary VARCHAR(200)
);
-- Import hourly weather data into staging table
BULK INSERT WEATHER_STAGING
FROM 'C:\Users\Titanium\Downloads\archive\weather_hourly_darksky.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);


-- Check how many records imported
SELECT COUNT(*) AS TotalRecords FROM WEATHER_STAGING;
SELECT TOP 5 * FROM WEATHER_STAGING;

-- Copy data from WEATHER_STAGING to WEATHER table
INSERT INTO WEATHER (ReadingDateTime, Temperature, Visibility, Humidity, WindSpeed, Pressure)
SELECT 
    CAST(time AS DATETIME) AS ReadingDateTime,  -- Simple cast to datetime
    temperature,
    visibility,
    humidity,
    windSpeed,
    pressure
FROM WEATHER_STAGING;


-- Verify the data in WEATHER table
SELECT COUNT(*) AS TotalWeatherRecords FROM WEATHER;
SELECT TOP 10 * FROM WEATHER ORDER BY ReadingDateTime;


-- Clean up: Drop the staging table (we don't need it anymore)
DROP TABLE WEATHER_STAGING;
 

-- Create staging table matching the daily CSV structure
CREATE TABLE CONSUMPTION_STAGING (
    LCLid VARCHAR(50),
    day VARCHAR(50),
    energy_median DECIMAL(18,10),
    energy_mean DECIMAL(18,10),
    energy_max DECIMAL(18,10),
    energy_count INT,
    energy_std DECIMAL(18,10),
    energy_sum DECIMAL(18,10),
    energy_min DECIMAL(18,10)
);

-- Drop and recreate staging table
DROP TABLE CONSUMPTION_STAGING;
GO

CREATE TABLE CONSUMPTION_STAGING (
    LCLid VARCHAR(50),
    day VARCHAR(50),
    energy_median VARCHAR(100),
    energy_mean VARCHAR(100),
    energy_max VARCHAR(100),
    energy_count VARCHAR(50),
    energy_std VARCHAR(100),
    energy_sum VARCHAR(100),
    energy_min VARCHAR(100)
);
GO

--  importing with different settings
BULK INSERT CONSUMPTION_STAGING
FROM 'C:\Users\Titanium\Downloads\archive\daily_dataset.csv'
WITH (
    CODEPAGE = '65001',        -- UTF-8 encoding
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',    -- Unix-style line ending
    TABLOCK,
    MAXERRORS = 0
);


-- Confirm the import
SELECT COUNT(*) AS TotalRecords FROM CONSUMPTION_STAGING;
SELECT TOP 10 * FROM CONSUMPTION_STAGING;AGING;


-- Copy data from CONSUMPTION_STAGING to CONSUMPTION table
INSERT INTO CONSUMPTION (CustomerID, ReadingDateTime, kWh_Consumed)
SELECT 
    LCLid AS CustomerID,                          -- Rename column
    CAST(day AS DATETIME) AS ReadingDateTime,     -- Convert to datetime
    CAST(energy_sum AS DECIMAL(10,4)) AS kWh_Consumed  -- Convert to number, use daily total
FROM CONSUMPTION_STAGING
WHERE LCLid IN (SELECT CustomerID FROM CUSTOMERS);  -- Only import customers we have
GO

-- Verify the import
SELECT COUNT(*) AS TotalConsumptionRecords FROM CONSUMPTION;

-- See sample data
SELECT TOP 10 
    C.CustomerID,
    C.ReadingDateTime,
    C.kWh_Consumed,
    CU.TariffType,
    CU.ACORN_Group
FROM CONSUMPTION C
INNER JOIN CUSTOMERS CU ON C.CustomerID = CU.CustomerID
ORDER BY C.ReadingDateTime;
GO

-- Clean up: Drop the staging table
DROP TABLE CONSUMPTION_STAGING;
