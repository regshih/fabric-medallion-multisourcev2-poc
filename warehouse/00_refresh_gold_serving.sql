-- Refresh gold_wh's serving layer from gold_lh. Must run FIRST, every time,
-- before 10_apply_security.sql — masking/RLS are wiped whenever a base
-- table is dropped+recreated (per physical table object, not per name).
-- See warehouse/README.md for the full execution-order rationale.
--
-- Table names are lowercase here on purpose: Spark/Hive lowercases table
-- names in the metastore regardless of the case used in saveAsTable() (see
-- CLAUDE.md "Notebook deploy/run mechanics"), so gold_lh's SQL-visible
-- objects are dimcustomer/facttransactions/etc, not DimCustomer/
-- FactTransactions. View/table names on this (gold_wh) side stay
-- PascalCase to match the star-schema design in ARCHITECTURE.md, since we
-- choose those names directly in T-SQL, not via Spark.
--
-- Cross-database read syntax (gold_wh querying gold_lh directly, same
-- workspace, no linked server) follows the pattern already live-verified
-- in the sibling banking POC's warehouse/ddl_gold_views.sql.

-- Governed objects — physical local copies. DimCustomer/DimDevice need
-- native Dynamic Data Masking (10_apply_security.sql); AggCustomerRiskProfile
-- needs SESSION_CONTEXT-gated row-level security. Both require a table
-- gold_wh itself owns — a cross-database view is not a valid DDM/RLS
-- target (see warehouse/README.md).
DROP TABLE IF EXISTS dbo._base_DimCustomer;
GO
CREATE TABLE dbo._base_DimCustomer AS SELECT * FROM gold_lh.dbo.dimcustomer;
GO

DROP TABLE IF EXISTS dbo._base_DimDevice;
GO
CREATE TABLE dbo._base_DimDevice AS SELECT * FROM gold_lh.dbo.dimdevice;
GO

DROP TABLE IF EXISTS dbo._base_AggCustomerRiskProfile;
GO
CREATE TABLE dbo._base_AggCustomerRiskProfile AS SELECT * FROM gold_lh.dbo.aggcustomerriskprofile;
GO

-- Everything else needs no governance — zero-copy cross-database views
-- straight over gold_lh.
CREATE OR ALTER VIEW dbo.DimAccount AS
SELECT AccountSK, AccountID, CustomerID, CustomerSK, _gold_loaded_at
FROM gold_lh.dbo.dimaccount;
GO

CREATE OR ALTER VIEW dbo.DimMerchant AS
SELECT MerchantSK, MerchantID, MerchantName, MerchantCategory, City, State, Country,
       MerchantRiskCategory, _gold_loaded_at
FROM gold_lh.dbo.dimmerchant;
GO

CREATE OR ALTER VIEW dbo.DimDate AS
SELECT DateKey, FullDate, Year, Quarter, Month, MonthName, Day, DayOfWeek, DayName, IsWeekend,
       _gold_loaded_at
FROM gold_lh.dbo.dimdate;
GO

CREATE OR ALTER VIEW dbo.FactTransactions AS
SELECT TransactionID, CustomerSK, AccountSK, MerchantSK, DateKey, TransactionTimestamp, Amount,
       Currency, TransactionType, Channel, Country, DeviceID, CardPresent, TransactionStatus,
       RiskScore, RiskBand, _gold_loaded_at
FROM gold_lh.dbo.facttransactions;
GO

CREATE OR ALTER VIEW dbo.FactDigitalSessions AS
SELECT SessionID, CustomerSK, CustomerID, DeviceID, DeviceSK, DateKey, LoginTimestamp,
       LogoutTimestamp, DeviceType, DeviceOS, DeviceBrowser, GeoCountry, GeoCity, GeoIpAddress,
       AuthMethod, AuthMfaUsed, AuthSuccess, ActivitiesJson, _gold_loaded_at
FROM gold_lh.dbo.factdigitalsessions;
GO

CREATE OR ALTER VIEW dbo.FactFraudAlerts AS
SELECT AlertID, CustomerSK, CustomerID, TransactionID, DateKey, AlertTimestamp, Severity,
       AlertType, _gold_loaded_at
FROM gold_lh.dbo.factfraudalerts;
GO
