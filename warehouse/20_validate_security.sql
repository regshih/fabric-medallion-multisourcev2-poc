-- Read-only verification queries for masking + RLS — a runbook step, not a
-- destructive script. Run after 00_refresh_gold_serving.sql ->
-- 10_apply_security.sql. Each SELECT below is meant to be eyeballed, not
-- asserted against automatically (no fixed expected row counts — those
-- depend on live data volumes).

-- 1. Confirm the two masked columns are genuinely configured.
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    c.name AS column_name,
    c.is_masked,
    mc.masking_function
FROM sys.masked_columns mc
JOIN sys.columns c ON mc.object_id = c.object_id AND mc.column_id = c.column_id
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name IN ('_base_DimCustomer', '_base_DimDevice');
GO
-- Expect exactly 2 rows: _base_DimCustomer.CustomerID and
-- _base_DimDevice.DeviceFingerprint, each is_masked = 1.

-- 2. Sample the masked values (this session is db_owner and DDM exempts
-- db_owner unconditionally — confirmed live in the sibling banking POC —
-- so this correctly shows PLAINTEXT for this session, not proof of
-- enforcement for a lesser-privileged principal).
SELECT TOP 5 CustomerSK, CustomerID FROM dbo.DimCustomer;
GO
SELECT TOP 5 DeviceSK, DeviceID, DeviceFingerprint FROM dbo.DimDevice;
GO

-- 3. Confirm the RLS view exists and carries the expected predicate text
-- (Fabric Warehouse RLS here is a WHERE-clause view, not a native
-- CREATE SECURITY POLICY object — see 10_apply_security.sql for why — so
-- there's no sys.security_policies row to check; the view definition
-- itself IS the control).
SELECT definition
FROM sys.sql_modules
WHERE object_id = OBJECT_ID('dbo.AggCustomerRiskProfile');
GO
-- Expect the definition text to contain both 'CustomerRiskBand' and
-- 'SESSION_CONTEXT'.

-- 4. Functional RLS check: no SESSION_CONTEXT set -> zero 'High' band rows
-- visible (default-deny), but total row count still positive since
-- Medium/Low rows are not restricted.
SELECT COUNT(*) AS high_band_rows_visible
FROM dbo.AggCustomerRiskProfile
WHERE CustomerRiskBand = 'High';
GO
-- Expect 0 in a session with no InvestigatorRole session context set.

EXEC sys.sp_set_session_context @key = N'InvestigatorRole', @value = N'RiskInvestigator';
GO
SELECT COUNT(*) AS high_band_rows_visible_as_investigator
FROM dbo.AggCustomerRiskProfile
WHERE CustomerRiskBand = 'High';
GO
-- Expect this count to be >= the previous one (all High-band rows now
-- visible in this session).
