-- Native Dynamic Data Masking on _base_DimCustomer.CustomerID and
-- _base_DimDevice.DeviceFingerprint, plus SESSION_CONTEXT-gated row-level
-- security on _base_AggCustomerRiskProfile. Must run AFTER
-- 00_refresh_gold_serving.sql (which just dropped+recreated these three
-- tables, wiping any masking/RLS the previous incarnation carried) and
-- BEFORE 20_validate_security.sql. See warehouse/README.md.
--
-- Design mirrors the sibling banking POC's live-verified findings (see its
-- CLAUDE.md "Warehouse governance constraints"):
--   - Native DDM/RLS can only target a physical table gold_wh itself owns
--     — gold_lh is a Lakehouse (no ALTER TABLE at all) and a cross-database
--     view isn't a valid target either way. Hence the _base_* physical
--     copies from 00_refresh_gold_serving.sql.
--   - SESSION_CONTEXT() cannot appear in a SELECT list against any real
--     table, local or cross-database, in Fabric Warehouse's distributed
--     engine (error 15816, "not supported in distributed processing
--     mode") — but IS allowed in a WHERE clause against a real table. RLS
--     below is therefore a SESSION_CONTEXT-gated WHERE clause on a view
--     over the physical _base_AggCustomerRiskProfile table, the same
--     pattern proven live in the sibling POC — not native
--     CREATE SECURITY POLICY / ADD FILTER PREDICATE, which was not
--     exercised there and isn't gambled on here either.
--   - "Masked for a non-owner" is not independently verifiable in this
--     session (workspace creator = db_owner, and DDM exempts db_owner
--     unconditionally — confirmed live in the sibling POC). What IS
--     verified: the mask is genuinely configured (sys.masked_columns,
--     see 20_validate_security.sql).

-- --- Masking ---
-- CustomerID format is "CUST-" + 6 digits (e.g. CUST-000042); partial()
-- shows the "CUST-" prefix (5 chars) and masks the rest, since the prefix
-- alone carries no identifying information.
ALTER TABLE dbo._base_DimCustomer
ALTER COLUMN CustomerID ADD MASKED WITH (FUNCTION = 'partial(5,"XXXXXX",0)');
GO

-- DeviceFingerprint is an opaque device-identifying string (see
-- nb_gold_build.py / infra/governance/onelake_security.py, which already
-- targets this exact column name at /Tables/dimdevice ->
-- devicefingerprint). default() fully masks it — there's no safe partial
-- prefix/suffix to preserve for an opaque fingerprint the way there is for
-- a formatted business key.
ALTER TABLE dbo._base_DimDevice
ALTER COLUMN DeviceFingerprint ADD MASKED WITH (FUNCTION = 'default()');
GO

-- Governed passthrough views — masking is enforced automatically for any
-- query against the base table's column, view or not, so no CASE needed
-- here (same pattern as the sibling POC's DimCustomer view in
-- security_masking.sql).
CREATE OR ALTER VIEW dbo.DimCustomer AS
SELECT CustomerSK, CustomerID, _gold_loaded_at
FROM dbo._base_DimCustomer;
GO

CREATE OR ALTER VIEW dbo.DimDevice AS
SELECT DeviceSK, DeviceID, CustomerID, CustomerSK, OS, DeviceFingerprint, IsTrusted,
       _gold_loaded_at
FROM dbo._base_DimDevice;
GO

-- --- Row-level security ---
-- "Risk investigator" role/claim, gating access to the highest band that
-- actually exists on this table. Design note: the task brief's shorthand
-- was "High/Critical band rows", carried over from the transaction-level
-- RiskBand vocabulary (Low/Medium/High/Critical, used on
-- transaction_risk/FactTransactions). AggCustomerRiskProfile's own
-- CustomerRiskBand only ever takes Low/Medium/High per the exact formula
-- in ARCHITECTURE.md (no Critical band at the customer-aggregate grain) —
-- so the predicate below gates 'High' band rows, the highest band that
-- exists here. Flagged explicitly in this repo's delivery report as a
-- point where the task brief's wording didn't quite match
-- ARCHITECTURE.md's own formula.
--
-- Default-deny: no SESSION_CONTEXT set means the equality comparison
-- against NULL matches nothing extra, so 'High' rows are hidden and only
-- Medium/Low rows show — not everything.
CREATE OR ALTER VIEW dbo.AggCustomerRiskProfile AS
SELECT CustomerID, TransactionCount30D, TotalTransactionAmount30D, AverageTransactionRiskScore,
       HighRiskTransactionCount, FraudAlertCount, FailedLoginCount, DistinctDeviceCount,
       UntrustedDeviceCount, GeographicAnomalyCount, CustomerRiskScore, CustomerRiskBand,
       _gold_loaded_at
FROM dbo._base_AggCustomerRiskProfile
WHERE CustomerRiskBand <> 'High'
   OR CAST(SESSION_CONTEXT(N'InvestigatorRole') AS VARCHAR(30)) = 'RiskInvestigator';
GO
