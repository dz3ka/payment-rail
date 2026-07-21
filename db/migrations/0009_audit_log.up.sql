-- Payment Rail append-only hash-chained audit log (F9 / WP1).
--
-- Every operator action and system state transition appends exactly one row
-- whose entry_hash = sha256(prev_hash || canonical(fields)), chaining each row
-- to its predecessor. Tampering with any historical row breaks the chain from
-- that point on, so the log is tamper-EVIDENT: a verifier rehashing the chain
-- detects the first altered row. seq is app-assigned (gap-free) under a chain
-- advisory lock rather than a serial/identity column, because a rolled-back
-- transaction must not burn a sequence value and leave a gap the verifier would
-- read as a deleted row. payload stores the exact canonical marshaled event
-- bytes (not jsonb) so the hash preimage stays byte-for-byte reproducible even
-- as future code evolves.

CREATE TABLE audit_log (
    seq            BIGINT PRIMARY KEY,              -- app-assigned under the chain advisory lock, gap-free (NOT serial/identity: a rolled-back tx must not consume a seq)
    prev_hash      BYTEA  NOT NULL,                 -- 32 bytes; genesis row's prev = 32 zero bytes
    entry_hash     BYTEA  NOT NULL UNIQUE,          -- sha256(prev_hash || canonical(...)) = this row's chain hash
    actor          TEXT   NOT NULL,                 -- operator handle, or "system:<component>" for state transitions
    action         TEXT   NOT NULL,                 -- namespaced verb, e.g. payment.created / settlement.confirmed / operator.approve
    aggregate_type TEXT   NOT NULL,
    aggregate_id   TEXT   NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,            -- event time, micro-truncated by the app before hashing
    payload        BYTEA  NOT NULL                  -- exact canonical marshaled event Data bytes (the hash preimage's data segment); NOT jsonb, so verify is byte-exact across future code changes
);

-- append-only enforcement (defense-in-depth; the hash chain is the tamper-EVIDENCE guarantee):
CREATE FUNCTION audit_log_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_log_no_mutate BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION audit_log_immutable();

CREATE TRIGGER audit_log_no_truncate BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit_log_immutable();
