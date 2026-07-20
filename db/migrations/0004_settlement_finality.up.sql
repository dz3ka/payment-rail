-- Settlement finality (M3 / settlement-recovery slice). Once a settled tx is
-- buried deep enough to be reorg-safe, the chainwatcher promotes it to the
-- terminal 'finalized' status and records the block that carried it, so a
-- later reorg of a shallower block can no longer walk it back. The block
-- columns are nullable: they stay null until a tx settles.
ALTER TABLE settlements
    ADD COLUMN settled_block_hash   TEXT,
    ADD COLUMN settled_block_number BIGINT;
ALTER TABLE settlements DROP CONSTRAINT settlements_status_check,
    ADD CONSTRAINT settlements_status_check
    CHECK (status IN ('pending','settled','reorged','finalized'));
