-- Reverse finality. Demote finalized rows back to settled before narrowing the
-- CHECK, or the new constraint would reject them; drop the block columns last.
UPDATE settlements SET status='settled' WHERE status='finalized';
ALTER TABLE settlements DROP CONSTRAINT settlements_status_check,
    ADD CONSTRAINT settlements_status_check
    CHECK (status IN ('pending','settled','reorged'));
ALTER TABLE settlements DROP COLUMN settled_block_number, DROP COLUMN settled_block_hash;
