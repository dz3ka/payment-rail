-- Reverse dependency order: entry_lines references journal_entries and accounts.
DROP TABLE IF EXISTS entry_lines;
DROP TABLE IF EXISTS journal_entries;
DROP TABLE IF EXISTS accounts;
