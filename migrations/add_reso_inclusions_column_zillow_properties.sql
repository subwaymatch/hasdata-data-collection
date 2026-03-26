-- Migration: add reso_inclusions column to zillow_properties
-- Safe to re-run (ADD COLUMN IF NOT EXISTS).

-- 1. Add the new column
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_inclusions JSONB;

-- 2. Backfill from existing raw_json data
--    raw_json->'resoData'->'inclusions' is a JSON array (or absent/null).
UPDATE zillow_properties
SET reso_inclusions = raw_json->'resoData'->'inclusions'
WHERE raw_json->'resoData'->'inclusions' IS NOT NULL;
