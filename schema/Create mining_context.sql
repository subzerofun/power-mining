-- Drop everything first
DROP TABLE IF EXISTS mining_context;
DROP TYPE IF EXISTS mineral_category;

-- Create the category enum (this one is correct)
CREATE TYPE mineral_category AS ENUM ('Core', 'Surface', 'Both');

-- Create the table with separate columns for each ring type
CREATE TABLE mining_context (
    mineral_name TEXT PRIMARY KEY,
    short_name TEXT NOT NULL UNIQUE,
    mineral_category mineral_category NOT NULL,
    icy JSONB,
    metallic JSONB,
    metal_rich JSONB,
    rocky JSONB,
    hotspot BOOLEAN NOT NULL,
    conditions TEXT,
    typical_value TEXT
);

-- Create the indexes
CREATE INDEX idx_mining_context_category ON mining_context(mineral_category);
CREATE INDEX idx_mining_context_icy ON mining_context USING gin (icy);
CREATE INDEX idx_mining_context_metallic ON mining_context USING gin (metallic);
CREATE INDEX idx_mining_context_metal_rich ON mining_context USING gin (metal_rich);
CREATE INDEX idx_mining_context_rocky ON mining_context USING gin (rocky);
CREATE INDEX idx_mining_context_hotspot ON mining_context(hotspot) WHERE hotspot = true;