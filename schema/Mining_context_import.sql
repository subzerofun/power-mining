-- Drop everything in correct order
DROP TABLE IF EXISTS mining_context;
DROP TYPE IF EXISTS mineral_category;

-- Create the enum type
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

-- Create indexes
CREATE INDEX idx_mining_context_category ON mining_context(mineral_category);
CREATE INDEX idx_mining_context_icy ON mining_context USING gin (icy);
CREATE INDEX idx_mining_context_metallic ON mining_context USING gin (metallic);
CREATE INDEX idx_mining_context_metal_rich ON mining_context USING gin (metal_rich);
CREATE INDEX idx_mining_context_rocky ON mining_context USING gin (rocky);
CREATE INDEX idx_mining_context_hotspot ON mining_context(hotspot) WHERE hotspot = true;

-- Import function
CREATE OR REPLACE FUNCTION import_mining_context(data jsonb)
RETURNS void AS $$
DECLARE
    material jsonb;
    material_name text;
    category text;
    has_hotspot boolean;
    ring_data jsonb;
BEGIN
    FOR material_name, material IN SELECT * FROM jsonb_each(data->'materials')
    LOOP
        -- Map the mineral category
        category := CASE material->>'type'
            WHEN 'core' THEN 'Core'
            WHEN 'non-core' THEN 'Surface'
            WHEN 'both' THEN 'Both'
            ELSE NULL
        END;

        -- Check if material has hotspots in any ring type
        SELECT EXISTS (
            SELECT 1 
            FROM jsonb_each(material->'ring_types')
            WHERE value->>'hotspot' = 'true'
        ) INTO has_hotspot;

        -- Insert or update the record
        INSERT INTO mining_context (
            mineral_name,
            short_name,
            mineral_category,
            icy,
            metallic,
            metal_rich,
            rocky,
            hotspot,
            conditions,
            typical_value
        ) VALUES (
            material->>'name',
            material->>'short',
            category::mineral_category,
            -- For each ring type, create an array of available mining methods
            (
                SELECT jsonb_agg(method)
                FROM (
                    SELECT unnest(ARRAY['core', 'surfaceDeposit', 'subSurfaceDeposit', 'surfaceLaserMining']) as method
                    WHERE (material->'ring_types'->'Icy'->>'core')::boolean 
                       OR (material->'ring_types'->'Icy'->>'surfaceDeposit')::boolean
                       OR (material->'ring_types'->'Icy'->>'subSurfaceDeposit')::boolean
                       OR (material->'ring_types'->'Icy'->>'surfaceLaserMining')::boolean
                ) m
                WHERE (material->'ring_types'->'Icy'->>method)::boolean = true
            ),
            (
                SELECT jsonb_agg(method)
                FROM (
                    SELECT unnest(ARRAY['core', 'surfaceDeposit', 'subSurfaceDeposit', 'surfaceLaserMining']) as method
                    WHERE (material->'ring_types'->'Metallic'->>'core')::boolean 
                       OR (material->'ring_types'->'Metallic'->>'surfaceDeposit')::boolean
                       OR (material->'ring_types'->'Metallic'->>'subSurfaceDeposit')::boolean
                       OR (material->'ring_types'->'Metallic'->>'surfaceLaserMining')::boolean
                ) m
                WHERE (material->'ring_types'->'Metallic'->>method)::boolean = true
            ),
            (
                SELECT jsonb_agg(method)
                FROM (
                    SELECT unnest(ARRAY['core', 'surfaceDeposit', 'subSurfaceDeposit', 'surfaceLaserMining']) as method
                    WHERE (material->'ring_types'->'Metal Rich'->>'core')::boolean 
                       OR (material->'ring_types'->'Metal Rich'->>'surfaceDeposit')::boolean
                       OR (material->'ring_types'->'Metal Rich'->>'subSurfaceDeposit')::boolean
                       OR (material->'ring_types'->'Metal Rich'->>'surfaceLaserMining')::boolean
                ) m
                WHERE (material->'ring_types'->'Metal Rich'->>method)::boolean = true
            ),
            (
                SELECT jsonb_agg(method)
                FROM (
                    SELECT unnest(ARRAY['core', 'surfaceDeposit', 'subSurfaceDeposit', 'surfaceLaserMining']) as method
                    WHERE (material->'ring_types'->'Rocky'->>'core')::boolean 
                       OR (material->'ring_types'->'Rocky'->>'surfaceDeposit')::boolean
                       OR (material->'ring_types'->'Rocky'->>'subSurfaceDeposit')::boolean
                       OR (material->'ring_types'->'Rocky'->>'surfaceLaserMining')::boolean
                ) m
                WHERE (material->'ring_types'->'Rocky'->>method)::boolean = true
            ),
            has_hotspot,
            material->>'conditions',
            material->>'typical_value'
        ) ON CONFLICT (mineral_name) DO UPDATE SET
            short_name = EXCLUDED.short_name,
            mineral_category = EXCLUDED.mineral_category,
            icy = EXCLUDED.icy,
            metallic = EXCLUDED.metallic,
            metal_rich = EXCLUDED.metal_rich,
            rocky = EXCLUDED.rocky,
            hotspot = EXCLUDED.hotspot,
            conditions = EXCLUDED.conditions,
            typical_value = EXCLUDED.typical_value;
    END LOOP;
END;
$$ LANGUAGE plpgsql;