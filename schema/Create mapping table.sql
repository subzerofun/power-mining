-- Create mapping table
CREATE TABLE commodity_types (
    commodity_id INTEGER PRIMARY KEY,
    commodity_name TEXT UNIQUE
);

-- Populate it with unique commodities
INSERT INTO commodity_types (commodity_id, commodity_name)
SELECT ROW_NUMBER() OVER (ORDER BY commodity_name), commodity_name
FROM (SELECT DISTINCT commodity_name FROM station_commodities) AS unique_commodities;

-- Create new commodities table with IDs
CREATE TABLE station_commodities_mapped (  -- Changed name to _mapped instead of _new
    system_id64 BIGINT,
    station_id BIGINT,
    station_name TEXT,
    commodity_id INTEGER REFERENCES commodity_types(commodity_id),
    sell_price INTEGER,
    demand INTEGER,
    FOREIGN KEY(system_id64) REFERENCES systems(id64),
    FOREIGN KEY(system_id64, station_id) REFERENCES stations(system_id64, station_id)
);

-- Copy data with mapped IDs
INSERT INTO station_commodities_mapped (system_id64, station_id, station_name, commodity_id, sell_price, demand)
SELECT sc.system_id64, sc.station_id, sc.station_name, ct.commodity_id, sc.sell_price, sc.demand
FROM station_commodities sc
JOIN commodity_types ct ON sc.commodity_name = ct.commodity_name;

-- Verify counts match
SELECT COUNT(*) FROM station_commodities;
SELECT COUNT(*) FROM station_commodities_mapped;

-- If everything looks good:
DROP TABLE station_commodities;  -- Drop old table

-- Create view with the original name
CREATE VIEW station_commodities AS  -- View gets the original name
SELECT sc.system_id64, sc.station_id, sc.station_name, ct.commodity_name, sc.sell_price, sc.demand
FROM station_commodities_mapped sc  -- Points to our mapped table
JOIN commodity_types ct ON sc.commodity_id = ct.commodity_id;