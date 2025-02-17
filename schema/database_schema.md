# Database Schema

## Table: `systems`

**Columns:**

| Column              | Type               | Nullable | Default | Description/Notes                                                 |
| ------------------- | ------------------ | -------- | ------- | ----------------------------------------------------------------- |
| `id64`              | `BIGINT`           | NO       |         | Primary key (unique system identifier).                           |
| `name`              | `TEXT`             | NO       |         | Unique system name.                                               |
| `x`                 | `DOUBLE PRECISION` | YES      |         | X coordinate.                                                     |
| `y`                 | `DOUBLE PRECISION` | YES      |         | Y coordinate.                                                     |
| `z`                 | `DOUBLE PRECISION` | YES      |         | Z coordinate.                                                     |
| `controlling_power` | `TEXT`             | YES      |         | Name of the controlling power (if any).                           |
| `power_state`       | `TEXT`             | YES      |         | A string indicating the system's power state (e.g., "Fortified"). |
| `powers_acquiring`  | `JSONB`            | YES      |         | JSON field for additional power-acquisition data.                 |
| `distance_from_sol` | `DOUBLE PRECISION` | YES      |         | Pre-calculated distance to Sol, if used.                          |

**Constraints and Indexes:**

- **Primary Key**: `systems_pkey` on `(id64)`
- **Unique Constraint**: `unique_system_name` on `(name)`
- **Indexes**:
  - `idx_systems_coords` (btree on `(x, y, z)`)
  - `idx_systems_name` (btree on `(name)`)
  - `idx_systems_powers_acquiring` (GIN on `(powers_acquiring)`)
- **Foreign-Key References**:
  - Referenced by `mineral_signals(system_id64)`, `station_commodities(system_id64)`, and `stations(system_id64)`.

---

## Table: `mineral_signals`

**Columns:**

| Column          | Type      | Nullable | Default | Description/Notes                                     |
| --------------- | --------- | -------- | ------- | ----------------------------------------------------- |
| `system_id64`   | `BIGINT`  | NO       |         | References `systems(id64)`, identifying which system. |
| `body_name`     | `TEXT`    | NO       |         | Name of the celestial body (planet, ring, etc.).      |
| `ring_name`     | `TEXT`    | NO       |         | Specific name of the ring.                            |
| `ring_type`     | `TEXT`    | NO       |         | Type of ring (e.g., Metal Rich, Rocky, Icy).          |
| `mineral_type`  | `TEXT`    | YES      |         | Type of mineral detected (e.g., Painite, LTD, etc.).  |
| `signal_count`  | `INTEGER` | YES      |         | Number of hotspot signals for that mineral.           |
| `reserve_level` | `TEXT`    | YES      |         | Reserve level (e.g., "Major", "Pristine").            |

**Constraints and Indexes:**

- **Foreign Key**: `fk_mineral_signals_system` (on `(system_id64) REFERENCES systems(id64) ON DELETE CASCADE`)
- **Index**: `idx_mineral_signals_type` on `(mineral_type)`

---

## Table: `stations`

**Columns:**

| Column                | Type                          | Nullable | Default | Description/Notes                                                    |
| --------------------- | ----------------------------- | -------- | ------- | -------------------------------------------------------------------- |
| `system_id64`         | `BIGINT`                      | NO       |         | References `systems(id64)`; identifies the system the station is in. |
| `station_id`          | `BIGINT`                      | NO       |         | Station identifier (combined with `system_id64` for a composite PK). |
| `body`                | `TEXT`                        | YES      |         | Name/description of the body the station orbits, if relevant.        |
| `station_name`        | `TEXT`                        | NO       |         | Name of the station.                                                 |
| `station_type`        | `TEXT`                        | YES      |         | E.g., Coriolis, Outpost, Megaship, etc.                              |
| `primary_economy`     | `TEXT`                        | YES      |         | Main economy type (e.g., Industrial, High Tech).                     |
| `distance_to_arrival` | `DOUBLE PRECISION`            | YES      |         | Distance from the main star in LS (light-seconds).                   |
| `landing_pad_size`    | `TEXT`                        | YES      |         | E.g., Small, Medium, Large, or None.                                 |
| `update_time`         | `TIMESTAMP WITHOUT TIME ZONE` | YES      |         | Last update timestamp.                                               |

**Constraints and Indexes:**

- **Primary Key**: `pk_stations` on `(system_id64, station_id)`
- **Foreign Key**: `fk_stations_system` (on `(system_id64) REFERENCES systems(id64) ON DELETE CASCADE`)
- **Referenced by**: `station_commodities(system_id64, station_id)`

---

## Table: `commodity_types`

**Columns:**

| Column         | Type      | Nullable | Default | Description/Notes                                    |
| -------------- | --------- | -------- | ------- | -------------------------------------------------- |
| `commodity_id` | `INTEGER` | NO       |         | Primary key for commodity mapping.                  |
| `commodity_name`| `TEXT`   | NO       |         | Unique commodity name.                              |

**Constraints and Indexes:**

- **Primary Key**: `commodity_types_pkey` on `(commodity_id)`
- **Unique Constraint**: `commodity_types_commodity_name_key` on `(commodity_name)`
- **Referenced by**: `station_commodities_mapped(commodity_id)`

---

## Table: `station_commodities_mapped`

**Columns:**

| Column          | Type      | Nullable | Default | Description/Notes                                       |
| --------------- | --------- | -------- | ------- | ------------------------------------------------------- |
| `system_id64`   | `BIGINT`  | NO       |         | References `systems(id64)`.                             |
| `station_id`    | `BIGINT`  | NO       |         | References `stations(station_id)` (with `system_id64`). |
| `station_name`  | `TEXT`    | NO       |         | Name of the station.                                    |
| `commodity_id`  | `INTEGER` | NO       |         | References `commodity_types(commodity_id)`.              |
| `sell_price`    | `INTEGER` | YES      |         | Sell price (credits).                                   |
| `demand`        | `INTEGER` | YES      |         | Demand level.                                           |

**Constraints and Indexes:**

- **Primary Key**: `pk_station_commodities` on `(system_id64, station_id, commodity_id)`
- **Index**: `idx_station_commodities_price` on `(commodity_id, sell_price DESC)`
- **Foreign Keys**:
  - `fk_station_commodities_commodity` (on `(commodity_id) REFERENCES commodity_types(commodity_id) ON DELETE CASCADE`)
  - `fk_station_commodities_station` (on `(system_id64, station_id) REFERENCES stations(system_id64, station_id) ON DELETE CASCADE`)
  - `fk_station_commodities_system` (on `(system_id64) REFERENCES systems(id64) ON DELETE CASCADE`)

---

## View: `station_commodities`

**Description:**
A view providing backward compatibility with the original schema, joining the mapped commodities with their names.

**Columns:**

| Column           | Type      | Description/Notes                                       |
| ---------------- | --------- | ------------------------------------------------------- |
| `system_id64`    | `BIGINT`  | References `systems(id64)`.                             |
| `station_id`     | `BIGINT`  | References `stations(station_id)` (with `system_id64`). |
| `station_name`   | `TEXT`    | Name of the station.                                    |
| `commodity_name` | `TEXT`    | Name of the commodity from `commodity_types`.            |
| `sell_price`     | `INTEGER` | Sell price (credits).                                   |
| `demand`         | `INTEGER` | Demand level.                                           |

**Definition:**
```sql
CREATE VIEW station_commodities AS
SELECT 
    sc.system_id64, 
    sc.station_id, 
    sc.station_name, 
    ct.commodity_name, 
    sc.sell_price, 
    sc.demand
FROM station_commodities_mapped sc
JOIN commodity_types ct ON sc.commodity_id = ct.commodity_id;
```
