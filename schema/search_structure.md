# Project Merit Miner

This website/app should show you optimal mining systems for the game Elite Dangerous based on your filters to help you make the most merits out of your trade and plan strategic system acquisition/reinforcement/undermining.

## Search Form

| Input Field       | Search form  html id | Data Type        | Value example                                                                                         | database table      | database column           | database format         | Role                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Order of Query |
|-------------------|----------------------|------------------|-------------------------------------------------------------------------------------------------------|---------------------|---------------------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| Reference System  | system               | text             | HR 8514                                                                                               | systems             | name                      | string                  | The name of a Solar System, the origin  point of our search                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 1              |
| Max Distance      | distance             | number           | 200                                                                                                   | systems             | x, y, z                   | integer                 | The radius in which we search systems                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 2              |
| Search Results    | limit                | number           | 50                                                                                                    | -                   | -                         | -                       | Number of search results we want  to see in the html table, one system = one row                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | ?              |
| Your Power        | controlling_power    | text             | Archon Delaine, XXXXX YYYY, Any (all powers), None (no powers)                                        | systems             | controlling_power         | string                  | Filters systems with the name of your power,  the faction you work for. Will be used to  limit the search result for this power only  or all powers (Any) or no powers (NULL, None)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | 3              |
| Power Goal        | power_goal           | text             | Acquisition, Reinforce, Undermine                                                                     | -                   | -                         | -                       | Determines the main kind of search: Acquisition, Reinforce or Undermine                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | 0              |
| Opposing Power    | opposing_power       | text             | Zemina Torval, XXXXX YYYY, Any (all powers),  One, Two, Multiple,  None (no powers)                   | systems             | powers_acquiring          | jsonb array             | Filters all systems with the specified power name from a jsonb array. multiple powers can try to acquire/oppose a system with a controlling power. has to be other power than controlling  power. None is NULL in the database, Any means all powers are valid except NULL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 4              |
| Mineral/Metal     | signal_type          | text             | Any (all mining materials), Monazite, Gold, XXX, YYY,                                                 | station_commodities | commodity_name            | string (id)             | The main mining commodity we search for in the station_commodities table. It is tied to the occurence of mining sources in the same system (reinforce, undermine) or in systems in a radius of 20ly of "Fortified" or 30ly of "Stronghold" systems (acquisition). The rule is the commodity needs to be able to be mined in asteroid belts and sold at stations in the same system.  "Any" is a special case where the highest selling material will be searched in all stations in the system and compared to available mining sources.  The string is mapped to a lookup table in the database, but we use a view to connect the ids and the names of the materials. when we search for the material name,  the database in reality stores this material as 1-50  integers. | 5              |
| Ring Types        | ring_type_filter     | text             | Hotspots (rings with signals), Without hotspots (rings w/o signals), Rocky, Metal Rich, Metallic, Icy | mineral_signals     | mineral_type signal_count | string                  | The ring type determines if we can find this mineral/metal in an asteroid belt in a system. Hotspotd are special places with higher chance of  finding the searched material. "Without Hotspots" means ignoring all Hotspot signals and just showing asteroid rings were we can mine the material in principle. It does not exclude Hotspots, just also shows rings were there are no hotspots for the material, but where we can also mine it.  Then Rocky, Metal Rich, Metallic and Icy all have  different conditions for different materials.   The relations of materials and ring types is in data/mining_materials.json  mineral_type = commodity_name except for "LowTemperatureDiamond" = "Low Temperature Diamonds"  signal_count = Number of signals in the ring   | 7              |
| Reserve Level     | reserve_level        | text             | All (shows all types), Pristine,  Major, XXX, YYY, Unknown                                            | mineral_signals     | reserve_level             | string                  | A string that shows the asteroid ring depletion status, not relevant for core asteroid search, but important for laser mining, better condition = more minerals/metals  found                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | 8              |
| Mining Type       | miningTypeInput      | text  (multiple) | Surface Laser, Surface Deposit,  Subsurface Deposit, Core                                             | -                   | -                         | -                       | The mining type with which we can gather the material from an asteroid.  The relations of materials and mining types is in data/mining_materials.json                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 6              |
| Min. Demand       | minDemand            | number           | 0-90000                                                                                               | station_commodities | demand                    | integer (=> comparison) | Minimum Demand of commodity at certain station.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 9              |
| Max. Demand       | maxDemand            | number           | 0-90000                                                                                               | station_commodities | demand                    | integer (=< comparison) | Maximum Demand of commodity at certain station.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 9              |
| Landing Pad       | landingPadSize       | text             | Any (all sizes), S, M, L                                                                              | stations            | landing_pad_size          | string                  | The biggest landing pad size of this station. S means includes only S. M means includes S & M. L means include S & M & L. Any includes all.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 9              |
| Add mineral/metal | materialsInput       | text  (multiple) | Monazite, Gold, XXX, YYY                                                                              | station_commodities | commodity_name            | string (id)             | There is a column called "Other commodities" that shows other commodities being bought at stations. We can select multiple materials that will then be displayed there.  Up to 5 materials displayed. When "Default" (=None) is  selected we show the top 5 selling materials besides the main commodity search (not showing two times the same commodity).                                                                                                                                                                                                                                                                                                                                                                                                                   | 11             |
| System Sate       | systemStateInput     | text  (multiple) | Any (Default = All), Boom,  Expansion, Outbreak, Blight, etc.                                         | systems             | system_state              | string                  | The state a system is in, Boom or Expansion  mean better prices.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | 12             |


## Database Schema
@schema/database_schema.md


## Search types:

### Reinforce:

Search for systems:
- Power Goal is Reinforce 
- from the origin point of reference system,
- in the radius of the distance filter,
- limit search results to input search results filter: one system per row
- systems controlled by your power or Any, 
- systems with opposing power or Any or None,
- systems where you can mine and sell the selected mineral/metal
- mining type filters applied, 
- ring type filters applied, 
- reserve level filters applied, 
- demand filters applied,
- landing pad filters applied,
- system state filters applied,
- other mineral/metal commodity filters applied

Search result table:
System | DST (Distance) | Ring Details | Stations | State | Power

Ordered by: Selling Price at stations, then Distance

Search result row:

- System:   
Suya Copy

- Distance:
61 Ly	

- Ring Details:
Planet2 A Ring:  Monazite: 1 Hotspot (Major)
Planet3 A Ring:  Monazite: 1 Hotspot (Major)
Planet6 A Ring:  Monazite: 3 Hotspots (Major)
Show all signals

- Station:
Rutherford Port (L)
Price: 770.777 CR +++++
Demand:  2.295
Distance: 402 Ls
Updated: 21h 28m ago

Other Commodities:
MON 770.777 CR +++++ 2.295 Demand
MUS 436.530 CR +++ 813 Demand
LTD 324.333 CR +++++ 1.155 Demand
GRA 297.957 CR + 813 Demand
ALE 295.427 CR + 813 Demand
RHO 282.053 CR ++ 1.286 Demand

- State:
Exploited	

- Power:
Archon Delaine

The basic cells of the table columns are displayed the same way for all search types, but combined in different ways.
Acquisition has a different station display and css. Res Data and High Platinum have a different table column order.

### Undermine:

Search for systems:
- Power Goal is Undermine 
- from the origin point of reference system,
- in the radius of the distance filter,
- limit search results to input search results filter: one system per row
- systems NOT controlled by your power, 
- systems with opposing power as controlling power,
- systems where you can mine and sell the selected mineral/metal
- mining type filters applied, 
- ring type filters applied, 
- reserve level filters applied, 
- demand filters applied,
- landing pad filters applied,
- system state filters applied,
- other mineral/metal commodity filters applied

Ordered by: Selling Price at stations, then Distance


### Acquisition:

Search for systems:
- Power Goal is Acquisition 

Table for Unoccupied systems (single system where we sell the material):
- from the origin point of reference system,
- search in radius of distance filter
- systems NOT controlled by Any power, power state: "NULL", "Prepared", "InPrepareRadius" = "Unoccupied"
- radius calculation: 20ly of "Fortified" or 30ly of "Stronghold" systems, controlled by your power
- systems where you can sell the selected mineral/metal
- demand filters applied,
- landing pad filters applied,
- system state filters applied,
- other mineral/metal commodity filters applied
- limit search results to input search results filter: one system per row
- single result on left side of table

Table for Mining Systems (multiple systems where we mine the material):
- systems controlled by your power,
 - 20ly of "Fortified" or 30ly of "Stronghold" distance to Occupied systems
- systems where you can mine the selected mineral/metal
- mining type filters applied,
- ring type filters applied, 
- reserve level filters applied, 
- multiple results on right side of table, in the same table as the unoccupied system, 
  mining systems are connected to the unoccupied system visually

Search result table:
Target System |	State | DST | Stations | ─┰─ | Mining System | Ring Details | State | Power
---------------------------------------|  ├─ | Mining System | Ring Details | State | Power
---------------------------------------|  └─ | Mining System | Ring Details | State | Power

Ordered by: Selling Price at stations, then Distance

### Best Prices / Highest Prices:

Search for systems:
- Power Goal is Reinforce/Undermine/Acquisition 
- from the origin point of reference system,
- in the radius of the distance filter,
- limit search results to input search results filter: one system per row

Fork here:
  A. Reinforce:
    - systems controlled by your power or Any, 
    - systems with opposing power or Any or None,
    - systems where you can mine and sell the selected mineral/metal
  B. Undermine:
    - systems NOT controlled by your power, 
    - systems with your power as opposing power,
    - systems where you can mine and sell the selected mineral/metal
  C. Acquisition:
    Systems to sell:
      - Unoccupied systems in radius of 20ly of "Fortified" or 30ly of "Stronghold" systems, controlled by your power
      - systems where you can sell the selected mineral/metal
    Systems to mine:
      - systems controlled by your power,
      - 20ly of "Fortified" or 30ly of "Stronghold" distance to Occupied systems
      - systems where you can mine the selected mineral/metal
Fork ends:

- systems where you can mine and sell the selected mineral/metal (ignored by Acquisition, because already in C.)
- mining type filters applied, (Acquisition: Only Systems to mine)
- ring type filters applied, (Acquisition: Only Systems to mine)
- reserve level filters applied, (Acquisition: Only Systems to mine)
- demand filters applied, (Acquisition: Only Systems to sell)
- landing pad filters applied, (Acquisition: Only Systems to sell)
- system state filters applied, (Acquisition: Only Systems to sell)
- other mineral/metal commodity filters applied (Acquisition: Only Systems to sell)

Search result table Reinforce/Undermine:
Material | Price | Demand | System | Station | Pad Size | Station Distance | Reserve Level | Power | Power State | Last Update

Search result table Acquisition:
Mineral/Metal | Price | Demand | System Buying | State | Station | Pad Size | Station Distance | System Mine | Ring Details | Reserve Level | Power | Last Update
If Hotspot:
Monazite | 770.777 CR +++++ | 2.295 | Morgoth | Exploited | Rutherford Port (L) | L | 402 Ls | Barator | 2 A Ring: 1 Hotspot | Major | Archon Delaine | 21h 28m ago
If No Hotspot:
Monazite | 770.777 CR +++++ | 2.295 | Morgoth | Exploited | Rutherford Port (L) | L | 402 Ls | Barator | 2 A Ring: Metallic | Major | Archon Delaine | 21h 28m ago

Ordered by: Selling Price at stations, then Distance


### Res Data:

Search for systems:
- Power Goal is DISABLED 
- system list is loaded from a csv file: data/plat-hs-and-res-maps.csv

- only systems in list are searched (get id64 via "System" = "system_name" in csv file)
- from the origin point of reference system,
- DISABLE radius filter - should include all systems in the csv file
- limit search results to input search results filter: one system per row
- systems controlled by your power or Any, 
- systems with opposing power or Any or None,
- landing pad filters applied,
- system state filters applied,
- other mineral/metal commodity filters applied

Search result table:
System | Power | DST | Ring Details | Ls (Station Distance) | RES Zone | Stations

Ordered by: Distance


### High Platinum Hotspots:

Search for systems:
- Power Goal is DISABLED 
- system list is loaded from a csv file: data/plat-high-yield-hotspots.csv
- only systems in list are searched (get id64 via "System" = "system_name" in csv file)
- from the origin point of reference system,
- DISABLE radius filter - should include all systems in the csv file
- limit search results to input search results filter: one system per row
- systems controlled by your power or Any, 
- systems with opposing power or Any or None,
- landing pad filters applied,
- system state filters applied,
- other mineral/metal commodity filters applied, always include Platinum in the "Other Commodities" station column

Search result table:
System | Power | DST | Ring Details | Ls (Station Distance) | RES Zone | Stations

Ordered by: Distance


# Files

## Main Project Files
- @server.py creates a flask app that serves the website either in local (dev) or production mode
- @gunicorn_config.py is the config for the production webserver
- server.py uses @common.py and @mining_data.py for importing functions
- the website @index.html is served and offers a search form that filters out results from a postgres database and display it in a html table via javascript
- depending on the input field selection we execute different search functions:
    - @search_reinforce.py default search to reinforce systems
    - @search_acquisition.py search to find systems to aquire
    - @search_undermine.py search to find systems to undermine
    - @res_data.py search for res hotspots and high platinum hotspots
- the sql query for the search functions is getting build together from:
    @search_queries.py
    @search_common.py
    @search_power.py

### Javascript functions
- @autocomplete.js holds autocomplete functions for the searchform
- @menu.js controls the switching of selected menu points and html templates
- @search_format.js includes formatting functions for the results displayed in the html table
- @search_special.js includes additional search functions like res spots and high platinum spots
- @storage.js saves the input fields to local storage
- @systemWorker.js worker helper function for loading systems from our database for the 3d map
- @websocket.js manages the communication between update_live.py and update_daemon.py

### Update Scripts
- @update_live.py is the script that updates the database with journal messages received from the EDDN network (player data)
- @update_daemon.py checks the production web server for a running "update_live.py" process and if it does not find one (via ZMQ websocket messages) it starts a new process

- @map.html is an experimental 3d map of the games systems

### Helper Scripts
- @converter.py creates the base data for our database from json/galaxy_stations_MM_DD.json (different timestamps) to sqlite
- @migrate_to_postgres.py converts sqlite to postgres database
- @export_db.py exports the current postgres database to a sql dump
- @dump_powerplay.py creates the system information file used by @map.html to generate points of systems in the 3d map

## Folders
- backup: ignore those files except when we need to look for older working files when we encounter sever errors
- cache: probably created by some function of the 3d map
- css: css for the website
- data: json and csv files containing mining infos
- docs: documentation from 3rd party frameworks we use. for you to lookup functions you don't know
    - eddn: network we receive player data from
    - examples: three js documentation of marching ray cubes
    - threejs: threejs documentation
    - timescaledb: timescaledb documentatio
- dumps: sql dumps
- fonts: fonts for website
- img: pictures and icons for the website
- js: javascript for the website
- json: source data for database and json samples to show the structure of json entries
- schema: database schema and source data schema
- scripts: python script that generate the sqlite and postgres data from the input json files
- templates: html templates for different sub-pages/sections of the site
- timescaledb: 
- utils: python search helpers and common functions