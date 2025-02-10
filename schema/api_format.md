api_system_sample_entry.json

---
Legend:
key: value (database table->column)
An array of json objects based on input:
id64 (systems->id64) : bigint
name (systems->name) : text
coords(x,y,z) : (systems->x, systems->y, systems->z) : float
radius : int : a radius in which systems should be retrieved
cube x1,y1,z1,x2,y2,z2 : float : a cube in which systems should be retrieved
---
id64: id64-number (systems->id64)    
name: system name (systems->name)
coords:
	x: x-coordinate (systems->x)
	y: y-coordinate (systems->y) 
	z: z-coordinate (systems->z)
population: population number (systems->population)
system state: system state (systems->system_state)
distance from sol: distance in light seconds (systems->distance_from_sol)
controllingPower: power name (systems->controlling_power)
powers:(systems->powers_acquiring:jsonb)
	0: power name (systems->powers_acquiring->0)
	1: power name (systems->powers_acquiring->1)
	...
powerState: powerState (systems->power_state)
bodies:(mineral_signals via unique system_id64 from systems->id64)
	0:
		name: body name (mineral_signals->body_name)
		reserveLevel: reserve level string (mineral_signals->reserve_level)
		rings:
			0:
				name: ring name (mineral_signals->rings_name)
				type: ring type (mineral_signals->rings_type)
				signals:
					signals:
						mineral signal name: count (mineral_signals->mineral_type) :  (mineral_signals->signal_count)
						mineral signal name: signal count (mineral_signals->mineral_type) :  (mineral_signals->signal_count) 
						mineral signal name: signal count (mineral_signals->mineral_type) :  (mineral_signals->signal_count)
						mineral signal name: signal count (mineral_signals->mineral_type) :  (mineral_signals->signal_count)
						...
			1:
                name: ring name (mineral_signals->rings_name)
				...
	1:
        name: body name (mineral_signals->body_name)
		...
stations: (stations via unique system_id64 from systems->id64)
	0:
		name: station name (stations->station_name)
		id: station id (stations->station_id)
		updateTime: YYYY-MM-DD HH:MM:SS (stations->update_time)
		distanceToArrival: distance in light seconds (ls) (stations->distance_to_arrival)
		primaryEconomy: primary Economy Type (stations->primary_economy)
        body: body name (stations->body [optional! can be null])
		type: station type name (stations->station_type)
		landingPads: L, M, S (stations->landing_size)
		market: (station_commodities via unique station_id from stations->station_id or system_id64 and station_name from stations->system_id64+station_name)
				commodities: 
					0:
						name: commodity name (station_commodities->commodity_name)
						demand: demand count (station_commodities->demand)
						sellPrice: sell price (station_commodities->sell_price)
					1:  
                        name: commodity name (station_commodities->commodity_name)
						demand: demand count (station_commodities->demand)
						sellPrice: sell price (station_commodities->sell_price)
						...
