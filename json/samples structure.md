spansh dump of "galaxy_stations.json" (includes all stations and bodies in a system)

id64: id64-number
name: system name
coords:
	x: x-coordinate
	y: y-coordinate
	z: z-coordinate
allegiance: allegiance
government: government
primaryEconomy: primary Economy Type
secondaryEconomy: secondary Economy Type
security: security Status
population: population number
bodyCount: number of bodies
controllingFaction:
	name: faction name
	government: faction government
	allegiance: faction allegiance
factions:
	0:
		name: faction name
		allegiance: faction allegiance
		government: faction government
		influence: influence number
		state: faction state (boom, expansion, etc)
	1: 
		...
controllingPower: power name
powers:
	0: power name
	1: power name
	...
powerState: powerState
date: YYYY-MM-DD HH:MM:SS+XX
bodies:
	0:
		id64: id64-number
		name: body name
		type: body type
		subType: body sub type
		distanceToArrival: distance in light seconds (ls)
		isLandable: boolean
		reserveLevel: reserve level string
		rings:
			0:
				name: ring name
				type: ring type
				signals:
					signals:
						mineral signal name: signal count
						mineral signal name: signal count
						mineral signal name: signal count
						mineral signal name: signal count
						...
				updateTime: YYYY-MM-DD HH:MM:SS+XX
			1:
				...
		stations: 
			0: (same as stations entry below, on root level)
			1: (same as stations entry below, on root level)
		updateTime: YYYY-MM-DD HH:MM:SS+XX
	1:
		...
stations:
	0:
		name: station name
		id: station id
		updateTime: YYYY-MM-DD HH:MM:SS
		controllingFaction: faction name
		controllingFactionState: faction state
		distanceToArrival: distance in light seconds (ls)
		primaryEconomy: primary Economy Type
		economies: 
			0:
				Economy Type: Percent number
			1:
				Economy Type: Percent number
		services: 
			[ "Service Name", "Service Name", ... ]
		type: station type name	
		landingPads:
			large: number
			medium: number
			small: number
		market: 
				commodities: 
					0:
						name: commodity name
						symbol: commodity symbol
						category: category of commodity
						commodityId: commidity id number
						demand: demand count
						supply: supply count
						buyPrice: buy price
						sellPrice: sell price
					1:
				prohibitedCommodities: [
					"Prohibited Commodity Name", "Prohibited Commodity Name",
					...
				]
				updateTime: YYYY-MM-DD HH:MM:SS+XX
