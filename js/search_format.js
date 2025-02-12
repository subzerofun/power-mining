// Functions for formatting and displaying data in the mining search interface

// Client-side price data loaded from current_prices.csv
let PRICE_DATA = null;

// Commodity code to full name mapping
const COMMODITY_MAP = {
    "ALU": "Aluminium",
    "BER": "Beryllium",
    "BIS": "Bismuth",
    "BAU": "Bauxite",
    "BRT": "Bertrandite",
    "COB": "Cobalt",
    "CLT": "Coltan",
    "CRY": "Cryolite",
    "COP": "Copper",
    "GAL": "Gallite",
    "GLM": "Gallium",
    "GLD": "Gold",
    "GOS": "Goslarite",
    "HAF": "Hafnium 178",
    "IND": "Indium",
    "IDT": "Indite",
    "JAD": "Jadeite",
    "LAN": "Lanthanum",
    "LEP": "Lepidolite",
    "LIT": "Lithium",
    "LHY": "Lithium Hydroxide",
    "MNL": "Methanol Monohydrate Crystals",
    "MCL": "Methane Clathrate",
    "MOI": "Moissanite",
    "OSM": "Osmium",
    "PAL": "Palladium",
    "PLA": "Platinum",
    "RUT": "Rutile",
    "VOP": "Void Opal",
    "LTD": "Low Temperature Diamonds",
    "PAI": "Painite",
    "MUS": "Musgravite",
    "GRA": "Grandidierite",
    "MON": "Monazite",
    "ALE": "Alexandrite",
    "BEN": "Benitoite",
    "RHO": "Rhodplumsite",
    "SER": "Serendibite",
    "BRO": "Bromellite"
};

// Load price data on script load
fetch('/data/current_prices.csv')
    .then(response => response.text())
    .then(csv => {
        PRICE_DATA = {};
        // Skip header row and process each line
        const lines = csv.split('\n').slice(1);
        lines.forEach(line => {
            if (!line.trim()) return;
            const [material, type, avgPrice, minPrice, maxPrice] = line.split(',');
            PRICE_DATA[material] = {
                avg_price: parseInt(avgPrice),
                max_price: parseInt(maxPrice)
            };
        });
        console.log('Loaded price data:', PRICE_DATA);
    })
    .catch(err => console.error('Failed to load price data:', err));

function getStationIcon(stationType, onlyIcon = false) {
    const iconMap = {
        'Coriolis Starport': 'Coriolis_sm.svg',
        'Orbis Starport': 'Orbis_sm.svg',
        'Ocellus Starport': 'Ocellus_sm.svg',
        'Asteroid base': 'Asteroid_Station.svg',
        'Outpost': 'Outpost_sm.svg',
        'Surface Port': 'surface_port_sm.svg',
        'Planetary Outpost': 'surface_port_sm.svg',
        'Settlement': 'settlement_sm.svg'
    };
    const icon = iconMap[stationType] || 'Outpost_sm.svg';
    
    if (onlyIcon) {
        return `/img/icons/${icon}`;
    }
    
    return `<img src="/img/icons/${icon}" alt="${stationType}" class="station-icon">`;
}

function getPriceComparison(currentPrice, referencePrice, showIndicators = true) {
    if (currentPrice === 0 || referencePrice === 0) {
        console.log('Zero price detected:', { currentPrice, referencePrice });
        return { color: null, indicator: '' };
    }
    
    const percentage = (currentPrice / referencePrice - 1) * 100;
    
    if (percentage >= 125) {
        return { color: '#f0ff00', indicator: showIndicators ? '     +++++' : '' };
    } else if (percentage >= 100) {
        return { color: '#fff000', indicator: showIndicators ? '     ++++' : '' };
    } else if (percentage >= 75) {
        return { color: '#ffcc00', indicator: showIndicators ? '     +++' : '' };
    } else if (percentage >= 50) {
        return { color: '#ff9600', indicator: showIndicators ? '     ++' : '' };
    } else if (percentage >= 25) {
        return { color: '#ff7e00', indicator: showIndicators ? '     +' : '' };
    } else if (percentage >= -5) {
        return { color: null, indicator: '' };
    } else if (percentage >= -25) {
        return { color: '#ff2a00', indicator: showIndicators ? '     -' : '' };
    } else if (percentage >= -50) {
        return { color: '#af0019', indicator: showIndicators ? '     --' : '' };
    } else {
        return { color: '#af0019', indicator: showIndicators ? '     ---' : '' };
    }
}

async function formatPrices(items, useMaxPrice = false) {
    if (!PRICE_DATA) {
        console.warn('Price data not loaded yet');
        return items.map(() => ({ color: null, indicator: '' }));
    }

    return items.map(item => {
        let commodity = item.commodity;
        
        // If it's a 3-letter code, convert to full name
        if (commodity && commodity.length === 3 && COMMODITY_MAP[commodity]) {
            commodity = COMMODITY_MAP[commodity];
        }
        
        if (!commodity || !(commodity in PRICE_DATA)) {
            console.log('No price data for:', item.commodity, 'mapped to:', commodity);
            return { color: null, indicator: '' };
        }
        
        const referencePrice = PRICE_DATA[commodity][useMaxPrice ? 'max_price' : 'avg_price'];
        /*console.log('Price data for', commodity, ':', {
            currentPrice: item.price,
            referencePrice,
            useMaxPrice
        });*/
        const result = getPriceComparison(item.price, referencePrice);
        return result;
    });
}

function formatPriceSpan(price, data) {
    const formattedPrice = price.toLocaleString();
    const priceSpan = document.createElement('span');
    if (data && typeof data === 'object') {
        if (data.color) {
            priceSpan.style.color = data.color;
        }
        priceSpan.textContent = formattedPrice + ' CR' + (data.indicator || '');
    } else {
        priceSpan.textContent = formattedPrice + ' CR';
    }
    return priceSpan;
}

function formatNumber(number) {
    return Math.floor(number).toLocaleString();
}

function formatUpdateTime(updateTimeStr) {
    if (!updateTimeStr) return '';

    // Parse the input time string and ensure UTC
    let dateTime;
    if (updateTimeStr.includes('T')) {
        // Handle ISO format
        dateTime = new Date(updateTimeStr + (updateTimeStr.endsWith('Z') ? '' : 'Z'));
    } else {
        // Handle "YYYY-MM-DD HH:mm:ss" format
        dateTime = new Date(updateTimeStr.replace(' ', 'T') + 'Z');
    }

    // Get current time in UTC
    const now = new Date();

    Date.prototype.deleteHours= function(h){
        this.setHours(this.getHours()-h);
        return this;
    }

    dateTime.deleteHours(1);

    const nowUTC = new Date(now.getTime() + now.getTimezoneOffset() * 60000);

    // Calculate time difference in minutes
    const diffMinutes = Math.floor((nowUTC - dateTime) / (1000 * 60));

    // Format based on time difference
    if (diffMinutes < 60) {
        return `Updated: ${diffMinutes} min ago`;
    } else if (diffMinutes < 24 * 60) {
        const hours = Math.floor(diffMinutes / 60);
        const mins = diffMinutes % 60;
        return `Updated: ${hours}h ${mins}m ago`;
    } else if (diffMinutes < 48 * 60) {
        const hours = Math.floor(diffMinutes / 60);
        return `Updated: ${hours} hours ago`;
    } else {
        const days = Math.floor(diffMinutes / (24 * 60));
        if (days >= 365) {
            const years = Math.floor(days / 365);
            const remainingDays = days % 365;
            return `Updated: ${years}y, ${remainingDays} days ago`;
        }
        return `Updated: ${days} days ago`;
    }
}

function getCommodityCode(name) {
    const codeMap = {
        'Aluminium': 'ALU',
        'Beryllium': 'BER',
        'Bismuth': 'BIS',
        'Bauxite': 'BAU',
        'Bertrandite': 'BRT',
        'Cobalt': 'COB',
        'Coltan': 'CLT',
        'Cryolite': 'CRY',
        'Copper': 'COP',
        'Gallite': 'GAL',
        'Gallium': 'GLM',
        'Gold': 'GLD',
        'Goslarite': 'GOS',
        'Hafnium 178': 'HAF',
        'Indium': 'IND',
        'Indite': 'IDT',
        'Jadeite': 'JAD',
        'Lanthanum': 'LAN',
        'Lepidolite': 'LEP',
        'Lithium': 'LIT',
        'Lithium Hydroxide': 'LHY',
        'Methanol Monohydrate Crystals': 'MNL',
        'Methane Clathrate': 'MCL',
        'Moissanite': 'MOI',
        'Osmium': 'OSM',
        'Palladium': 'PAL',
        'Platinum': 'PLA',
        'Praseodymium': 'PRA',
        'Pyrophyllite': 'PYR',
        'Rutile': 'RUT',
        'Samarium': 'SAM',
        'Silver': 'SIL',
        'Taaffeite': 'TAF',
        'Tantalum': 'TAN',
        'Thallium': 'THL',
        'Thorium': 'THR',
        'Titanium': 'TIT',
        'Uranium': 'URN',
        'Uraninite': 'URT',
        'Void Opal': 'VOP',
        'Low Temperature Diamonds': 'LTD',
        'LowTemperatureDiamond': 'LTD',
        'Monazite': 'MON',
        'Musgravite': 'MUS',
        'Grandidierite': 'GRA',
        'Alexandrite': 'ALE',
        'Benitoite': 'BEN',
        'Rhodplumsite': 'RHO',
        'Serendibite': 'SER',
        'Bromellite': 'BRO'
    };
    return codeMap[name] || name.substring(0, 3).toUpperCase();
}

function toggleAllSignals(button, allSignals, systemName) {
    const signalList = button.previousElementSibling;
    const isShowingAll = button.textContent === 'Show less';
    
    if (!isShowingAll) {
        // Store the original HTML before showing all signals
        button.dataset.originalHtml = signalList.innerHTML;
        
        // Get the current ring type filter and selected commodity
        const ringTypeFilter = document.getElementById('ring_type_filter').value;
        const selectedCommodity = document.getElementById('signal_type').value;
        
        //console.log('Ring Type Filter:', ringTypeFilter);
        //console.log('All Signals:', allSignals);
        
        // Filter signals based on ring type filter
        const filteredSignals = allSignals.filter(signal => {
            // First filter by ring type if specific type selected
            if (ringTypeFilter !== 'All' && ringTypeFilter !== 'Hotspots' && ringTypeFilter !== 'Without Hotspots') {
                if (signal.ring_type !== ringTypeFilter) return false;
            }
            
            // Then filter by hotspot/non-hotspot status
            if (ringTypeFilter === 'Hotspots') {
                return signal.mineral_type !== null;
            } else if (ringTypeFilter === 'Without Hotspots') {
                return signal.mineral_type === null;
            }
            
            // For 'All', show everything
            return true;
        });
        
        console.log('Filtered Signals:', filteredSignals);
        
        // Group signals by ring name
        const signalsByRing = {};
        filteredSignals.forEach(signal => {
            // Format ring name - remove system name if it appears at the start
            let displayRingName = signal.ring_name;
            if (displayRingName.startsWith(systemName)) {
                displayRingName = displayRingName.slice(systemName.length).trim();
            }
            
            if (!signalsByRing[displayRingName]) {
                signalsByRing[displayRingName] = {
                    ring_type: signal.ring_type,
                    reserve_level: signal.reserve_level,
                    hotspots: [],
                    non_hotspot: null
                };
            }
            
            if (signal.mineral_type) {
                signalsByRing[displayRingName].hotspots.push(signal);
            } else {
                signalsByRing[displayRingName].non_hotspot = signal;
            }
        });
        
        //console.log('Signals By Ring:', signalsByRing);
        
        // Create HTML for all signals, grouped by ring
        const allSignalsHtml = `<ul class="signal-list">
            ${Object.entries(signalsByRing)
                .sort(([a], [b]) => a.localeCompare(b))  // Sort by ring name
                .map(([ringName, data], ringIndex) => {
                    const lines = [];
                    const planetIcon = '<img src="/img/icons/ringed-planet-2.svg" class="planet-icon" alt="Planet">';
                    const iconSpace = '<span class="planet-icon-space"></span>';
                    
                    let mineral_name = selectedCommodity;
                    if(mineral_name === "Low Temperature Diamonds") { mineral_name = "Low T. Diamonds" } 
                    if(mineral_name === "Any") { mineral_name = "Multiple Minerals/Metals" } 

                    // Add non-hotspot entry if it exists and we're not in 'Hotspots' mode
                    if (data.non_hotspot && ringTypeFilter !== 'Hotspots') {
                        //console.log('Adding non-hotspot for ring:', ringName);
                        lines.push(`${planetIcon}${ringName} <img src="/img/icons/rings/${data.ring_type.toLowerCase()}.png" width="16" height="16" class="ring-type-icon" alt="${data.ring_type}" title="Ring Type: ${data.ring_type}" style="vertical-align: middle;"> ${getReserveIcon(data.reserve_level,14,13,'#f5730d')} : ${mineral_name}`);
                    }
                    

                    // Add hotspot entries if we're not in 'Without Hotspots' mode
                    if (ringTypeFilter !== 'Without Hotspots') {
                        //console.log('Adding hotspots for ring:', ringName, data.hotspots);
                        data.hotspots.forEach((hotspot, index) => {
                            const icon = lines.length === 0 ? planetIcon : iconSpace;
                            const hotspotText = hotspot.signal_count === 1 ? "Hotspot" : "Hotspots";
                            mineral_name = hotspot.mineral_type;
                            if(mineral_name === "Low Temperature Diamonds") { mineral_name = "Low T. Diamonds" } 

                            lines.push(`${icon}${ringName} <img src="/img/icons/rings/${data.ring_type.toLowerCase()}.png" width="16" height="16" class="ring-type-icon" alt="${data.ring_type}" title="Ring Type: ${data.ring_type}" style="vertical-align: middle;"> ${getReserveIcon(data.reserve_level,14,13,'#f5730d')} : <img src='img/icons/hotspot-systemview.svg' width='13' height='13' class='hotspot-icon'> ${mineral_name}: ${hotspot.signal_count} ${hotspotText}`);
                        });
                    }
                    
                    return lines.map(line => `<li>${line}</li>`).join('');
                }).join('')}
        </ul>`;
        
        signalList.innerHTML = allSignalsHtml;
        button.textContent = 'Show less';
    } else {
        // Restore original HTML
        signalList.innerHTML = button.dataset.originalHtml;
        button.textContent = 'Show all signals';
    }
}

function getDemandIcon(demand, isOtherCommodity = false) {
    let iconId = 'demand-none';
    let color = '#ff0000';  // red for none/low
    
    if (demand > 2000) {
        iconId = 'demand-veryhigh';
        color = '#ffff00';  // yellow
    }
    else if (demand > 1000) {
        iconId = 'demand-high';
        color = '#00ff00';  // lime
    }
    else if (demand > 300) {
        iconId = 'demand-medium';
        color = '#00ff00';  // lime
    }
    else if (demand > 100) {
        iconId = 'demand-low';
        color = '#ff0000';  // red
    }
    
    const height = isOtherCommodity ? '8' : '12';
    return `<svg class="demand-icon" width="13" height="${height}" style="margin-right: 2px; color: ${color}"><use href="img/icons/demand.svg#${iconId}"></use></svg>`;
}

function getReserveIcon(reserveLevel, width = 17, height = 16, color='white') {
    let iconId = 'reserve-level-unknwon';
    let colorRanked = '#ff0000';
    
    switch(reserveLevel) {
        case 'Pristine':
            iconId = 'reserve-level-pristine';
            colorRanked = '#00ff00';  // yellow
            break;
        case 'Major':
            iconId = 'reserve-level-major';
            colorRanked = '#00ff00';  // lime
            break;
        case 'Common':
            iconId = 'reserve-level-common';
            colorRanked = '#ffff00';  // lime
            break;
        case 'Low':
            iconId = 'reserve-level-low';
            colorRanked = '#aaff00';  // red
            break;
        case 'Depleted':
            iconId = 'reserve-level-depleted';
            colorRanked = '#ff0000';  // red
            break;
        case 'Unknown':
            iconId = 'reserve-level-depleted';
            colorRanked = '#ff0000';  // red
            break;
        // Depleted and Unknown use default values
    }
    
    return `<svg class="reserve-level-icon" width="${width}" height="${height}" style="margin-right: 2px; color: ${color}"><title>Reserve Level: ${reserveLevel}</title><use href="img/icons/reserve-level.svg#${iconId}"></use></svg>`;
}

function showAllSignals(system, showPopup) {
    const signalList = document.createElement('ul');
    signalList.className = 'signal-list';
    
    system.all_signals.forEach(signal => {
        const li = document.createElement('li');
        li.innerHTML = `<img src="/img/icons/planet.svg" width="11" height="11"> ${signal.ring_name}: ${signal.signal_text}`;
        signalList.appendChild(li);
    });
    
    const title = `All signals in ${system.name}`;
    showPopup(title, signalList);
}

function formatStations(stations) {
    if (!stations || stations.length === 0) return '';
    
    return stations.map(station => `
        <div class="station-entry">
            <div class="station-name">
                ${getStationIcon(station.station_type)}
                ${station.name}
            </div>
            <div class="station-info">
                ${station.pad_size} pad, ${formatNumber(station.distance)}Ls
                ${station.sell_price ? `<br>Sell: ${formatNumber(station.sell_price)} cr` : ''}
                ${station.demand ? `<br>Demand: ${formatNumber(station.demand)}` : ''}
            </div>
            ${station.update_time ? 
                `<div class="update-time">Updated: ${formatUpdateTime(station.update_time)}</div>` : 
                ''}
        </div>
    `).join('');
}

function formatAcquisitionStation(station, searchedCommodity, selectedMaterials = ['Default']) {
    // Debug logging
    //console.log('Formatting station:', station);
    //console.log('Searched commodity:', searchedCommodity);
    //console.log('Selected materials:', selectedMaterials);

    if (!station) return '';
    
    // Normalize the searched commodity name for comparison
    const normalizedSearchCommodity = searchedCommodity.toLowerCase().replace(/\s+/g, '');
    
    return `
        <div class="station-entry">
            <div class="station-main">
                ${getStationIcon(station.station_type)}${station.name} (${station.pad_size})
                <div class="station-details">Distance: ${formatNumber(station.distance)} Ls</div>
                <div class="station-updated">${formatUpdateTime(station.update_time)}</div>
            </div>
            ${station.sell_price || station.other_commodities.length > 0 ? `
                <div class="other-commodities">
                    <div class="other-commodities-list">
                        ${station.sell_price ? `
                            <div class="commodity-item">
                                <span class="commodity-code">${getCommodityCode(station.commodity_name)}</span>
                                ${formatPriceSpan(station.sell_price, station.priceData).outerHTML}
                                <div class="demand-block">
                                    ${getDemandIcon(station.demand)}
                                    <span class="demand">${formatNumber(station.demand)} Demand</span>
                                </div>
                            </div>
                        ` : ''}
                        ${station.other_commodities
                            .filter(commodity => {
                                if (!selectedMaterials.includes('Default')) {
                                    const code = getCommodityCode(commodity.name);
                                    if (!selectedMaterials.includes(code)) return false;
                                }
                                // Case-insensitive comparison after normalizing both strings
                                const normalizedCommodity = commodity.name.toLowerCase().replace(/\s+/g, '');
                                return normalizedCommodity !== normalizedSearchCommodity;
                            })
                            .sort((a, b) => b.sell_price - a.sell_price)
                            .map(commodity => `
                                <div class="commodity-item">
                                    <span class="commodity-code">${getCommodityCode(commodity.name)}</span>
                                    ${formatPriceSpan(commodity.sell_price, commodity.priceData).outerHTML}
                                    <div class="demand-block">
                                        ${getDemandIcon(commodity.demand, true)}
                                        <span class="demand">${formatNumber(commodity.demand)} Demand</span>
                                    </div>
                                </div>
                            `).join('')}
                    </div>
                </div>
            ` : ''}
        </div>
    `;
}
const POWER_COLORS = {
    'Aisling Duval': '#0099ff',
    'Edmund Mahon': '#019c00',
    'A. Lavigny-Duval': '#7f00ff',
    'Nakato Kaine': '#a3f127',
    'Felicia Winters': '#ffc400',
    'Denton Patreus': '#00ffff',
    'Jerome Archer': '#df1de4',
    'Zemina Torval': '#0040ff',
    'Pranav Antal': '#ffff00',
    'Li Yong-Rui': '#33d688',
    'Archon Delaine': '#ff0000',
    'Yuri Grom': '#ff8000'
};

function getPowerStateIcon(powerState, controllingPower, size = 11) {
    if (!powerState) return '';
    
    // Default color for uncontrolled states
    let color = '#DDD';
    
    // Special case for Stronghold with carrier
    let symbolId = powerState;
    /*if (powerState === 'Stronghold' && controllingPower) {
        symbolId = 'Stronghold-Carrier';
    }*/
    
    // Set color based on controlling power for controlled states
    if (controllingPower && ['Stronghold', 'Fortified', 'Reinforced', 'Exploited'].includes(powerState)) {
        color = POWER_COLORS[controllingPower] || '#DDD';
    }
    
    return `<span class="power-state-tag"><svg class="power-state-icon" width="${size}" height="${size}" style="color: ${color}"><use href="img/icons/power-state-icons.svg#${symbolId}"></use></svg></span>`;
}

function formatSystemState(powerState, controllingPower) {
    if (!powerState) return '';
    return `
        <div class="system-state">
            ${getPowerStateIcon(powerState, controllingPower)}${powerState || 'Unoccupied'}
        </div>`;
}

function formatControllingPower(controllingPower) {
    if (!controllingPower) return '-';
    return `
        <div class="power-info">
            <div>
                <span style="display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: ${POWER_COLORS[controllingPower] || '#DDD'}; margin-right: 4px;"></span>
                <span class="controlling-power" style="color: #ffff00">${controllingPower}</span>
            </div>
        </div>`;
}

function formatPowerInfo(controllingPower, powerState, powersAcquiring = []) {
    let html = formatControllingPower(controllingPower);
    
    if (powersAcquiring && powersAcquiring.length > 0) {
        html += `<div class="undermining-power">
            ${powersAcquiring.map(power => `
                <div>
                    <span style="display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: ${POWER_COLORS[power] || '#DDD'}; margin-right: 4px;"></span>
                    <span style="color: #ff8000">${power}</span>
                </div>
            `).join('')}
        </div>`;
    }
    
    return html;
}

function formatSystemName(systemName) {
    if (!systemName) return '-';
    return `
        <div class="system-name">
            ${systemName}
            <img src="/img/icons/copy.svg" 
                 class="copy-icon" 
                 width="12" 
                 height="12" 
                 alt="Copy" 
                 onclick="navigator.clipboard.writeText('${systemName}').then(() => this.classList.add('copied'))"
                 onanimationend="this.classList.remove('copied')"
                 title="Copy system name">
        </div>`;
}

function shortenStationName(name) {
    if (name.length > 20) {
        return name.substring(0, 18) + '...';
    }
    return name;
}

function filterAndSortStations(stations, priorityMaterials = ['Platinum', 'Painite', 'Osmium', 'Low Temperature Diamonds']) {
    // Filter stations to only include those with priority materials
    const filteredStations = stations.filter(station => {
        if (!station.other_commodities) return false;
        return station.other_commodities.some(commodity => 
            priorityMaterials.includes(commodity.name)
        );
    });

    // Sort stations by best price of any priority material
    filteredStations.sort((a, b) => {
        const aMaxPrice = Math.max(...a.other_commodities
            .filter(c => priorityMaterials.includes(c.name))
            .map(c => c.sell_price));
        const bMaxPrice = Math.max(...b.other_commodities
            .filter(c => priorityMaterials.includes(c.name))
            .map(c => c.sell_price));
        return bMaxPrice - aMaxPrice;
    });

    // Return top 5 stations
    return filteredStations.slice(0, 5);
}

async function formatCommodityPrice(price, commodityName, useMaxPrice) {
    const priceData = await formatPrices([{ price, commodity: commodityName }], useMaxPrice);
    return {
        color: priceData[0]?.color,
        indicator: priceData[0]?.indicator || ''
    };
}

function getSystemState(systemState) {
    console.log('getSystemStateIcons called with:', systemState);
    if (!systemState) return '';
    
    switch(systemState) {
        case 'Boom':
            console.log('Rendering Boom icon');
            return '<span class="system-state-span"><img src="/img/icons/boom.svg" alt="Boom" width="10" height="10">Boom</span>';
        case 'Expansion':
            console.log('Rendering Expansion icon');
            return '<span class="system-state-span"><img src="/img/icons/expansion.svg" alt="Expansion" width="10" height="10">Expansion</span>';
        default:
            console.log('No matching state found');
            return '';
    }
}

function getSystemStateColor(state) {
    // Return both icon HTML and color based on state
    const stateColors = {
        // Level -2 (Strongly Negative) - Red
        'War': '#b20101',
        'Civil War': '#b20101',
        'Lockdown': '#b20101',
        'Outbreak': '#b20101',
        'Famine': '#b20101',
        'Incursion': '#b20101',
        'Terrorist Attack': '#b20101',
        'Natural Disaster': '#b20101',
        'Blight': '#b20101',
        'Pandemic': '#b20101',

        // Level -1 (Negative) - Yellow
        'Bust': '#d9b909',
        'Civil Unrest': '#d9b909',
        'Pirate Attack': '#d9b909',
        'Retreat': '#d9b909',
        'Drought': '#d9b909',
        'Infrastructure Failure': '#d9b909',

        // Level 0 (Neutral) - Gray
        'None': '#bbafa6',
        'Election': '#bbafa6',

        // Level +1 (Positive) - Cyan
        'Civil Liberty': '#00bfff',
        'Expansion': '#00bfff',
        'Boom': '#00bfff',
        'Investment': '#00bfff',
        'Public Holiday': '#00bfff'
    };

    // Convert state name to icon filename format
    const getIconName = (state) => {
        if (!state) return '';
        return state.toLowerCase().replace(/ /g, '-');
    };

    // Try to create icon HTML if state exists
    let iconHtml = '';
    if (state) {
        const iconName = getIconName(state);
        // Create a temporary image element to check if icon exists
        const img = new Image();
        img.src = `img/icons/system-state/${iconName}.svg`;
        if (img.complete) {
            iconHtml = `<img src="img/icons/system-state/${iconName}.svg" class="state-icon" alt="${state}">`;
        }
    }

    return {
        color: stateColors[state] || '#bbafa6', // Default to neutral gray if state not found
        iconHtml: iconHtml
    };
}

// Add new formatCommodities function for system info view
function formatCommodities(commodities) {
    if (!commodities || commodities.length === 0) {
        return '<p>No market data available</p>';
    }
    
    return commodities.map(commodity => {
        const priceInfo = getPriceComparison(commodity.sellPrice, commodity.avgPrice || 0, false);
        const priceStyle = priceInfo.color ? `color: ${priceInfo.color}` : '';
        
        return `
            <div class="commodity-row">
                <span class="commodity-name">${commodity.name}</span>
                <span class="commodity-demand">${commodity.demand.toLocaleString()}</span>
                <span class="commodity-price" style="${priceStyle}">${commodity.sellPrice.toLocaleString()} CR</span>
            </div>
        `;
    }).join('');
}

// Export all functions
export {
    getStationIcon,
    getPriceComparison,
    formatPrices,
    formatPriceSpan,
    formatNumber,
    formatUpdateTime,
    getCommodityCode,
    toggleAllSignals,
    getDemandIcon,
    getReserveIcon,
    showAllSignals,
    formatStations,
    formatAcquisitionStation,
    getPowerStateIcon,
    formatSystemState,
    formatControllingPower,
    formatPowerInfo,
    formatSystemName,
    shortenStationName,
    filterAndSortStations,
    formatCommodityPrice,
    getSystemState,
    getSystemStateColor,
    formatCommodities,
    POWER_COLORS
}; 
