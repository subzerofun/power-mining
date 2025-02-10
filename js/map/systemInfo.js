import { getSystemInfo, formatSystemInfoForDisplay } from './api.js';
import { POWER_COLORS, 
    getPowerStateIcon, 
    getSystemStateColor, 
    getStationIcon, 
    formatCommodities, 
    getReserveIcon, 
    getPriceComparison, 
    formatPriceSpan, 
    getDemandIcon } 
    from '../search_format.js';



// Load mining materials data
let miningMaterialsData = null;
fetch('/data/mining_materials.json')
    .then(response => response.json())
    .then(data => {
        miningMaterialsData = data.materials;
        console.log('Loaded mining materials data:', miningMaterialsData);
    })
    .catch(err => console.error('Failed to load mining materials data:', err));

// Add helper function to format commodity names
function formatCommodityName(name) {
    if (name === 'Low Temperature Diamonds') {
        return 'Low T. Dmnd.';
    }
    return name;
}

// Helper function to find best prices in stations
function findBestPrices(stations) {
    const allPrices = [];
    stations.forEach(station => {
        if (station.market && station.market.commodities) {
            station.market.commodities.forEach(commodity => {
                allPrices.push({
                    price: commodity.sellPrice,
                    commodity: commodity.name,
                    stationId: station.id
                });
            });
        }
    });
    
    // Sort by price descending and take top 3
    return allPrices.sort((a, b) => b.price - a.price).slice(0, 3);
}

export async function showSystemInfo(system) {
    const systemInfoDiv = document.querySelector('#system-info');
    const systemInfoContent = systemInfoDiv.querySelector('.system-info-content');
    
    try {
        // Get fresh data from API
        const systemData = await getSystemInfo(system.name);
        console.log('Fresh system data from API:', systemData);
        
        // Find best prices across all stations
        const bestPrices = findBestPrices(systemData.stations);
        
        // Sort stations by their best commodity price
        systemData.stations.sort((a, b) => {
            const aMaxPrice = Math.max(...(a.market?.commodities?.map(c => c.sellPrice) || [0]));
            const bMaxPrice = Math.max(...(b.market?.commodities?.map(c => c.sellPrice) || [0]));
            return bMaxPrice - aMaxPrice;
        });

        // First update the header with system name
        const headerTitle = systemInfoDiv.querySelector('.system-info-header h3');
        headerTitle.textContent = systemData.name;
        
        // Create the power info section
        let powerHtml = '<div class="system-info-power">';

        // First check if system has no population
        if ((!systemData.population || systemData.population === 'NULL' || systemData.population === 0) && systemData.population !== undefined) {
            powerHtml += `
                <div class="system-info-power-state">
                    <div class="info-row">
                        <span class="label">Power State:</span>
                        <span class="value">Not populated</span>
                    </div>
                </div>`;
        } 
        // Then check if system is unoccupied (has population but no power control)
        else if ((!systemData.controllingPower || systemData.controllingPower === 'NULL') &&
                 (!systemData.powerState || systemData.powerState === 'NULL') &&
                 (!systemData.powers || systemData.powers.length === 0)) {
            powerHtml += `
                <div class="system-info-power-state">
                    <div class="info-row">
                        <span class="label">Power State:</span>
                        <span class="value">Unoccupied</span>
                    </div>
                </div>`;
        } else {
            // Only show controlling power if system is controlled
            if (systemData.powerState && ['Exploited', 'Reinforced', 'Fortified', 'Stronghold'].includes(systemData.powerState)) {
                powerHtml += `
                    <div class="info-row">
                        <span class="label">Controlling Power:</span>
                        <span class="value">
                            ${systemData.controllingPower ? 
                                `<span class="dot" style="background: ${POWER_COLORS[systemData.controllingPower]}"></span>${systemData.controllingPower}` : 
                                ''}
                        </span>
                    </div>`;
            }

            // Handle different power states for the second row
            if (systemData.powers && systemData.powers.length > 0) {
                let label = 'Powers acquiring:';
                
                // Change label based on power state
                if (['Exploited', 'Reinforced', 'Fortified', 'Stronghold'].includes(systemData.powerState)) {
                    label = 'Undermining:';
                } else if (['InPrepareRadius', 'Prepared', 'Expansion'].includes(systemData.powerState)) {
                    label = 'Expanding here:';
                } else if (systemData.powerState === 'Contested') {
                    label = 'Powers fighting:';
                } else if (['NULL', 'Unoccupied'].includes(systemData.powerState)) {
                    label = 'Acquiring:';
                }

                powerHtml += `
                    <div class="info-row">
                        <span class="label">${label}</span>
                        <div class="value-list">
                            ${systemData.powers.map(power => `
                                <div>
                                    <span class="dot" style="background: ${POWER_COLORS[power]}"></span>${power}
                                </div>
                            `).join('')}
                        </div>
                    </div>`;
            }
            
            // Add power state if it exists
            if (systemData.powerState && !['NULL', 'Unoccupied'].includes(systemData.powerState)) {
                powerHtml += `
                    <div class="system-info-power-state">
                        <div class="info-row">
                            <span class="label">Power State:</span>
                            <span class="value">
                                ${getPowerStateIcon(systemData.powerState, systemData.controllingPower)}${systemData.powerState}
                            </span>
                        </div>
                    </div>`;
            }
        }
        
        powerHtml += '</div>';

        // Add system state if it exists
        const systemState = systemData.systemState || 'None';
        const stateInfo = getSystemStateColor(systemState);
        
        powerHtml += `
            <div class="system-info-state">
                <div class="info-row">
                    <span class="label">System State:</span>
                    <span class="value" style="color: ${stateInfo.color}">
                        ${stateInfo.iconHtml}${systemState}
                    </span>
                </div>
            </div>`;

        // Add mineral signals information if available
        if (systemData.mineralSignals && systemData.mineralSignals.length > 0) {
            powerHtml += `
                <div class="system-info-columns">
                    <div class="system-info-hotspots">
                        <h4>Ring Signals:</h4>
                        <ul class="hotspot-list">
                            ${systemData.mineralSignals.map(signal => {
                                // Extract the ring identifier
                                let ringId = signal.ring_name;
                                
                                // Get the system name part (everything before the body number)
                                const systemNameParts = signal.body_name.match(/^(.*?)(?=\s*\d+\s*$|$)/);
                                if (systemNameParts && systemNameParts[1]) {
                                    const systemName = systemNameParts[1].trim();
                                    // Only remove the system name if it's at the start of ring_name
                                    if (ringId.startsWith(systemName)) {
                                        ringId = ringId.slice(systemName.length).trim();
                                    }
                                }
                                
                                // For systems like Sol where ring_name doesn't include system name
                                if (ringId === signal.ring_name && signal.body_name) {
                                    // Use the body_name directly if it's a simple name (like "Mars")
                                    if (!signal.body_name.includes(systemData.name)) {
                                        ringId = signal.body_name;
                                    }
                                }
                                
                                // Remove " Ring" from the end if present
                                ringId = ringId.replace(/ Ring$/, '').trim();

                                // Get ring type icon path
                                const getRingTypeIcon = (ringType) => {
                                    const iconMap = {
                                        'Icy': '/img/icons/rings/icy.png',
                                        'Rocky': '/img/icons/rings/rocky.png',
                                        'Metallic': '/img/icons/rings/metallic.png',
                                        'Metal Rich': '/img/icons/rings/metal-rich.png'
                                    };
                                    return iconMap[ringType] || '';
                                };

                                // Create HTML for the list item
                                const ringTypeIcon = signal.mineral_type ? '' : getRingTypeIcon(signal.ring_type);
                                const ringTypeHtml = ringTypeIcon ? 
                                    `<img src="${ringTypeIcon}" width="19" height="19" class="ring-type-icon" alt="${signal.ring_type}" title="Ring Type: ${signal.ring_type}" style="vertical-align: middle;">` : '';
                                
                                return `
                                    <li class="hotspot-item ${signal.mineral_type ? 'hotspot-entry' : 'ring-type-entry'}">
                                        <span class="body">${ringId}</span>
                                        ${signal.mineral_type ? 
                                            `<img src="img/icons/hotspot-systemview.svg" class="hotspot-icon" alt="hotspot">
                                             <span class="mineral">${formatCommodityName(signal.mineral_type)}</span>` 
                                            : 
                                            `${ringTypeHtml} ${signal.ring_type} ${getReserveIcon(signal.reserve_level)}`
                                        }
                                    </li>
                                `;
                            }).join('')}
                        </ul>
                    </div>

                    <div class="system-info-stations">
                        <h4>Stations:</h4>
                        ${systemData.stations.map(station => `
                            <div class="station">
                                <div class="station-header">
                                    <img class="station-icon-systemview" src="${getStationIcon(station.type, true)}" alt="${station.type}">
                                    <div class="station-name-systemview">${station.name}</div>
                                </div>
                                <div class="station-pad">${station.landingPads} pad - ${station.distanceToArrival.toLocaleString()} Ls</div>
                                ${station.market && station.market.commodities && station.market.commodities.length > 0 ? `
                                    <div class="commodities">
                                        <h5>Market</h5>
                                        <div class="commodity-list" data-station-id="${station.id}" 
                                             data-commodities='${JSON.stringify(station.market.commodities.map(c => ({...c, name: formatCommodityName(c.name)})))}'
                                             data-mineral-signals='${JSON.stringify(systemData.mineralSignals)}'>
                                            ${formatStationCommodities(
                                                station.market.commodities.map(c => ({...c, name: formatCommodityName(c.name)})),
                                                systemData.mineralSignals,
                                                'hotspots',
                                                miningMaterialsData
                                            )}
                                        </div>
                                        <div class="commodity-buttons">
                                            <button class="cmdt-btn commodity-hotspots active" data-station-id="${station.id}">Hotspots</button>
                                            <button class="cmdt-btn commodity-mineable" data-station-id="${station.id}">Mineable</button>
                                            <button class="cmdt-btn commodity-all" data-station-id="${station.id}">All</button>
                                        </div>
                                    </div>
                                ` : ''}
                            </div>
                        `).join('')}
                    </div>
                </div>`;
        }

        // Update the content
        systemInfoContent.innerHTML = powerHtml;
        setupCommodityButtons();
        systemInfoDiv.style.display = 'block';
        
    } catch (error) {
        console.error('Error fetching system data:', error);
        systemInfoContent.innerHTML = `<div class="error">Error loading system data</div>`;
    }
}

// Store active views per station
const stationViews = new Map();

// Helper function to filter commodities based on ring types
function filterMineableCommodities(commodities, ringTypes, miningMaterials) {
    return commodities.filter(commodity => {
        const material = miningMaterials[commodity.name];
        if (!material) return false;

        // Check if any of the system's ring types support this material
        return ringTypes.some(ringType => {
            const materialRingType = material.ring_types[ringType];
            if (!materialRingType) return false;

            // Check for any mining method
            return materialRingType.core || 
                   materialRingType.hotspot || 
                   materialRingType.surfaceLaserMining || 
                   materialRingType.surfaceDeposit || 
                   materialRingType.subSurfaceDeposit;
        });
    });
}

// Helper function to get unique ring types from mineral signals
function getRingTypesFromSignals(mineralSignals) {
    return [...new Set(mineralSignals.map(signal => signal.ring_type))];
}

// Helper function to format station commodities based on view type
function formatStationCommodities(commodities, mineralSignals, viewType, miningMaterials = null) {
    let filteredCommodities = [...commodities];
    const ringTypes = getRingTypesFromSignals(mineralSignals);

    // Filter based on view type
    switch (viewType) {
        case 'hotspots':
            // Only show commodities that have hotspots
            const hotspotMinerals = new Set(mineralSignals
                .filter(signal => signal.mineral_type)
                .map(signal => signal.mineral_type));
            filteredCommodities = commodities.filter(c => hotspotMinerals.has(c.name));
            break;
        case 'mineable':
            if (!miningMaterials) {
                console.warn('Mining materials data not loaded yet');
                return '<li>Loading mining data...</li>';
            }
            // Show commodities that can be mined in the available ring types
            filteredCommodities = filterMineableCommodities(commodities, ringTypes, miningMaterials);
            break;
        case 'all':
            // Show all commodities
            break;
    }

    // Sort by price and take top 5 (except for 'all' view)
    if (viewType !== 'all') {
        filteredCommodities.sort((a, b) => b.sellPrice - a.sellPrice);
        filteredCommodities = filteredCommodities.slice(0, 5);
    }

    // Format the list
    return filteredCommodities.map(commodity => {
        const priceInfo = getPriceComparison(commodity.sellPrice, commodity.avgPrice || 0, false);
        
        return `
            <li class="station-item">
                <span class="name">${commodity.name}</span>
                <span class="price" style="color: ${priceInfo.color || '#ffee00'}">${commodity.sellPrice.toLocaleString()} CR</span>
                ${getDemandIcon(commodity.demand)}
                <span class="demand">${commodity.demand.toLocaleString()}</span>
            </li>
        `;
    }).join('');
}

// Update the click handler setup
function setupCommodityButtons() {
    document.addEventListener('click', function(event) {
        const button = event.target.closest('.cmdt-btn');
        if (!button) return;

        event.preventDefault();
        const stationId = button.dataset.stationId;
        const viewType = button.className.split('commodity-')[1].split(' ')[0];
        const station = button.closest('.station');
        const commodityList = station.querySelector('.commodity-list');
        const buttons = station.querySelectorAll('.cmdt-btn');

        // Update active button
        buttons.forEach(btn => {
            btn.classList.remove('active');
            if (btn.classList.contains(`commodity-${viewType}`)) {
                btn.classList.add('active');
            }
        });

        // Store the active view for this station
        stationViews.set(stationId, viewType);

        // Update the commodity list
        const commodities = JSON.parse(commodityList.dataset.commodities);
        const mineralSignals = JSON.parse(commodityList.dataset.mineralSignals);
        commodityList.innerHTML = formatStationCommodities(
            commodities,
            mineralSignals,
            viewType,
            miningMaterialsData
        );
    });
}

