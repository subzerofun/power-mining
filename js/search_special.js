import { 
    getStationIcon, 
    formatPrices, 
    formatPriceSpan, 
    formatNumber, 
    formatUpdateTime,
    getCommodityCode, 
    toggleAllSignals, 
    getDemandIcon, 
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
    POWER_COLORS
} from './search_format.js';

async function searchResHotspots() {
    const search = window.miningSearch;
    search.showLoading();
    try {
        // Get the required DOM elements
        const systemElement = document.getElementById('system');
        const distanceElement = document.getElementById('distance');
        const limitElement = document.getElementById('limit');
        const controllingPowerElement = document.getElementById('controlling_power');
        const opposingPowerElement = document.getElementById('opposing_power');
        
        if (!systemElement) {
            throw new Error('Required form elements not found. Please ensure the page is fully loaded.');
        }

        // Get the current values
        const refSystem = systemElement.value;
        const distance = distanceElement ? distanceElement.value : 100;
        const limit = limitElement ? limitElement.value : 10;
        const controllingPower = controllingPowerElement ? controllingPowerElement.value : 'Any';
        const opposingPower = opposingPowerElement ? opposingPowerElement.value : 'Any';
        
        const response = await fetch(`/search_res_hotspots?system=${encodeURIComponent(refSystem)}&distance=${distance}&limit=${limit}&controlling_power=${controllingPower}&opposing_power=${opposingPower}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        if (data.error) {
            alert('Search error: ' + data.error);
            return;
        }

        // Collect all price items up front
        const allPriceItems = [];
        data.forEach(result => {
            if (result.stations) {
                result.stations.forEach(station => {
                    if (station.other_commodities) {
                        station.priceItemsStartIndex = allPriceItems.length;
                        allPriceItems.push(...station.other_commodities.map(commodity => ({
                            price: commodity.sell_price,
                            commodity: commodity.name
                        })));
                    }
                });
            }
        });

        // Get all price comparisons in one request
        let allPriceData = [];
        if (allPriceItems.length > 0) {
            allPriceData = await formatPrices(allPriceItems, search.useMaxPrice);
        }

        const table = document.getElementById('resultsTable');
        table.className = 'results-table res-hotspot-table';
        
        // Update table headers
        const thead = table.querySelector('thead tr');
        thead.innerHTML = `
            <th>System</th>
            <th>Power</th>
            <th>DST</th>
            <th>Ring Details</th>
            <th>Ls</th>
            <th>RES Zone</th>
            <th>Stations</th>
        `;
        
        const tbody = table.getElementsByTagName('tbody')[0];
        tbody.innerHTML = '';

        // Sort data by distance
        data.sort((a, b) => a.distance - b.distance);

        data.forEach(result => {
            const row = tbody.insertRow();
            
            // System
            const systemCell = row.insertCell();
            systemCell.innerHTML = formatSystemName(result.system);

            // Power
            const powerCell = row.insertCell();
            powerCell.innerHTML = formatPowerInfo(result.controlling_power, result.power_state, result.powers_acquiring);

            // DST
            const dstCell = row.insertCell();
            dstCell.textContent = result.distance ? Math.floor(result.distance).toLocaleString() + ' Ly' : '';

            // Ring Details
            const ringCell = row.insertCell();
            ringCell.textContent = result.ring || '';

            // Ls
            const lsCell = row.insertCell();
            lsCell.textContent = result.ls || '';

            // RES Zone + Comment
            const resCell = row.insertCell();
            resCell.innerHTML = result.res_zone + (result.comment ? `<br><br>${result.comment}` : '');

            // Stations
            const stationsCell = row.insertCell();
            if (result.stations && result.stations.length > 0) {
                const topStations = filterAndSortStations(result.stations);
                if (topStations.length > 0) {
                    const stationList = document.createElement('ul');
                    stationList.className = 'station-list';
                
                    topStations.forEach(async (station) => {
                        const li = document.createElement('li');
                        const stationEntry = document.createElement('div');
                        stationEntry.className = 'station-entry';
                        
                        // Station main section
                        const stationMain = document.createElement('div');
                        stationMain.className = 'station-main';
                        
                        // Station name with icon
                        stationMain.innerHTML = `${getStationIcon(station.station_type)}${shortenStationName(station.name)} (${station.pad_size})`;
                        
                        // Station details
                        const details = document.createElement('div');
                        details.className = 'station-details';
                        const updateTime = station.update_time ? station.update_time.split(' ')[0] : null;
                        details.innerHTML = `
                            <div>Distance: ${Math.floor(station.distance).toLocaleString()} Ls</div>
                            <div class="update-time">${formatUpdateTime(updateTime)}</div>
                        `;
                        stationMain.appendChild(details);
                        stationEntry.appendChild(stationMain);
                        
                        // Commodities block
                        if (station.other_commodities && station.other_commodities.length > 0) {
                            const commoditiesBlock = document.createElement('div');
                            commoditiesBlock.className = 'other-commodities';
                            
                            const commoditiesHeader = document.createElement('div');
                            commoditiesHeader.className = 'other-commodities-title';
                            commoditiesHeader.textContent = 'Commodities:';
                            commoditiesBlock.appendChild(commoditiesHeader);
                            
                            const commoditiesList = document.createElement('div');
                            commoditiesList.className = 'other-commodities-list';

                            for (const commodity of station.other_commodities) {
                                const { color, indicator } = await formatCommodityPrice(commodity.sell_price, commodity.name, search.useMaxPrice);
                                const code = getCommodityCode(commodity.name);
                                
                                commoditiesList.innerHTML += `
                                    <div class="commodity-item">
                                        <span class="commodity-code">${code}</span>
                                        <span style="color: ${color || ''}">${commodity.sell_price.toLocaleString()} CR${indicator}</span>
                                        | ${getDemandIcon(commodity.demand)} ${commodity.demand.toLocaleString()} Demand
                                    </div>
                                `;
                            }
                            
                            commoditiesBlock.appendChild(commoditiesList);
                            stationEntry.appendChild(commoditiesBlock);
                        }
                        
                        li.appendChild(stationEntry);
                        stationList.appendChild(li);
                    });
                    stationsCell.appendChild(stationList);
                } else {
                    stationsCell.textContent = 'No stations with priority minerals & metals';
                }
            } else {
                stationsCell.textContent = 'No stations in system';
            }
        });

        table.style.display = 'table';
    } catch (error) {
        console.error('Error:', error);
        alert('An error occurred during the search. Please try again.');
    } finally {
        search.hideLoading();
    }
}

async function searchHighYieldPlatinum() {
    const search = window.miningSearch;
    search.showLoading();
    try {
        // Get the required DOM elements
        const systemElement = document.getElementById('system');
        const distanceElement = document.getElementById('distance');
        const limitElement = document.getElementById('limit');
        const controllingPowerElement = document.getElementById('controlling_power');
        const opposingPowerElement = document.getElementById('opposing_power');
        
        if (!systemElement) {
            throw new Error('Required form elements not found. Please ensure the page is fully loaded.');
        }

        // Get the current values
        const refSystem = systemElement.value;
        const distance = distanceElement ? distanceElement.value : 100;
        const limit = limitElement ? limitElement.value : 10;
        const controllingPower = controllingPowerElement ? controllingPowerElement.value : 'Any';
        const opposingPower = opposingPowerElement ? opposingPowerElement.value : 'Any';
        
        const response = await fetch(`/search_high_yield_platinum?system=${encodeURIComponent(refSystem)}&distance=${distance}&limit=${limit}&controlling_power=${controllingPower}&opposing_power=${opposingPower}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        if (data.error) {
            alert('Search error: ' + data.error);
            return;
        }

        const table = document.getElementById('resultsTable');
        table.className = 'results-table high-yield-table';
        
        // Update table headers
        const thead = table.querySelector('thead tr');
        thead.innerHTML = `
            <th>System</th>
            <th>Power</th>
            <th>DST</th>
            <th>Ring Details</th>
            <th>Percentage</th>
            <th>Stations</th>
        `;
        
        const tbody = table.getElementsByTagName('tbody')[0];
        tbody.innerHTML = '';

        // Sort data by distance
        data.sort((a, b) => a.distance - b.distance);

        data.forEach(result => {
            const row = tbody.insertRow();
            
            // System
            const systemCell = row.insertCell();
            systemCell.textContent = result.system;

            // Power
            const powerCell = row.insertCell();
            powerCell.innerHTML = formatPowerInfo(result.controlling_power, result.power_state, result.powers_acquiring);

            // DST
            const dstCell = row.insertCell();
            dstCell.textContent = result.distance ? Math.floor(result.distance).toLocaleString() + ' Ly' : '';

            // Ring Details
            const ringCell = row.insertCell();
            ringCell.textContent = result.ring || '';

            // Percentage
            const percentageCell = row.insertCell();
            percentageCell.textContent = result.percentage || '';

            // Stations
            const stationsCell = row.insertCell();
            if (result.stations && result.stations.length > 0) {
                const topStations = filterAndSortStations(result.stations);
                if (topStations.length > 0) {
                    const stationList = document.createElement('ul');
                    stationList.className = 'station-list';
                
                    topStations.forEach(async (station) => {
                        const li = document.createElement('li');
                        const stationEntry = document.createElement('div');
                        stationEntry.className = 'station-entry';
                        
                        // Station main section
                        const stationMain = document.createElement('div');
                        stationMain.className = 'station-main';
                        
                        // Station name with icon
                        stationMain.innerHTML = `${getStationIcon(station.station_type)}${shortenStationName(station.name)} (${station.pad_size})`;
                        
                        // Station details
                        const details = document.createElement('div');
                        details.className = 'station-details';
                        const updateTime = station.update_time ? station.update_time.split(' ')[0] : null;
                        details.innerHTML = `
                            <div>Distance: ${Math.floor(station.distance).toLocaleString()} Ls</div>
                            <div class="update-time">${formatUpdateTime(updateTime)}</div>
                        `;
                        stationMain.appendChild(details);
                        stationEntry.appendChild(stationMain);
                        
                        // Commodities block
                        if (station.other_commodities && station.other_commodities.length > 0) {
                            const commoditiesBlock = document.createElement('div');
                            commoditiesBlock.className = 'other-commodities';
                            
                            const commoditiesHeader = document.createElement('div');
                            commoditiesHeader.className = 'other-commodities-title';
                            commoditiesHeader.textContent = 'Commodities:';
                            commoditiesBlock.appendChild(commoditiesHeader);
                            
                            const commoditiesList = document.createElement('div');
                            commoditiesList.className = 'other-commodities-list';

                            for (const commodity of station.other_commodities) {
                                const { color, indicator } = await formatCommodityPrice(commodity.sell_price, commodity.name, search.useMaxPrice);
                                const code = getCommodityCode(commodity.name);
                                
                                commoditiesList.innerHTML += `
                                    <div class="commodity-item">
                                        <span class="commodity-code">${code}</span>
                                        <span style="color: ${color || ''}">${commodity.sell_price.toLocaleString()} CR${indicator}</span>
                                        | ${getDemandIcon(commodity.demand)} ${commodity.demand.toLocaleString()} Demand
                                    </div>
                                `;
                            }
                            
                            commoditiesBlock.appendChild(commoditiesList);
                            stationEntry.appendChild(commoditiesBlock);
                        }
                        
                        li.appendChild(stationEntry);
                        stationList.appendChild(li);
                    });
                    stationsCell.appendChild(stationList);
                } else {
                    stationsCell.textContent = 'No stations with priority minerals & metals';
                }
            } else {
                stationsCell.textContent = 'No stations in system';
            }
        });

        table.style.display = 'table';
    } catch (error) {
        console.error('Error:', error);
        alert('An error occurred during the search. Please try again.');
    } finally {
        search.hideLoading();
    }
}

// Make functions globally available after they're defined
document.addEventListener('DOMContentLoaded', () => {
    // Wait for miningSearch to be available
    const checkMiningSearch = () => {
        if (window.miningSearch) {
            // Make functions available globally for onclick attributes
            window.searchResHotspots = searchResHotspots;
            window.searchHighYieldPlatinum = searchHighYieldPlatinum;
            console.log('search_special.js loaded and initialized');
        } else {
            console.log('Waiting for miningSearch to be available...');
            setTimeout(checkMiningSearch, 100);
        }
    };
    checkMiningSearch();
});

// Export all functions
export { searchResHotspots, searchHighYieldPlatinum };