import { showLoading, hideLoading } from './search.js';

// Make functions globally available for onclick handlers
document.addEventListener('DOMContentLoaded', () => {
    window.searchResHotspots = searchResHotspots;
    window.searchHighYieldPlatinum = searchHighYieldPlatinum;
});

function getCommodityCode(commodityName) {
    const codeMap = {
        'Platinum': 'PLA',
        'Painite': 'PAI',
        'Osmium': 'OSM',
        'Gold': 'GLD',
        'Silver': 'SLV',
        'Tritium': 'TRI',
        'Low Temperature Diamonds': 'LTD',
        'Void Opals': 'VOP',
        'Grandidierite': 'GND',
        'Alexandrite': 'ALX',
        'Musgravite': 'MSG',
        'Benitoite': 'BEN',
        'Serendibite': 'SER',
        'Monazite': 'MON',
        'Rhodplumsite': 'RHO',
        'Bromellite': 'BRM'
    };
    return codeMap[commodityName] || commodityName.substring(0, 3).toUpperCase();
}

function getStationIcon(stationType) {
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
    return `<img src="/img/icons/${icon}" alt="${stationType}" class="station-icon">`;
}

async function searchResHotspots() {
    const search = window.miningSearch;
    search.showLoading();
    try {
        // Get the current reference system
        const refSystem = document.getElementById('system').value;
        const database = document.getElementById('database').value;
        
        const response = await fetch(`/search_res_hotspots?system=${encodeURIComponent(refSystem)}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                database: database
            })
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
            <th>Comment</th>
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
            powerCell.textContent = result.power || '';

            // DST
            const dstCell = row.insertCell();
            dstCell.textContent = result.distance ? Math.floor(result.distance).toLocaleString() + ' Ly' : '';

            // Ring Details
            const ringCell = row.insertCell();
            ringCell.textContent = result.ring || '';

            // Ls
            const lsCell = row.insertCell();
            lsCell.textContent = result.ls || '';

            // RES Zone
            const resCell = row.insertCell();
            resCell.textContent = result.res_zone || '';

            // Comment
            const commentCell = row.insertCell();
            commentCell.textContent = result.comment || '';

            // Stations
            const stationsCell = row.insertCell();
            if (result.stations && result.stations.length > 0) {
                const stationList = document.createElement('ul');
                stationList.className = 'station-list';
                
                result.stations.forEach(station => {
                    const li = document.createElement('li');
                    const stationEntry = document.createElement('div');
                    stationEntry.className = 'station-entry';
                    
                    // Station main section
                    const stationMain = document.createElement('div');
                    stationMain.className = 'station-main';
                    
                    // Station name with icon
                    stationMain.innerHTML = `${getStationIcon(station.station_type)}${station.name} (${station.pad_size})`;
                    
                    // Station details
                    const details = document.createElement('div');
                    details.className = 'station-details';
                    details.innerHTML = `
                        <div>Distance: ${Math.floor(station.distance).toLocaleString()} Ls</div>
                        <div class="update-time">Updated: ${station.update_time ? station.update_time.split(' ')[0] : ''}</div>
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

                        // Get price comparisons for all commodities
                        const priceItems = station.other_commodities.map(commodity => ({
                            price: commodity.sell_price,
                            commodity: commodity.name
                        }));

                        fetch('/get_price_comparison', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                items: priceItems,
                                use_max: search.useMaxPrice
                            })
                        })
                        .then(r => r.json())
                        .then(priceData => {
                            station.other_commodities.forEach((commodity, index) => {
                                const commodityItem = document.createElement('div');
                                commodityItem.className = 'commodity-item';
                                const code = getCommodityCode(commodity.name);
                                const priceSpan = document.createElement('span');
                                if (priceData[index] && priceData[index].color) {
                                    priceSpan.style.color = priceData[index].color;
                                }
                                priceSpan.textContent = `${commodity.sell_price.toLocaleString()} CR${priceData[index]?.indicator || ''}`;
                                commodityItem.innerHTML = `<span class="commodity-code">${code}</span>`;
                                commodityItem.appendChild(priceSpan);
                                commodityItem.innerHTML += ` | ${search.getDemandIcon(commodity.demand)} ${commodity.demand.toLocaleString()} Demand`;
                                commoditiesList.appendChild(commodityItem);
                            });
                        });
                        
                        commoditiesBlock.appendChild(commoditiesList);
                        stationEntry.appendChild(commoditiesBlock);
                    }
                    
                    li.appendChild(stationEntry);
                    stationList.appendChild(li);
                });
                stationsCell.appendChild(stationList);
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
        // Get the current reference system and database
        const refSystem = document.getElementById('system').value;
        const database = document.getElementById('database').value;
        
        const response = await fetch(`/search_high_yield_platinum?system=${encodeURIComponent(refSystem)}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                database: database
            })
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
        table.className = 'results-table res-hotspot-table';
        
        // Update table headers
        const thead = table.querySelector('thead tr');
        thead.innerHTML = `
            <th>System</th>
            <th>Power</th>
            <th>DST</th>
            <th>Ring Details</th>
            <th>Percentage</th>
            <th>Comment</th>
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
            powerCell.textContent = result.power || '';

            // DST
            const dstCell = row.insertCell();
            dstCell.textContent = result.distance ? Math.floor(result.distance).toLocaleString() + ' Ly' : '';

            // Ring Details
            const ringCell = row.insertCell();
            ringCell.textContent = result.ring || '';

            // Percentage
            const percentageCell = row.insertCell();
            percentageCell.textContent = result.percentage || '';

            // Comment
            const commentCell = row.insertCell();
            commentCell.textContent = result.comment || '';

            // Stations
            const stationsCell = row.insertCell();
            if (result.stations && result.stations.length > 0) {
                const stationList = document.createElement('ul');
                stationList.className = 'station-list';
                
                result.stations.forEach(station => {
                    const li = document.createElement('li');
                    const stationEntry = document.createElement('div');
                    stationEntry.className = 'station-entry';
                    
                    // Station main section
                    const stationMain = document.createElement('div');
                    stationMain.className = 'station-main';
                    
                    // Station name with icon
                    stationMain.innerHTML = `${getStationIcon(station.station_type)}${station.name} (${station.pad_size})`;
                    
                    // Station details
                    const details = document.createElement('div');
                    details.className = 'station-details';
                    details.innerHTML = `
                        <div>Distance: ${Math.floor(station.distance).toLocaleString()} Ls</div>
                        <div class="update-time">Updated: ${station.update_time ? station.update_time.split(' ')[0] : ''}</div>
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

                        // Get price comparisons for all commodities
                        const priceItems = station.other_commodities.map(commodity => ({
                            price: commodity.sell_price,
                            commodity: commodity.name
                        }));

                        fetch('/get_price_comparison', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                items: priceItems,
                                use_max: search.useMaxPrice
                            })
                        })
                        .then(r => r.json())
                        .then(priceData => {
                            station.other_commodities.forEach((commodity, index) => {
                                const commodityItem = document.createElement('div');
                                commodityItem.className = 'commodity-item';
                                const code = getCommodityCode(commodity.name);
                                const priceSpan = document.createElement('span');
                                if (priceData[index] && priceData[index].color) {
                                    priceSpan.style.color = priceData[index].color;
                                }
                                priceSpan.textContent = `${commodity.sell_price.toLocaleString()} CR${priceData[index]?.indicator || ''}`;
                                commodityItem.innerHTML = `<span class="commodity-code">${code}</span>`;
                                commodityItem.appendChild(priceSpan);
                                commodityItem.innerHTML += ` | ${search.getDemandIcon(commodity.demand)} ${commodity.demand.toLocaleString()} Demand`;
                                commoditiesList.appendChild(commodityItem);
                            });
                        });
                        
                        commoditiesBlock.appendChild(commoditiesList);
                        stationEntry.appendChild(commoditiesBlock);
                    }
                    
                    li.appendChild(stationEntry);
                    stationList.appendChild(li);
                });
                stationsCell.appendChild(stationList);
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