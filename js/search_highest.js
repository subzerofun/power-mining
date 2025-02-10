import { 
    getStationIcon, 
    formatPrices, 
    formatPriceSpan, 
    formatNumber, 
    formatUpdateTime,
    getDemandIcon,
    getPowerStateIcon,
    formatSystemState,
    formatControllingPower,
    formatPowerInfo,
    formatSystemName,
    getSystemState,
    POWER_COLORS
} from './search_format.js';


async function searchHighest() {
    const search = window.miningSearch;
    search.clearResults();
    search.showLoading();
    
    // Get form data from the current form
    const formData = new FormData(document.getElementById('searchForm'));
    const params = new URLSearchParams();
    
    // Add all form fields to params
    formData.forEach((value, key) => {
        if (key === 'power_state[]') {
            // Handle multiple power states
            const states = formData.getAll('power_state[]');
            states.forEach(state => params.append('power_state[]', state));
        } else {
            params.append(key, value);
        }
    });
    
    // Add selected materials to params
    if (search.selectedMaterials.size > 0) {
        Array.from(search.selectedMaterials).forEach(material => {
            params.append('selected_materials[]', material);
        });
    }

    // Add selected mining types to params
    if (search.selectedMiningTypes.size > 0) {
        Array.from(search.selectedMiningTypes).forEach(type => {
            params.append('mining_types[]', type);
        });
    }

    // Add selected system states to params
    if (window.selectedSystemStates && window.selectedSystemStates.size > 0) {
        Array.from(window.selectedSystemStates).forEach(state => {
            params.append('system_state[]', state);
        });
    } else {
        params.append('system_state[]', 'Any');  // Default to "Any" if no states selected
    }
    
    try {
        // Get power goal from form
        const powerGoal = formData.get('power_goal') || 'Reinforce';
        
        // Add display format and power goal to URL
        const response = await fetch('/search_highest?' + params.toString() + '&display_format=highest&power_goal=' + powerGoal);
        const data = await response.json();
        
        if (data.error) {
            search.showError(data.error);
            return;
        }
        
        const table = document.getElementById('resultsTable');
        // table.className = 'results-table highest-price-table';
        
        // Add table header based on power goal
        const thead = table.querySelector('thead tr');
        if (powerGoal === 'Acquire') {
            table.className = 'results-table highest-price-table';
            thead.innerHTML = `
                <th>Mineral/Metal</th>
                <th>Price</th>
                <th>Demand</th>
                <th>Acquisition System</th>
                <th>Station</th>
                <th>Pad <br/> Size</th>
                <th>Station Distance</th>
                <th>Mining System</th>
                <th>Ring Details</th>
                <th>Reserve Level</th>
                <th>Power</th>
                <th>Last Update</th>
            `;
        } else {
            table.className = 'results-table reinforce-highest-table';
            thead.innerHTML = `
                <th>Mineral/Metal</th>
                <th>Price</th>
                <th>Demand</th>
                <th>System</th>
                <th>Station</th>
                <th>Pad Size</th>
                <th>Station Distance</th>
                <th>Reserve Level</th>
                <th>Power</th>
                <th>Power State</th>
                <th>Last Update</th>
            `;
        }
        
        // Prepare price comparison data
        const priceItems = data.map(item => ({
            price: item.max_price,
            commodity: item.commodity_name
        }));
        
        // Get all price comparisons in one request
        const priceData = await formatPrices(priceItems, search.useMaxPrice);
        
        // Add table body
        data.forEach((item, index) => {
            const row = search.resultsBody.insertRow();
            
            if (powerGoal === 'Acquire') {
                // Mineral/Metal
                row.insertCell().textContent = item.mineral_type || item.commodity_name;
                
                // Price
                const priceCell = row.insertCell();
                const priceComparison = Array.isArray(priceData) ? priceData[index] : null;
                const priceSpan = formatPriceSpan(item.max_price, priceComparison);
                priceCell.appendChild(priceSpan);
                
                // Demand
                const demandCell = row.insertCell();
                demandCell.innerHTML = `${getDemandIcon(item.demand)} ${formatNumber(item.demand)}`;
                
                // System Buying with State
                const systemBuyingCell = row.insertCell();
                systemBuyingCell.innerHTML = `
                    ${formatSystemName(item.system_name)}
                    ${formatSystemState(item.power_state, item.controlling_power)}`;
                
                // Station
                const stationCell = row.insertCell();
                stationCell.innerHTML = `${getStationIcon(item.station_type)}${item.station_name}`;
                
                // Pad Size
                row.insertCell().textContent = item.landing_pad_size;
                
                // Station Distance
                row.insertCell().textContent = formatNumber(item.distance_to_arrival) + ' Ls';
                
                // System Mine
                const systemMineCell = row.insertCell();
                if (item.mining_system) {
                    systemMineCell.innerHTML = `
                        ${formatSystemName(item.mining_system)}
                        ${formatSystemState(item.mining_system_power_state, item.controlling_power)}`;
                } else {
                    systemMineCell.textContent = '-';
                }
                
                // Ring Details
                const ringCell = row.insertCell();
                ringCell.innerHTML = item.ring_details || '-';
                
                // Reserve Level
                row.insertCell().textContent = item.reserve_level || '-';
                
                // Power
                const powerCell = row.insertCell();
                powerCell.innerHTML = formatPowerInfo(item.controlling_power, item.power_state, item.powers_acquiring);
                
                // Last Update
                const updateCell = row.insertCell();
                updateCell.innerHTML = item.update_time ? formatUpdateTime(item.update_time) : '-';
            } else {
                // Standard display for non-acquisition results
                // The backend has already found the highest-priced material that matches the rings
                // item.commodity_name contains that material name
                row.insertCell().textContent = item.mineral_type || item.commodity_name;
                
                const priceCell = row.insertCell();
                const priceComparison = Array.isArray(priceData) ? priceData[index] : null;
                const priceSpan = formatPriceSpan(item.max_price, priceComparison);
                priceCell.appendChild(priceSpan);
                
                const demandCell = row.insertCell();
                demandCell.innerHTML = `${getDemandIcon(item.demand)} ${formatNumber(item.demand)}`;
                
                // System with copy icon
                const systemCell = row.insertCell();
                systemCell.innerHTML = formatSystemName(item.system_name);
                
                // Station with icon
                const stationCell = row.insertCell();
                stationCell.innerHTML = `${getStationIcon(item.station_type)}${item.station_name}`;
                
                row.insertCell().textContent = item.landing_pad_size;
                row.insertCell().textContent = formatNumber(item.distance_to_arrival) + ' Ls';
                row.insertCell().textContent = item.reserve_level;
                
                // Power info with icons
                const powerCell = row.insertCell();
                powerCell.innerHTML = formatPowerInfo(item.controlling_power, item.power_state, item.powers_acquiring);
                
                // Power state with icon
                const stateCell = row.insertCell();
                stateCell.innerHTML = formatSystemState(item.power_state, item.controlling_power);
                
                const updateCell = row.insertCell();
                updateCell.innerHTML = item.update_time ? formatUpdateTime(item.update_time) : '-';
            }
        });
        
        table.style.display = 'table';
        search.hideLoading();
    } catch (error) {
        search.showError('Error fetching results: ' + error);
        search.hideLoading();
    }
}




// Make function globally available after DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Wait for miningSearch to be available
    const checkMiningSearch = () => {
        if (window.miningSearch) {
            window.searchHighest = searchHighest;
            console.log('search_highest.js loaded and initialized');
        } else {
            console.log('Waiting for miningSearch to be available...');
            setTimeout(checkMiningSearch, 100);
        }
    };
    checkMiningSearch();
});

export { searchHighest };
