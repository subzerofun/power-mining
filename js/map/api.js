/**
 * API handler for system data retrieval and formatting
 */

/**
 * Fetch system information by name or id64
 * @param {string|number} identifier - System name or id64
 * @returns {Promise<Object>} Formatted system data
 */
export async function getSystemInfo(identifier) {
    try {
        const response = await fetch(`/api/system/${encodeURIComponent(identifier)}`);
        if (!response.ok) {
            throw new Error(response.status === 404 ? 'System not found' : 'Failed to fetch system data');
        }
        
        const data = await response.json();
        return formatSystemResponse(data);
    } catch (error) {
        console.error('Error fetching system info:', error);
        throw error;
    }
}

/**
 * Search for systems using various criteria
 * @param {Object} params - Search parameters
 * @param {string[]} [params.names] - Array of system names
 * @param {number[]} [params.id64s] - Array of system id64s
 * @param {Object} [params.radius] - Radius search parameters
 * @param {number} params.radius.x - Center X coordinate
 * @param {number} params.radius.y - Center Y coordinate
 * @param {number} params.radius.z - Center Z coordinate
 * @param {number} params.radius.distance - Search radius in ly
 * @param {Object} [params.cube] - Cube search parameters
 * @param {number} params.cube.x1 - First corner X coordinate
 * @param {number} params.cube.y1 - First corner Y coordinate
 * @param {number} params.cube.z1 - First corner Z coordinate
 * @param {number} params.cube.x2 - Second corner X coordinate
 * @param {number} params.cube.y2 - Second corner Y coordinate
 * @param {number} params.cube.z2 - Second corner Z coordinate
 * @returns {Promise<Array>} Array of formatted system data
 */
export async function searchSystems(params) {
    try {
        const searchParams = new URLSearchParams();
        
        // Add name parameters
        if (params.names) {
            params.names.forEach(name => searchParams.append('name', name));
        }
        
        // Add id64 parameters
        if (params.id64s) {
            params.id64s.forEach(id => searchParams.append('id64', id));
        }
        
        // Add radius search parameters
        if (params.radius) {
            searchParams.append('x', params.radius.x);
            searchParams.append('y', params.radius.y);
            searchParams.append('z', params.radius.z);
            searchParams.append('radius', params.radius.distance);
        }
        
        // Add cube search parameters
        if (params.cube) {
            searchParams.append('x1', params.cube.x1);
            searchParams.append('y1', params.cube.y1);
            searchParams.append('z1', params.cube.z1);
            searchParams.append('x2', params.cube.x2);
            searchParams.append('y2', params.cube.y2);
            searchParams.append('z2', params.cube.z2);
        }
        
        const response = await fetch(`/api/systems/search?${searchParams.toString()}`);
        if (!response.ok) {
            throw new Error('Failed to search systems');
        }
        
        const data = await response.json();
        return data.map(formatSystemResponse);
    } catch (error) {
        console.error('Error searching systems:', error);
        throw error;
    }
}

/**
 * Format raw system data according to API format
 * @param {Object} data - Raw system data from API
 * @returns {Object} Formatted system data
 */
function formatSystemResponse(data) {
    return {
        id64: data.id64,
        name: data.name,
        coords: data.coords,
        controllingPower: data.controllingPower,
        powerState: data.powerState,
        systemState: data.systemState,
        powers: data.powers || [],
        distanceFromSol: data.distanceFromSol,
        stations: data.stations.map(station => ({
            name: station.name,
            id: station.id,
            updateTime: station.updateTime,
            distanceToArrival: station.distanceToArrival,
            primaryEconomy: station.primaryEconomy,
            body: station.body,
            type: station.type,
            landingPads: station.landingPads,
            market: {
                commodities: station.market.commodities || []
            }
        })),
        mineralSignals: data.mineralSignals || []
    };
}

/**
 * Format station commodities for display
 * @param {Array} commodities - Array of commodity data
 * @returns {string} HTML formatted commodity list
 */
export function formatCommodities(commodities) {
    if (!commodities || commodities.length === 0) {
        return '<p>No market data available</p>';
    }
    
    return `
        <table class="commodities-table">
            <thead>
                <tr>
                    <th>Commodity</th>
                    <th>Demand</th>
                    <th>Price</th>
                </tr>
            </thead>
            <tbody>
                ${commodities.map(commodity => `
                    <tr>
                        <td>${commodity.name}</td>
                        <td>${commodity.demand.toLocaleString()}</td>
                        <td>${commodity.sellPrice.toLocaleString()} cr</td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

/**
 * Format system information for display
 * @param {Object} system - Formatted system data
 * @returns {string} HTML formatted system information
 */
export function formatSystemInfoForDisplay(system) {
    return `
        <h4>${system.name}</h4>
        <p>Coordinates: ${system.coords.x.toFixed(2)}, ${system.coords.y.toFixed(2)}, ${system.coords.z.toFixed(2)}</p>
        ${system.distanceFromSol ? `<p>Distance from Sol: ${system.distanceFromSol.toFixed(2)} ly</p>` : ''}
        ${system.controllingPower ? `<p>Controlling Power: ${system.controllingPower}</p>` : ''}
        ${system.powerState ? `<p>Power State: ${system.powerState}</p>` : ''}
        ${system.powers.length > 0 ? `<p>Powers Acquiring: ${system.powers.join(', ')}</p>` : ''}
        
        ${system.stations.length > 0 ? `
            <h5>Stations:</h5>
            ${system.stations.map(station => `
                <div class="station-info">
                    <h6>${station.name}</h6>
                    <p>Type: ${station.type}</p>
                    <p>Distance: ${station.distanceToArrival.toLocaleString()} ls</p>
                    <p>Landing Pads: ${station.landingPads}</p>
                    <p>Economy: ${station.primaryEconomy}</p>
                    ${station.body ? `<p>Body: ${station.body}</p>` : ''}
                    ${station.market.commodities.length > 0 ? `
                        <div class="market-data">
                            <h6>Market Data:</h6>
                            ${formatCommodities(station.market.commodities)}
                        </div>
                    ` : ''}
                </div>
            `).join('')}
        ` : '<p>No stations found</p>'}
    `;
} 