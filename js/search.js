import formStorage from './storage.js';
import { getStationIcon, 
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
    getSystemState,
    POWER_COLORS
} from './search_format.js';
import { 
    searchHighest
} from './search_highest.js';



class MiningSearch {
    constructor(container, options = {}) {
        this.container = container || document;
        this.form = this.container.querySelector('#searchForm');
        this.loadingIndicator = this.container.querySelector('.loading');
        this.loadingOverlay = this.container.querySelector('.loading-overlay');
        this.resultsTable = this.container.querySelector('#resultsTable');
        this.resultsBody = this.resultsTable.querySelector('tbody');
        this.useMaxPrice = false;
        
        // Handle options
        if (options.skipMaterialsSetup) {
            this.selectedMaterials = new Set(options.selectedMaterials || ['Default']);
        } else {
            this.selectedMaterials = new Set(['Default']);
            this.setupMaterialsAutocomplete();
        }
        
        this.selectedMiningTypes = new Set(['All']);
        this.formStorage = formStorage;
        
        // Create and add spinner
        const spinnerContainer = this.container.querySelector('#spinner-container');
        if (spinnerContainer) {
            this.spinner = createSpinner();
            spinnerContainer.appendChild(this.spinner);
        }
        
        // Load spinner CSS
        this.spinnerCSS = document.createElement('link');
        this.spinnerCSS.rel = 'stylesheet';
        this.spinnerCSS.type = 'text/css';
        this.spinnerCSS.href = '/img/loading/spinner.css';
        document.head.appendChild(this.spinnerCSS);
        
        // Store bound handlers for cleanup
        this.boundHandleSearch = (e) => {
            e.preventDefault();
            this.handleSearch();
        };
        this.boundHandlePowerSelection = this.handlePowerSelection.bind(this);
        
        this.setupEventListeners();
        this.setupMiningTypeAutocomplete();
        
        // Add event listener for power selection
        const powerSelect = this.container.querySelector('#controlling_power');
        if (powerSelect) {
            powerSelect.addEventListener('change', this.boundHandlePowerSelection);
        }
        
        // Initial power selection check
        this.handlePowerSelection();
    }

    cleanup() {
        // Remove event listeners
        if (this.form) {
            this.form.removeEventListener('submit', this.boundHandleSearch);
        }
        
        const powerSelect = this.container?.querySelector('#controlling_power');
        if (powerSelect) {
            powerSelect.removeEventListener('change', this.boundHandlePowerSelection);
        }

        // Remove spinner
        if (this.spinner && this.spinner.parentNode) {
            this.spinner.parentNode.removeChild(this.spinner);
        }

        // Remove spinner CSS if no other instances are using it
        if (this.spinnerCSS && this.container && !this.container.querySelector('.mining-search:not(#' + this.container.id + ')')) {
            this.spinnerCSS.parentNode?.removeChild(this.spinnerCSS);
        }

        // Clear loading overlay and indicator
        if (this.loadingOverlay) {
            this.loadingOverlay.style.display = 'none';
        }
        if (this.loadingIndicator) {
            this.loadingIndicator.style.display = 'none';
        }

        // Clear results
        if (this.resultsBody) {
            this.resultsBody.innerHTML = '';
        }
        if (this.resultsTable) {
            this.resultsTable.style.display = 'none';
        }
    }

    setupEventListeners() {
        if (this.form) {
            this.form.addEventListener('submit', this.boundHandleSearch);
        }
    }

    async init(container) {
        if (container) {
            this.container = container;
            this.form = this.container.querySelector('#searchForm');
            this.loadingIndicator = this.container.querySelector('.loading');
            this.loadingOverlay = this.container.querySelector('.loading-overlay');
            this.resultsTable = this.container.querySelector('#resultsTable');
            this.resultsBody = this.resultsTable.querySelector('tbody');
            
            // Re-setup event listeners with new container
            this.setupEventListeners();
            
            // Only setup materials autocomplete if the element exists
            const materialsInput = document.getElementById('materialsInput');
            if (materialsInput) {
                this.setupMaterialsAutocomplete();
            }

            // Only setup mining type autocomplete if the element exists
            const miningTypeInput = document.getElementById('miningTypeInput');
            if (miningTypeInput) {
                this.setupMiningTypeAutocomplete();
            }
        
            // Re-setup power selection
            const powerSelect = this.container.querySelector('#controlling_power');
            if (powerSelect) {
                powerSelect.addEventListener('change', this.handlePowerSelection.bind(this));
                this.handlePowerSelection();
            }
        }
    }

    async handleSearch() {
        this.formStorage.saveFormValues(this); // Pass the MiningSearch instance
        this.showLoading();
        
        // Get form data
        const signal_type = document.getElementById('signal_type').value;
        const power_goal = document.getElementById('power_goal').value;
        if (!signal_type) {
            alert('Please select a commodity');
            this.hideLoading();
            return;
        }

        // Reset to default table structure
        const resultsContainer = document.getElementById('resultsContainer');
        resultsContainer.innerHTML = `
            <table id="resultsTable" class="results-table reinforce-table" style="display: none;">
                <thead>
                    <tr>
                        <th>System</th>
                        <th>DST</th>
                        <th>Ring Details</th>
                        <th>Stations</th>
                        <th>State</th>
                        <th>Power</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        `;
        // Re-assign table references
        this.resultsTable = document.getElementById('resultsTable');
        this.resultsBody = this.resultsTable.querySelector('tbody');

        // Build search parameters
        const formData = new FormData(this.form);
        const params = new URLSearchParams();
            
        // Add all form fields to params, skipping power-related fields if "Any" is selected
        formData.forEach((value, key) => {
            const powerSelect = document.getElementById('controlling_power');
            if ((!powerSelect.value || powerSelect.value === "Any") && 
                (key === 'power_goal' || key === 'opposing_power')) {
                return; // Skip power-related fields when "Any" is selected
            }
            
            if (key === 'power_state[]') {
                // Handle multiple power states
                const states = formData.getAll('power_state[]');
                states.forEach(state => params.append('power_state[]', state));
            } else {
                params.append(key, value);
            }
        });
        
        // Add selected materials to params
        if (this.selectedMaterials.size > 0) {
            Array.from(this.selectedMaterials).forEach(material => {
                params.append('selected_materials[]', material);
            });
        }

        // Add selected mining types to params
        if (this.selectedMiningTypes.size > 0) {
            Array.from(this.selectedMiningTypes).forEach(type => {
                params.append('mining_types[]', type);
            });
        }

        // Add selected system states to params
        if (selectedSystemStates.size > 0) {
            Array.from(selectedSystemStates).forEach(state => {
                params.append('system_state[]', state);
            });
        } else {
            params.append('system_state[]', 'Any');  // Default to "Any" if no states selected
                }

        try {
            // Save form data - using correct method name
            this.formStorage.saveFormValues(this);
            
            // Make the search request
            const response = await fetch(`/search?${params.toString()}`);
            const results = await response.json();
            
            if (results.error) {
                this.showError(results.error);
            } else {
                // Use appropriate display function based on power goal
                if (power_goal === 'Acquire') {
                    this.displayAcquisitionResults(results);
                } else {
                    this.displayResults(results);
                }
            }
        } catch (error) {
            this.showError('Error performing search. Please try again.');
            console.error('Search error:', error);
        } finally {
            this.hideLoading();
        }
    }

    async displayResults(results) {
        this.resultsBody.innerHTML = '';
        
        // Filter out systems with no stations
        results = results.filter(system => system.stations && system.stations.length > 0);
        
        if (results.length === 0) {
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.setAttribute('colspan', '6');  // Span all columns
            td.textContent = 'No systems found matching your criteria';
            tr.appendChild(td);
            this.resultsBody.appendChild(tr);
            this.resultsTable.style.display = 'table';
            this.resultsTable.style.width = '100%';
            return;
        }

        // Get the current search type
        const searchType = document.getElementById('signal_type').value;

        // Prepare all price comparison items in a single array
        const allPriceItems = [];
        
        // Add main commodity prices and other commodities with consistent structure
        results.forEach(system => {
            system.stations.forEach(station => {
                allPriceItems.push({
                    price: station.sell_price,
                    commodity: searchType,
                    systemId: system.system_id64,
                    stationName: station.name,
                    commodityName: searchType
                });
                // Add other commodities in the same array
                station.other_commodities.forEach(commodity => {
                    allPriceItems.push({
                        price: commodity.sell_price,
                        commodity: getCommodityCode(commodity.name),
                        systemId: system.system_id64,
                        stationName: station.name,
                        commodityName: commodity.name
                    });
                });
            });
        });

        // Get all price comparisons in one request
        const allPriceData = await formatPrices(allPriceItems, this.useMaxPrice);
        
        // Create a map to look up price data for all commodities
        const priceDataMap = new Map();
        allPriceItems.forEach((item, index) => {
            const key = `${item.systemId}_${item.stationName}_${item.commodityName}`;
            priceDataMap.set(key, allPriceData[index]);
        });

        for (const system of results) {
            const row = document.createElement('tr');
            
            // Create station list HTML
            const stationListItems = system.stations.map(station => {
                const key = `${system.system_id64}_${station.name}_${searchType}`;
                const priceComparison = priceDataMap.get(key);
                const priceSpan = formatPriceSpan(station.sell_price, priceComparison);
                
                // For "Any" search, the backend has already matched materials with rings
                // and returned only valid matches with their prices
                return `
                <li>
                    <div class="station-entry">
                        <div class="station-main">
                            ${getStationIcon(station.station_type)}${station.name} (${station.pad_size})
                            <div class="station-details">
                                ${searchType === 'Any' ? `<div class="material-any">${station.mineral_type || station.commodity_name || 'Unknown'}</div>` : ''}
                                <div>Price: ${priceSpan.outerHTML}</div>
                                <div>Demand: ${getDemandIcon(station.demand)} ${formatNumber(station.demand)}</div>
                                <div>Distance: ${formatNumber(Math.floor(station.distance))} Ls</div>
                                <div class="update-time">${formatUpdateTime(station.update_time)}</div>
                            </div>
                        </div>
                        ${station.other_commodities.length > 0 ? `
                            <div class="other-commodities">
                                <div class="other-commodities-title">Other Commodities:</div>
                                <div class="other-commodities-list">
                                    ${station.other_commodities
                                        .filter(commodity => {
                                            if (this.selectedMaterials.has('Default')) {
                                                return true;  // Show all in default mode
                }
                                            const code = getCommodityCode(commodity.name);
                                            return this.selectedMaterials.has(code);  // Only show explicitly selected materials
                                        })
                                        .sort((a, b) => {
                                            if (this.selectedMaterials.has('Default')) {
                                                // Default sorting by price
                                                return b.sell_price - a.sell_price;
                }
                                            // Sort by order in selected materials
                                            const selectedArray = Array.from(this.selectedMaterials);
                                            const aCode = getCommodityCode(a.name);
                                            const bCode = getCommodityCode(b.name);
                                            return selectedArray.indexOf(aCode) - selectedArray.indexOf(bCode);
                                        })
                                        .map(commodity => {
                                            const key = `${system.system_id64}_${station.name}_${commodity.name}`;
                                            const priceData = priceDataMap.get(key);
                                            const priceSpan = formatPriceSpan(commodity.sell_price, priceData);
                                            const commodityCode = getCommodityCode(commodity.name);
                                            return `<div class="commodity-item"><span class="commodity-code">${commodityCode}</span>${priceSpan.outerHTML} ${getDemandIcon(commodity.demand, true)} ${formatNumber(commodity.demand)} Demand</div>`;
                                        }).join('')}
                                </div>
                            </div>` : ''}
                    </div>
                </li>`;
            });
                
            // Format system info
            console.log('Full system data:', system);
            console.log('System:', system.name);
            console.log('System state:', system.system_state);
            console.log('All signals:', system.all_signals);
            console.log('Rings:', system.rings);
            console.log('Search type:', searchType);
            console.log('System state for', system.name, ':', system.system_state);
                
            // Group rings by body_name
            const ringsByPlanet = {};
            system.rings.forEach(ring => {
                const bodyName = ring.body_name;
                if (!ringsByPlanet[bodyName]) {
                    ringsByPlanet[bodyName] = [];
                }
                ringsByPlanet[bodyName].push(ring);
                    });
                    
            // Create the signal list HTML
            const signalListHtml = Object.entries(ringsByPlanet).map(([bodyName, rings]) => {
                return rings.map((ring, index) => {
                    const isFirstInPlanet = index === 0;
                    const planetIcon = isFirstInPlanet ? '<img src="/img/icons/ringed-planet-2.svg" class="planet-icon" alt="Planet">' : '<span class="planet-icon-space"></span>';
                    return `<li>${planetIcon}${ring.name}: ${ring.signals}</li>`;
                }).join('');
            }).join('');
            
            // Check if there are any signals not currently shown
            const hasAdditionalSignals = system.all_signals.length > system.rings.length;
            
            row.innerHTML = `
                <td>
                    <div class="system-name">
                        ${system.name}
                        <img src="/img/icons/copy.svg" 
                             class="copy-icon" 
                             width="12" 
                             height="12" 
                             alt="Copy" 
                             onclick="navigator.clipboard.writeText('${system.name}').then(() => this.classList.add('copied'))"
                             onanimationend="this.classList.remove('copied')"
                             title="Copy system name">
                    </div>
                </td>
                <td>${formatNumber(system.distance)} Ly</td>
                <td>
                    <ul class="signal-list">
                        ${signalListHtml}
                    </ul>
                    ${hasAdditionalSignals ? `
                        <button class="btn btn-small show-all-signals">
                            Show all signals
                        </button>
                    ` : ''}
                </td>
                <td>
                    <ul class="station-list">
                        ${stationListItems.join('')}
                    </ul>
                </td>
                <td>
                    ${getPowerStateIcon(system.power_state, system.controlling_power)}${system.power_state || 'None'}
                    ${getSystemState(system.system_state)}
                </td>
                <td>${system.power_info || system.controlling_power || 'None'}</td>
            `;
            
            // Add click handler after the row is added to DOM
            if (hasAdditionalSignals) {
                const button = row.querySelector('.show-all-signals');
                button.addEventListener('click', () => toggleAllSignals(button, system.all_signals, system.name));
            }
            
            this.resultsBody.appendChild(row);
        }
        
        this.resultsTable.style.display = 'table';
        this.resultsTable.style.width = '100%';
    }

    async displayAcquisitionResults(results) {
        const SHOW_HEADERS = true;  // Toggle this to show/hide headers
        const HIDE_SUBSEQUENT_HEADERS = true;  // Hide headers after the first result
        this.clearResults();
        
        if (results.length === 0) {
            this.showNoResults();
            return;
        }

        // Debug logging
        //console.log('Results:', results);

        // Get the current search type
        const searchType = document.getElementById('signal_type').value;
        //console.log('Search type:', searchType);

        // Prepare all price comparison items
        const priceItems = results.flatMap(system => 
            system.stations.map(station => ({
                price: station.sell_price,
                commodity: station.commodity_name,
                systemId: system.system_id64,
                stationName: station.name,
                commodityName: station.commodity_name
            }))
        );

        // Get all price comparisons in one request
        const priceData = await formatPrices(priceItems, this.useMaxPrice);
        
        // Create a map to look up price data
        const priceDataMap = new Map();
        priceItems.forEach((item, index) => {
            const key = `${item.systemId}_${item.stationName}_${item.commodityName}`;
            priceDataMap.set(key, priceData[index]);
        });

        // Get all other commodity price comparisons in one request
        const allOtherCommodityPrices = [];
        results.forEach(system => {
            system.stations.forEach(station => {
                station.other_commodities.forEach(commodity => {
                    allOtherCommodityPrices.push({
                        price: commodity.sell_price,
                        commodity: getCommodityCode(commodity.name),
                        systemId: system.system_id64,
                        stationName: station.name,
                        commodityName: commodity.name
                    });
                });
            });
        });
        const allOtherPriceData = await formatPrices(allOtherCommodityPrices, this.useMaxPrice);

        // Add other commodity price data to the map
        if (Array.isArray(allOtherPriceData)) {
            allOtherCommodityPrices.forEach((item, index) => {
                const key = `${item.systemId}_${item.stationName}_${item.commodityName}`;
                priceDataMap.set(key, allOtherPriceData[index]);
            });
        }

        // Create main table structure
        const mainTable = document.createElement('table');
        mainTable.id = 'resultsTable';
        mainTable.className = 'results-table acquisition-table';

        const mainBody = document.createElement('tbody');
        
        // Process each unoccupied system
        results.forEach((unoccupiedSystem, systemIndex) => {
            // Convert mining_systems to array if it's an object
            const miningSystems = Array.isArray(unoccupiedSystem.mining_systems) ? 
                unoccupiedSystem.mining_systems : 
                Object.values(unoccupiedSystem.mining_systems);

            const row = document.createElement('tr');
            if (systemIndex > 0) {
                row.style.marginTop = '20px';  // Add margin between result sets
            }
            
            // Create target cell (unoccupied system info)
            const targetCell = document.createElement('td');
            targetCell.className = 'target-section';
            
            const targetTable = document.createElement('table');
            targetTable.className = 'inner-table';
            
            // Target table header - only show for first result if HIDE_SUBSEQUENT_HEADERS is true
            if (SHOW_HEADERS && (!HIDE_SUBSEQUENT_HEADERS || systemIndex === 0)) {
                const targetHeader = document.createElement('thead');
                targetHeader.innerHTML = `
                    <tr>
                        <th>Target System</th>
                        <th>State</th>
                        <th>DST</th>
                        <th>Stations</th>
                    </tr>
                `;
                targetTable.appendChild(targetHeader);
            }
            
            // Target table body
            const targetBody = document.createElement('tbody');
            const targetRow = document.createElement('tr');
            targetRow.innerHTML = `
                <td>
                    <div class="system-name">
                        ${unoccupiedSystem.name}
                        <img src="/img/icons/copy.svg" 
                             class="copy-icon" 
                             width="12" 
                             height="12" 
                             alt="Copy" 
                             onclick="navigator.clipboard.writeText('${unoccupiedSystem.name}').then(() => this.classList.add('copied'))"
                             onanimationend="this.classList.remove('copied')"
                             title="Copy system name">
                    </div>
                </td>
                <td>${getPowerStateIcon(unoccupiedSystem.power_state, unoccupiedSystem.controlling_power)}${unoccupiedSystem.power_state}
                ${getSystemState(unoccupiedSystem.system_state)}
                </td>
                <td>${formatNumber(unoccupiedSystem.distance)} Ly</td>
                <td>
                    <div class="station-list">
                        ${unoccupiedSystem.stations ? 
                            (unoccupiedSystem.stations.map(station => {
                                // Get price data for the main commodity
                                const key = `${unoccupiedSystem.system_id64}_${station.name}_${station.commodity_name}`;
                                station.priceData = priceDataMap.get(key);
                                
                                // Add price data for other commodities
                                station.other_commodities = station.other_commodities.map(commodity => {
                                    const key = `${unoccupiedSystem.system_id64}_${station.name}_${commodity.name}`;
                                    const priceData = priceDataMap.get(key);
                                    return { ...commodity, priceData };
                                });
                                
                                return formatAcquisitionStation(station, searchType, Array.from(this.selectedMaterials));
                            }).join('')) : ''}
                    </div>
                </td>
            `;
            targetBody.appendChild(targetRow);
            targetTable.appendChild(targetBody);
            targetCell.appendChild(targetTable);
            row.appendChild(targetCell);
            
            // Create mining systems cell
            const miningCell = document.createElement('td');
            miningCell.className = 'mining-section';
            
            // Create single table for all mining systems
            const miningTable = document.createElement('table');
            miningTable.className = 'inner-table';
            
            // Mining table header - only show for first result if HIDE_SUBSEQUENT_HEADERS is true
            if (SHOW_HEADERS && (!HIDE_SUBSEQUENT_HEADERS || systemIndex === 0)) {
                const miningHeader = document.createElement('thead');
                miningHeader.innerHTML = `
                    <tr>
                        <th class="gap-cell"></th>
                        <th>Mining System</th>
                        <th>Ring Details</th>
                        <th>State</th>
                        <th>Power</th>
                    </tr>
                `;
                miningTable.appendChild(miningHeader);
            }
            
            // Mining table body
            const miningBody = document.createElement('tbody');
            
            // Add each mining system as a row
            miningSystems.forEach((miningSystem, index) => {
                const miningRow = document.createElement('tr');
                const gapCell = document.createElement('td');
                gapCell.className = 'gap-cell';
                
                // Add specific connector class based on position
                if (miningSystems.length === 1) {
                    gapCell.classList.add('single');  // Only horizontal line for single mining system
                } else if (index === 0) {
                    gapCell.classList.add('first');   // "┰" shape for first mining system
                } else if (index === miningSystems.length - 1) {
                    gapCell.classList.add('last');    // "┕" shape for last mining system
                } else {
                    gapCell.classList.add('next');    // "┠" shape for middle mining systems
                }
                
                miningRow.innerHTML = `
                    <td class="${gapCell.className}"></td>
                    <td>
                        <div class="system-name">
                            ${miningSystem.name}
                            <img src="/img/icons/copy.svg" 
                                 class="copy-icon" 
                                 width="12" 
                                 height="12" 
                                 alt="Copy" 
                                 onclick="navigator.clipboard.writeText('${miningSystem.name}').then(() => this.classList.add('copied'))"
                                 onanimationend="this.classList.remove('copied')"
                                 title="Copy system name">
                        </div>
                    </td>
                    <td>
                        ${miningSystem.rings.map(ring => {
                            let displayRingName = ring.name;
                            if (displayRingName.startsWith(miningSystem.name)) {
                                displayRingName = displayRingName.slice(miningSystem.name.length).trim();
                            }
                            return `
                            <div>
                                <img src="/img/icons/ringed-planet-2.svg" class="planet-icon" alt="Planet">
                                ${displayRingName}: ${ring.signals}
                            </div>
                        `}).join('')}
                        ${unoccupiedSystem.all_signals && unoccupiedSystem.all_signals.length > miningSystem.rings.length ? `
                            <button class="btn btn-small show-all-signals">
                                Show all signals
                            </button>
                        ` : ''}
                    </td>
                    <td>${getPowerStateIcon(miningSystem.power_state, miningSystem.controlling_power)}${miningSystem.power_state}</td>
                    <td>
                        <div class="power-info">
                            <div>
                                <span style="display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: ${miningSystem.controlling_power ? POWER_COLORS[miningSystem.controlling_power] || '#DDD' : '#DDD'}; margin-right: 4px;"></span>
                                <span class="controlling-power">${miningSystem.controlling_power}</span>
                            </div>
                            ${miningSystem.powers_acquiring && miningSystem.powers_acquiring.length > 0 ? 
                                `<div class="undermining-power">${miningSystem.powers_acquiring.map(power => 
                                    `<div><span style="display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: ${POWER_COLORS[power] || '#DDD'}; margin-right: 4px;"></span>${power}</div>`
                                ).join('')}</div>` : 
                                ''}
                        </div>
                    </td>
                `;
                miningBody.appendChild(miningRow);

                // Add click handler for show all signals button
                const button = miningRow.querySelector('.show-all-signals');
                if (button) {
                    button.addEventListener('click', () => toggleAllSignals(button, unoccupiedSystem.all_signals, unoccupiedSystem.name));
                }
            });
            
            miningTable.appendChild(miningBody);
            miningCell.appendChild(miningTable);
            row.appendChild(miningCell);
            
            mainBody.appendChild(row);
        });
        
        mainTable.appendChild(mainBody);
        
        // Add to document
        const resultsContainer = document.getElementById('resultsContainer');
        resultsContainer.appendChild(mainTable);
        mainTable.style.display = 'table';
    }

    showNoResults() {
        this.resultsBody.innerHTML = `
            <tr>
                <td colspan="10" style="text-align: center;">
                    No systems found matching your criteria
                </td>
            </tr>
        `;
        this.resultsTable.style.display = 'table';
    }

    showError(message) {
        this.resultsBody.innerHTML = `
            <tr>
                <td colspan="10" style="text-align: center; color: #ff4444;">
                    ${message}
                </td>
            </tr>
        `;
        this.resultsTable.style.display = 'table';
    }

    showLoading() {
        this.loadingOverlay.style.display = 'block';
        this.loadingOverlay.classList.add('visible');
        this.loadingIndicator.style.display = 'block';
        this.resultsTable.style.display = 'none';
    }

    hideLoading() {
        this.loadingOverlay.style.display = 'none';
        this.loadingOverlay.classList.remove('visible');
        this.loadingIndicator.style.display = 'none';
    }

    clearResults() {
        this.resultsBody.innerHTML = '';
        this.resultsTable.style.display = 'none';
        this.hideLoading(); // Ensure loading is hidden when clearing results
    }

    setupMaterialsAutocomplete() {
        const input = document.getElementById('materialsInput');
        const autocompleteDiv = document.getElementById('materialsAutocomplete');
        const selectedDiv = document.querySelector('.selected-materials');
               
        // Initialize with Default tag
        this.selectedMaterials = new Set(['Default']);
        this.updateSelectedMaterials();
        input.setAttribute('autocomplete', 'off');

        // Show all options immediately on focus
        input.addEventListener('focus', () => {
            const commoditySelect = document.getElementById('signal_type');
            const commodities = ['Default'].concat(Array.from(commoditySelect.options).map(opt => opt.value));
            
            autocompleteDiv.innerHTML = commodities
                .map(name => {
                    const code = name === 'Default' ? 'Default' : getCommodityCode(name);
                    // Determine size class based on text length - only two sizes now
                    const lengthClass = name.length > 15 ? 'long' : '';
                    
                    return `<div class="autocomplete-item" data-name="${name}" data-code="${code}" data-length="${lengthClass}">${name}</div>`;
                })
                .join('');
            autocompleteDiv.style.display = 'grid';
        });

        // Filter on input
        input.addEventListener('input', () => {
            const value = input.value.toLowerCase();
            const commoditySelect = document.getElementById('signal_type');
            const commodities = ['Default'].concat(Array.from(commoditySelect.options).map(opt => opt.value));
            
            const matches = commodities.filter(name => name.toLowerCase().includes(value));

            if (matches.length > 0) {
                autocompleteDiv.innerHTML = matches
                    .map(name => {
                        const code = name === 'Default' ? 'Default' : getCommodityCode(name);
                        return `<div class="autocomplete-item" data-name="${name}" data-code="${code}">${name}</div>`;
                    })
                    .join('');
                autocompleteDiv.style.display = 'grid';
            } else {
                autocompleteDiv.style.display = 'none';
            }
        });

        // Handle selection
        autocompleteDiv.addEventListener('click', (e) => {
            const item = e.target.closest('.autocomplete-item');
            if (!item) return;
            
            const name = item.dataset.name;
            const code = item.dataset.code;

            if (name === 'Default') {
                this.selectedMaterials.clear();
                this.selectedMaterials.add('Default');
            } else {
                this.selectedMaterials.delete('Default');
                this.selectedMaterials.add(code);
            }

            this.updateSelectedMaterials();
            input.value = '';
            autocompleteDiv.style.display = 'none';
            
            // If we have results displayed, update them
            if (this.resultsTable.style.display === 'table') {
                this.handleSearch();
            }
        });

        // Close autocomplete when clicking outside
        document.addEventListener('click', (e) => {
            if (!input.contains(e.target) && !autocompleteDiv.contains(e.target)) {
                autocompleteDiv.style.display = 'none';
            }
        });
    }

    updateSelectedMaterials() {
        const selectedDiv = document.querySelector('.selected-materials');
        selectedDiv.innerHTML = Array.from(this.selectedMaterials)
            .map(code => `
                <span class="material-tag">
                    ${code}
                    <span class="remove" data-code="${code}">×</span>
                </span>
            `)
            .join('');

        // Add click handlers for remove buttons
        selectedDiv.querySelectorAll('.remove').forEach(btn => {
            btn.addEventListener('click', () => {
                const code = btn.dataset.code;
                this.selectedMaterials.delete(code);
                if (this.selectedMaterials.size === 0) {
                    this.selectedMaterials.add('Default');
                }
                this.updateSelectedMaterials();
                
                // If we have results displayed, update them
                if (this.resultsTable.style.display === 'table') {
                    this.handleSearch();
                }
            });
        });
    }

    setupMiningTypeAutocomplete() {
        const input = document.getElementById('miningTypeInput');
        const autocompleteDiv = document.getElementById('miningTypeAutocomplete');
        const selectedDiv = document.querySelector('.selected-mining-types');
        
        // Available mining types
        const miningTypes = ['All', 'Core', 'Laser Surface', 'Surface Deposit', 'Sub Surface Deposit'];
        input.setAttribute('autocomplete', 'off');
        
        // Set initial value
        this.selectedMiningTypes = new Set(['All']);
        this.updateSelectedMiningTypes();

        // Show dropdown when clicking in the input field
        input.addEventListener('click', () => {
            const matches = miningTypes.filter(type => !this.selectedMiningTypes.has(type));
            
            if (matches.length > 0) {
                autocompleteDiv.innerHTML = matches
                    .map(type => `<div class="autocomplete-item">${type}</div>`)
                    .join('');
                autocompleteDiv.style.display = 'block';
            }
        });
        
        input.addEventListener('input', () => {
            const value = input.value.toLowerCase();
            if (!value) {
                const matches = miningTypes.filter(type => !this.selectedMiningTypes.has(type));
                if (matches.length > 0) {
                    autocompleteDiv.innerHTML = matches
                        .map(type => `<div class="autocomplete-item">${type}</div>`)
                        .join('');
                    autocompleteDiv.style.display = 'block';
                } else {
                autocompleteDiv.style.display = 'none';
                }
                return;
            }
            
            const matches = miningTypes.filter(type => 
                type.toLowerCase().includes(value) && 
                !this.selectedMiningTypes.has(type)
            );

            if (matches.length > 0) {
                autocompleteDiv.innerHTML = matches
                    .map(type => `<div class="autocomplete-item">${type}</div>`)
                    .join('');
                autocompleteDiv.style.display = 'block';
            } else {
                autocompleteDiv.style.display = 'none';
            }
        });

        autocompleteDiv.addEventListener('click', (e) => {
            if (e.target.classList.contains('autocomplete-item')) {
                const type = e.target.textContent;
                
                // If "All" is selected, clear other selections
                if (type === 'All') {
                    this.selectedMiningTypes.clear();
                } else {
                    // If adding a specific type, remove "All"
                    this.selectedMiningTypes.delete('All');
                }
                
                this.selectedMiningTypes.add(type);
                this.updateSelectedMiningTypes();
                
                input.value = '';
                autocompleteDiv.style.display = 'none';
                
                // If we have results displayed, update them
                if (this.resultsTable.style.display === 'table') {
                    this.handleSearch();
                }
            }
        });

        // Close autocomplete when clicking outside
        document.addEventListener('click', (e) => {
            if (!input.contains(e.target) && !autocompleteDiv.contains(e.target)) {
                autocompleteDiv.style.display = 'none';
            }
        });
    }

    updateSelectedMiningTypes() {
        const selectedDiv = document.querySelector('.selected-mining-types');
        selectedDiv.innerHTML = Array.from(this.selectedMiningTypes)
            .map(type => `
                <span class="selected-item">
                    ${type}
                    <span class="remove-item" data-value="${type}">&times;</span>
                </span>
            `)
            .join('');
        
        // Add click handlers for remove buttons
        selectedDiv.querySelectorAll('.remove-item').forEach(button => {
            button.addEventListener('click', (e) => {
                const type = e.target.dataset.value;
                this.selectedMiningTypes.delete(type);
                
                // If no types are selected, default to "All"
                if (this.selectedMiningTypes.size === 0) {
                    this.selectedMiningTypes.add('All');
                }
                
                this.updateSelectedMiningTypes();
            });
        });
    }

    async search() {
        this.showLoading();
        try {
            const table = document.getElementById('resultsTable');
            table.className = 'results-table';  // Reset to default table class
            
            // Get form data
            const formData = new FormData(document.getElementById('searchForm'));
        } catch (error) {
            console.error('Error searching:', error);
            this.hideLoading();
        }
    }

    handlePowerSelection() {
        const powerSelect = document.getElementById('controlling_power');
        const powerGoalSelect = document.getElementById('power_goal');
        const opposingPowerSelect = document.getElementById('opposing_power');
        
        // If "Any" is selected
        if (!powerSelect.value || powerSelect.value === "Any" || powerSelect.value === "None") {
            powerGoalSelect.disabled = true;
            opposingPowerSelect.disabled = true;
            powerGoalSelect.value = "Reinforce"; // Default value
            opposingPowerSelect.value = "Any"; // Default value
        } else {
            powerGoalSelect.disabled = false;
            opposingPowerSelect.disabled = false;
        }
    }

}

// Initialize MiningSearch only if not explicitly skipped
document.addEventListener('DOMContentLoaded', () => {
    if (!window.skipMiningSearch) {
        window.miningSearch = new MiningSearch(document);
        window.miningSearch.init(document);
    }
});

function togglePriceReference() {
    const search = window.miningSearch;
    search.useMaxPrice = !search.useMaxPrice;
    
    // Update toggle appearance
    const toggleSwitch = document.querySelector('.toggle-switch');
    const avgToggle = document.getElementById('avgPriceToggle');
    const maxToggle = document.getElementById('maxPriceToggle');
    
    if (search.useMaxPrice) {
        toggleSwitch.classList.add('max');
        avgToggle.classList.remove('active');
        maxToggle.classList.add('active');
    } else {
        toggleSwitch.classList.remove('max');
        maxToggle.classList.remove('active');
        avgToggle.classList.add('active');
    }
    
    // Refresh results if they exist
    if (search.resultsTable.style.display === 'table') {
        if (search.resultsTable.querySelector('thead tr').cells.length === 10) {
            searchHighest();
        } else {
            search.handleSearch();
        }
    }
}

// Loading indicator functions
export function showLoading() {
    document.getElementById('loading').style.display = 'block';
}

export function hideLoading() {
    document.getElementById('loading').style.display = 'none';
}

export { MiningSearch, togglePriceReference };