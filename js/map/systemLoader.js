import { loadAndDecompress, checkStorageAvailability, getUncompressedSize, USE_LOCAL_STORAGE, STORAGE_KEY, STORAGE_META_KEY } from '/js/map/decompress.js';
import { globalState, POWER_DATA } from './globalState.js';
import { createPowerStateInstances, updateSpatialIndex, updateSystemPoints } from './systems.js';
import { waitForGeometries } from './powerStateShapes.js';
import { PowerVisualizerExtra } from '/js/map/visualizeExtra.js';

// Constants for data loading
const DEBUG_LOADING = false;  // Set to false to disable dummy loading

// Debug constants
console.log('%c Storage Constants:', 'background: #222; color: #ff5555; font-size: 14px;', {
    USE_LOCAL_STORAGE,
    STORAGE_KEY,
    STORAGE_META_KEY,
    POWER_DATA
});

// Class to handle system loading and state
export class SystemsLoader {
    constructor(scene, loadingDiv, circleGeometry, circleMaterial, showPowerIcons = true) {
        this.loadingDiv = loadingDiv;
        globalState.scene = scene;
        this.circleGeometry = circleGeometry;
        this.circleMaterial = circleMaterial;
        this.showPowerIcons = showPowerIcons;

        // Center the loading div
        this.loadingDiv.style.position = 'fixed';
        this.loadingDiv.style.top = '50%';
        this.loadingDiv.style.left = '50%';
        this.loadingDiv.style.transform = 'translate(-50%, -50%)';
        this.loadingDiv.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
        this.loadingDiv.style.padding = '20px';
        this.loadingDiv.style.borderRadius = '10px';
        this.loadingDiv.style.color = '#FFA500';
        this.loadingDiv.style.fontSize = '24px';
        this.loadingDiv.style.fontWeight = 'bold';
        this.loadingDiv.style.zIndex = '1000';
    }

    async loadAllSystemsFromJson() {
        this.loadingDiv.style.display = 'block';
        
        try {
            // Step 1: First check the .gz file stats
            const response = await fetch(POWER_DATA, { method: 'HEAD' });
            if (!response.ok) {
                throw new Error(`Failed to get file stats: ${response.status}`);
            }
            
            const fileSize = response.headers.get('content-length');
            const fileLastModified = response.headers.get('last-modified');
            
            console.log('%c Server File Stats:', 'background: #222; color: #bada55; font-size: 14px;', {
                path: POWER_DATA,
                size: fileSize + ' bytes',
                lastModified: fileLastModified
            });

            // Step 2: Check local storage
            const storedMeta = localStorage.getItem(STORAGE_META_KEY);
            let needsUpdate = false;
            
            if (storedMeta) {
                const meta = JSON.parse(storedMeta);
                console.log('%c Local Storage Status:', 'background: #222; color: #bada55; font-size: 14px;', {
                    size: meta.size + ' bytes',
                    timestamp: meta.timestamp
                });

                // Compare compressed file sizes and dates
                const sizeMatches = meta.size === fileSize;
                const storedTime = new Date(meta.timestamp);
                const fileTime = new Date(fileLastModified);
                const isNewer = fileTime > storedTime;
                const daysDiff = Math.floor((fileTime - storedTime) / (1000 * 60 * 60 * 24));

                if (isNewer || !sizeMatches) {
                    console.log('%c Local Storage outdated:', 'background: #222; color: #ff5555; font-size: 14px;',
                        `${isNewer ? 'Server file is ' + daysDiff + ' days newer' : 'Size mismatch'}`);
                    needsUpdate = true;
                } else {
                    console.log('%c Local Storage up-to-date:', 'background: #222; color: #bada55; font-size: 14px;',
                        `Data from ${meta.timestamp}`);
                    const storedData = localStorage.getItem(STORAGE_KEY);
                    if (storedData) {
                        const data = JSON.parse(storedData);
                        await this.processSystemData(data.systems);
                        return;
                    }
                }
            } else {
                console.log('%c Local Storage empty:', 'background: #222; color: #ff5555; font-size: 14px;',
                    'First time load');
                needsUpdate = true;
            }

            // Step 3: Load new data if needed
            if (needsUpdate) {
                console.log('%c Downloading new data from server...', 'background: #222; color: #ff5555; font-size: 14px;');
                const dataResponse = await fetch(POWER_DATA);
                if (!dataResponse.ok) throw new Error(`HTTP error! status: ${dataResponse.status}`);
                
                let data;
                if (POWER_DATA.toLowerCase().endsWith('.gz')) {
                    console.log('%c Decompressing .gz file:', 'background: #222; color: #ff5555; font-size: 14px;');
                    this.loadingDiv.textContent = 'Decompressing data...';
                    const compressed = await dataResponse.arrayBuffer();
                    console.log('- Compressed size:', compressed.byteLength + ' bytes');
                    
                    const expectedSize = await getUncompressedSize(compressed);
                    console.log('- Expected uncompressed size:', expectedSize + ' bytes');
                    
                    const decompressed = pako.inflate(new Uint8Array(compressed), { to: 'string' });
                    console.log('- Actual uncompressed size:', decompressed.length + ' bytes');
                    
                    if (decompressed.length !== expectedSize) {
                        console.warn('Decompressed size mismatch! Expected:', expectedSize, 'Got:', decompressed.length);
                    } else {
                        console.log('- Decompression successful, sizes match');
                    }
                    
                    data = JSON.parse(decompressed);
                    console.log('- JSON parsed successfully');
                } else {
                    data = await dataResponse.json();
                }

                // Update local storage with new data
                if (USE_LOCAL_STORAGE && checkStorageAvailability()) {
                    console.log('%c Attempting to update local storage:', 'background: #222; color: #ff5555; font-size: 14px;', {
                        USE_LOCAL_STORAGE,
                        storageAvailable: checkStorageAvailability(),
                        dataSize: JSON.stringify(data).length,
                        fileSize,
                        fileLastModified
                    });
                    
                    try {
                        const meta = {
                            size: fileSize,
                            timestamp: fileLastModified || new Date().toISOString()
                        };
                        
                        localStorage.setItem(STORAGE_META_KEY, JSON.stringify(meta));
                        localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
                        console.log('%c Local Storage updated successfully:', 'background: #222; color: #bada55; font-size: 14px;',
                            `New data from ${meta.timestamp}`);
                    } catch (e) {
                        console.error('Failed to update local storage:', e);
                        console.error('Error details:', {
                            errorName: e.name,
                            errorMessage: e.message,
                            errorStack: e.stack
                        });
                    }
                } else {
                    console.warn('%c Skipping local storage update:', 'background: #222; color: #ff5555; font-size: 14px;',
                        `USE_LOCAL_STORAGE=${USE_LOCAL_STORAGE}, storageAvailable=${checkStorageAvailability()}`);
                }

                // Process the new data
                await this.processSystemData(data.systems);
            }
            
        } catch (error) {
            console.error('Error loading systems:', error);
            this.loadingDiv.textContent = 'Error loading powerplay data';
        }
    }

    // Helper method to process system data
    async processSystemData(allSystems) {
        // Start dummy loading before processing systems
        await dummyLoading(this.loadingDiv);
        
        // Process all systems at once and assign instanceIds
        let exploitedCount = 0;
        let strongholdCount = 0;
        let strongholdCarrierCount = 0;
        let fortifiedCount = 0;
        let unoccupiedCount = 0;
        let contestedCount = 0;

        // First pass: count how many of each type we need
        allSystems.forEach(system => {
            if (system.power_state === 'Exploited') exploitedCount++;
            else if (system.power_state === 'Stronghold') {
                if (system.hasStrongholdCarrier) strongholdCarrierCount++;
                else strongholdCount++;
            }
            else if (system.power_state === 'Fortified') fortifiedCount++;
            else if (system.power_state === 'Contested') contestedCount++;
            else unoccupiedCount++;
        });

        // Reset counters for second pass
        globalState.nextExploitedId = 0;
        globalState.nextStrongholdId = 0;
        globalState.nextStrongholdCarrierId = 0;
        globalState.nextFortifiedId = 0;
        globalState.nextUnoccupiedId = 0;
        globalState.nextContestedId = 0;

        allSystems.forEach((system, index) => {
            const systemKey = system.name.toLowerCase();
            if (!globalState.systems[systemKey]) {
                // Store in Elite coordinates, but negate X to match Elite's East/West
                globalState.systems[systemKey] = {
                    name: system.name,  // Keep original mixed case
                    x: -parseFloat(system.x),  // Negate X to match Elite's coordinate system
                    y: parseFloat(system.y),
                    z: parseFloat(system.z),
                    controlling_power: system.controlling_power,
                    power_state: system.power_state,
                    powers_acquiring: system.powers_acquiring,
                    hasStrongholdCarrier: system.hasStrongholdCarrier || false,
                    hasText: false,
                    instanceId: index,
                    // Assign stable power state instance IDs
                    powerStateInstanceId: system.power_state === 'Exploited' ? globalState.nextExploitedId++ :
                        system.power_state === 'Stronghold' ? (system.hasStrongholdCarrier ? globalState.nextStrongholdCarrierId++ : globalState.nextStrongholdId++) :
                        system.power_state === 'Fortified' ? globalState.nextFortifiedId++ :
                        system.power_state === 'Contested' ? globalState.nextContestedId++ :
                        (system.power_state === 'Prepared' || system.power_state === 'InPrepareRadius') ? globalState.nextExpansionId++ :
                        globalState.nextUnoccupiedId++
                };
            }
        });

        // Make systems globally available for autocomplete
        window.systems = globalState.systems;

        // Initialize spatial index
        Object.values(globalState.systems).forEach(system => {
            updateSpatialIndex(system);
        });

        this.loadingDiv.style.display = 'none';
        console.log('All systems loaded from JSON');

        // Load power state geometries and wait for them to be ready
        console.log('Loading power state geometries...');
        globalState.geometries = await waitForGeometries();
        globalState.powerStateGeometriesLoaded = true;
        console.log('Power state geometries loaded');
        
        // Create power state instances since they're on by default
        createPowerStateInstances();
        
        // Force an update of system points
        updateSystemPoints();
        
        // Initialize power glow visualization
        PowerVisualizerExtra.powerGlow(globalState.systems, globalState.camera);
        
        // Add dummy loading at the end
        await dummyLoading(this.loadingDiv);
    }
}

// Add helper function for dummy loading
async function dummyLoading(loadingDiv) {
    if (!DEBUG_LOADING) return;
    

    // Start with quick progress to 40%
    for (let i = 0; i <= 40; i += 2) {
        loadingDiv.textContent = `${i}% loading of powerplay data complete`;
        await new Promise(resolve => setTimeout(resolve, 4));  // 200ms delay per step
    }
    
    // Slow down for the middle part
    for (let i = 42; i <= 75; i += 1) {
        loadingDiv.textContent = `${i}% loading of powerplay data complete`;
        await new Promise(resolve => setTimeout(resolve, 4));  // 300ms delay per step
    }
    
    // Even slower for the next part
    for (let i = 76; i <= 95; i += 1) {
        loadingDiv.textContent = `${i}% loading of powerplay data complete`;
        await new Promise(resolve => setTimeout(resolve, 10));  // 400ms delay per step
    }
    
    // Very slow for the last 5%
    for (let i = 96; i <= 100; i += 1) {
        loadingDiv.textContent = `${i}% loading of powerplay data complete`;
        await new Promise(resolve => setTimeout(resolve, 4));  // 1 second delay per step
    }
}

