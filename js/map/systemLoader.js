import { loadAndDecompress, checkStorageAvailability, getUncompressedSize, USE_LOCAL_STORAGE, STORAGE_KEY, STORAGE_META_KEY } from '/js/map/decompress.js';
import { globalState, POWER_DATA } from './globalState.js';
import { createPowerStateInstances, updateSpatialIndex, updateSystemPoints } from './systems.js';
import { waitForGeometries } from './powerStateShapes.js';
import { PowerVisualizerExtra } from '/js/map/visualizeExtra.js';

// Constants for data loading
const DEBUG_LOADING = false;  // Set to false to disable dummy loading

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
            // Step 1: Downloading
            const isGzipped = POWER_DATA.toLowerCase().endsWith('.gz');
            this.loadingDiv.textContent = `Downloading ${isGzipped ? 'compressed' : 'JSON'} data...`;
            
            // Try to get from local storage first if enabled
            if (USE_LOCAL_STORAGE && checkStorageAvailability()) {
                const storedMeta = localStorage.getItem(STORAGE_META_KEY);
                const storedData = localStorage.getItem(STORAGE_KEY);
                
                if (storedMeta && storedData) {
                    const meta = JSON.parse(storedMeta);
                    const response = await fetch(POWER_DATA, { method: 'HEAD' });
                    const currentSize = response.headers.get('content-length');
                    
                    if (meta.size === currentSize) {
                        console.log('Using cached data from local storage');
                        const data = JSON.parse(storedData);
                        this.loadingDiv.textContent = 'Loading cached data...';
                        await this.processSystemData(data.systems);
                        return;
                    }
                }
            }

            const response = await fetch(POWER_DATA);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            
            let data;
            if (isGzipped) {
                // Step 2: Decompression (only for .gz files)
                this.loadingDiv.textContent = 'Decompressing data...';
                const compressed = await response.arrayBuffer();
                const expectedSize = await getUncompressedSize(compressed);
                const decompressed = pako.inflate(new Uint8Array(compressed), { to: 'string' });
                
                if (decompressed.length !== expectedSize) {
                    console.warn('Decompressed size mismatch! Expected:', expectedSize, 'Got:', decompressed.length);
                }
                
                data = JSON.parse(decompressed);
            } else {
                data = await response.json();
            }

            // Store in local storage if enabled and available
            if (USE_LOCAL_STORAGE && checkStorageAvailability()) {
                try {
                    const meta = {
                        size: response.headers.get('content-length'),
                        timestamp: new Date().toISOString()
                    };
                    
                    localStorage.setItem(STORAGE_META_KEY, JSON.stringify(meta));
                    localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
                    console.log('Data cached in local storage');
                } catch (e) {
                    console.warn('Failed to cache in local storage:', e);
                }
            }

            // Step 3: Loading into scene
            this.loadingDiv.textContent = 'Loading data into scene...';
            await this.processSystemData(data.systems);
            
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

