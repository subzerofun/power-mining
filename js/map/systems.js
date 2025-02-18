import * as THREE from '../threejs/three.module.js';
import { TextGeometry } from '../threejs/jsm/geometries/TextGeometry.js';
import { PowerVisualizerExtra } from '/js/map/visualizeExtra.js';
import { getGeometryForPowerState, waitForGeometries, powerCapitals } from './powerStateShapes.js';
import { globalState, MAX_ZOOM_LEVEL, MIN_ZOOM_LEVEL, MAX_RADIUS, MIN_RADIUS, 
         FADE_START_ZOOM, FADE_MARGIN, TEXT_CUTOFF_FRONT, TEXT_CUTOFF_FAR, 
         STAR_VISIBLE_RADIUS, SPATIAL_GRID_SIZE, CHUNK_SIZE, MAX_TEXT_PER_FRAME } from './globalState.js';

// Add at the top after imports
const FPS_DEBUG = false;

function debugLog(type, ...args) {
    if (FPS_DEBUG) {
        if (type === 'time') {
            console.time(...args);
        } else if (type === 'timeEnd') {
            console.timeEnd(...args);
        } else {
            console.log(...args);
        }
    }
}

// These variables are shared with map.html
let camera = globalState.camera;
export let targetPoint = globalState.targetPoint;  // Export targetPoint so other modules can use it
let systems = globalState.systems;
let circleInstances = globalState.circleInstances;
let systemNameRadius;
let MAX_Y_BUBBLE;
let powerColors = globalState.powerColors;
let loadedChunks;
let systemWorker;
let font = globalState.font;
let scene = globalState.scene;
let textObjects = globalState.textObjects;

// Batch text creation to prevent stuttering
let pendingTextCreation = [];

let textVisibilityRadius = 15;
// Add movement state flag
export let isMovingToTarget = false;

// Add power state visualization state
export let showPowerStateIcons = true;  // Set to true by default

// Add instance meshes for power state shapes
let strongholdInstances = globalState.strongholdInstances;
let fortifiedInstances = globalState.fortifiedInstances;
let exploitedInstances = globalState.exploitedInstances;
let unoccupiedInstances = globalState.unoccupiedInstances;
let strongholdCarrierInstances = globalState.strongholdCarrierInstances;
let expansionInstances = globalState.expansionInstances;
let contestedInstances = globalState.contestedInstances;

// Add spatial indexing structures
const spatialIndex = globalState.spatialIndex;

// New function to create power state instances
let powerStateGeometriesLoaded = globalState.powerStateGeometriesLoaded;
let geometries = globalState.geometries;

// Add instance ID counters at the top of the file
let nextExploitedId = 0;
let nextStrongholdId = 0;
let nextStrongholdCarrierId = 0;
let nextFortifiedId = 0;
let nextUnoccupiedId = 0;
let nextExpansionId = 0;  // New counter for expansion instances
let nextContestedId = 0;

// Add configuration for system skipping
let skipSystemsWhenZoomedOut = true;  // Can be toggled
let systemSkipRatio = {
    HIGH_ZOOM: 1,      // No skipping below FADE_START_ZOOM
    MEDIUM_ZOOM: 2,    // Skip every 2nd system between FADE_START_ZOOM and MAX_ZOOM_LEVEL
    LOW_ZOOM: 2     // Skip every 3rd system above MAX_ZOOM_LEVEL
};

// Add at module level, near other constants
const hideMatrix = new THREE.Matrix4();  // Cache hide matrix once
const position = new THREE.Vector3();    // Reusable position vector
const scale = new THREE.Vector3();       // Reusable scale vector
const matrix = new THREE.Matrix4();      // Reusable matrix
const baseColor = new THREE.Color();     // Reusable color object
const hidePosition = new THREE.Vector3(100000, 100000, 100000);  // Cached hide position

// Color cache using Map for O(1) lookup - more efficient than object properties
const systemColorCache = new Map();  // Cache colors by system ID

// Cache instance arrays for batch updates
const instanceArrays = {
    stronghold: null,
    strongholdCarrier: null,
    fortified: null,
    exploited: null,
    unoccupied: null,
    expansion: null,
    contested: null
};

// Add at module level, near other constants
const GRID_LEVELS = [500, 100, 20]; // Hierarchical grid sizes
const gridCaches = GRID_LEVELS.map(() => new Map()); // Cache for each level

// Add at module level, near other constants
const activeInstances = {
    stronghold: new Set(),
    strongholdCarrier: new Set(),
    fortified: new Set(),
    exploited: new Set(),
    unoccupied: new Set(),
    expansion: new Set(),
    contested: new Set()
};

async function loadPowerStateGeometries() {
    if (!globalState.powerStateGeometriesLoaded) {
        globalState.geometries = await waitForGeometries();
        geometries = globalState.geometries;
        globalState.powerStateGeometriesLoaded = true;
        powerStateGeometriesLoaded = true;
    }
}

export function createPowerStateInstances() {
    if (globalState.strongholdInstances || !globalState.powerStateGeometriesLoaded || !globalState.geometries) return;

    // Reset instance ID counters
    globalState.nextExploitedId = 0;
    globalState.nextStrongholdId = 0;
    globalState.nextStrongholdCarrierId = 0;
    globalState.nextFortifiedId = 0;
    globalState.nextUnoccupiedId = 0;
    globalState.nextExpansionId = 0;
    globalState.nextContestedId = 0;

    // Count how many instances we need of each type
    let exploitedCount = 0;
    let strongholdCount = 0;
    let strongholdCarrierCount = 0;
    let fortifiedCount = 0;
    let unoccupiedCount = 0;
    let expansionCount = 0;  // New counter for expansion systems
    let contestedCount = 0;

    // Count instances needed for each type
    Object.values(systems).forEach(system => {
        // Skip capital systems
        if (powerCapitals.isCapitalSystem(system.id64)) {
            return;
        }

        if (system.power_state === 'Exploited') exploitedCount++;
        else if (system.power_state === 'Stronghold') {
            if (system.hasStrongholdCarrier) strongholdCarrierCount++;
            else strongholdCount++;
        }
        else if (system.power_state === 'Fortified') fortifiedCount++;
        else if (system.power_state === 'Contested') contestedCount++;
        else if (system.power_state === 'Prepared' || system.power_state === 'InPrepareRadius') expansionCount++;
        else unoccupiedCount++;
    });

    console.log('Instance counts:', {
        exploited: exploitedCount,
        stronghold: strongholdCount,
        strongholdCarrier: strongholdCarrierCount,
        fortified: fortifiedCount,
        unoccupied: unoccupiedCount,
        expansion: expansionCount,
        contested: contestedCount
    });

    // Create instance meshes for each power state using the actual counts
    const defaultMaterial = new THREE.MeshBasicMaterial({ 
        color: 0xffffff,
        transparent: true,
        depthTest: false,
        side: THREE.DoubleSide
    });

    // Create instances for each power state with their actual needed counts
    globalState.strongholdInstances = new THREE.InstancedMesh(getGeometryForPowerState('Stronghold'), defaultMaterial.clone(), strongholdCount);
    globalState.strongholdCarrierInstances = new THREE.InstancedMesh(getGeometryForPowerState('Stronghold-Carrier'), defaultMaterial.clone(), strongholdCarrierCount);
    globalState.fortifiedInstances = new THREE.InstancedMesh(getGeometryForPowerState('Fortified'), defaultMaterial.clone(), fortifiedCount);
    globalState.exploitedInstances = new THREE.InstancedMesh(getGeometryForPowerState('Exploited'), defaultMaterial.clone(), exploitedCount);
    globalState.unoccupiedInstances = new THREE.InstancedMesh(getGeometryForPowerState('Unoccupied'), defaultMaterial.clone(), unoccupiedCount);
    globalState.expansionInstances = new THREE.InstancedMesh(getGeometryForPowerState('Expansion'), defaultMaterial.clone(), expansionCount);
    globalState.contestedInstances = new THREE.InstancedMesh(getGeometryForPowerState('Contested'), defaultMaterial.clone(), contestedCount);

    // Disable frustum culling for all instances since they're spread across space
    globalState.strongholdInstances.frustumCulled = false;
    globalState.strongholdCarrierInstances.frustumCulled = false;
    globalState.fortifiedInstances.frustumCulled = false;
    globalState.exploitedInstances.frustumCulled = false;
    globalState.unoccupiedInstances.frustumCulled = false;
    globalState.expansionInstances.frustumCulled = false;
    globalState.contestedInstances.frustumCulled = false;

    // Set render order to ensure icons render on top (higher than everything else)
    globalState.strongholdInstances.renderOrder = 9999;
    globalState.strongholdCarrierInstances.renderOrder = 9999;
    globalState.fortifiedInstances.renderOrder = 9999;
    globalState.exploitedInstances.renderOrder = 9999;
    globalState.unoccupiedInstances.renderOrder = 9999;
    globalState.expansionInstances.renderOrder = 9999;
    globalState.contestedInstances.renderOrder = 9999;

    // Also ensure they're not depth tested to always be on top
    globalState.strongholdInstances.material.depthTest = false;
    globalState.strongholdCarrierInstances.material.depthTest = false;
    globalState.fortifiedInstances.material.depthTest = false;
    globalState.exploitedInstances.material.depthTest = false;
    globalState.unoccupiedInstances.material.depthTest = false;
    globalState.expansionInstances.material.depthTest = false;
    globalState.contestedInstances.material.depthTest = false;

    // Create default matrix for hiding instances
    const defaultMatrix = new THREE.Matrix4();
    defaultMatrix.makeScale(0.00001, 0.00001, 0.00001);
    defaultMatrix.setPosition(new THREE.Vector3(100000, 100000, 100000));

    // Initialize each instance type with its own count
    for (let i = 0; i < strongholdCount; i++) {
        globalState.strongholdInstances.setMatrixAt(i, defaultMatrix);
    }
    for (let i = 0; i < strongholdCarrierCount; i++) {
        globalState.strongholdCarrierInstances.setMatrixAt(i, defaultMatrix);
    }
    for (let i = 0; i < fortifiedCount; i++) {
        globalState.fortifiedInstances.setMatrixAt(i, defaultMatrix);
    }
    for (let i = 0; i < exploitedCount; i++) {
        globalState.exploitedInstances.setMatrixAt(i, defaultMatrix);
    }
    for (let i = 0; i < unoccupiedCount; i++) {
        globalState.unoccupiedInstances.setMatrixAt(i, defaultMatrix);
    }
    for (let i = 0; i < expansionCount; i++) {
        globalState.expansionInstances.setMatrixAt(i, defaultMatrix);
    }
    for (let i = 0; i < contestedCount; i++) {
        globalState.contestedInstances.setMatrixAt(i, defaultMatrix);
    }

    // Mark instance matrices as needing update
    globalState.strongholdInstances.instanceMatrix.needsUpdate = true;
    globalState.strongholdCarrierInstances.instanceMatrix.needsUpdate = true;
    globalState.fortifiedInstances.instanceMatrix.needsUpdate = true;
    globalState.exploitedInstances.instanceMatrix.needsUpdate = true;
    globalState.unoccupiedInstances.instanceMatrix.needsUpdate = true;
    globalState.expansionInstances.instanceMatrix.needsUpdate = true;
    globalState.contestedInstances.instanceMatrix.needsUpdate = true;

    // Add all instance types to the scene
    scene.add(globalState.exploitedInstances);
    scene.add(globalState.strongholdInstances);
    scene.add(globalState.strongholdCarrierInstances);
    scene.add(globalState.fortifiedInstances);
    scene.add(globalState.unoccupiedInstances);
    scene.add(globalState.expansionInstances);
    scene.add(globalState.contestedInstances);

    // Ensure power state instances are visible
    globalState.exploitedInstances.visible = true;
    globalState.strongholdInstances.visible = true;
    globalState.strongholdCarrierInstances.visible = true;
    globalState.fortifiedInstances.visible = true;
    globalState.unoccupiedInstances.visible = true;
    globalState.expansionInstances.visible = true;
    globalState.contestedInstances.visible = true;

    // Hide circle instances since power icons are on by default
    if (circleInstances) {
        circleInstances.visible = false;
    }

    console.log('Created power state instances');
}

function processPendingText() {
    if (pendingTextCreation.length === 0) return;
    
    // Process a limited number of text objects per frame
    const batch = pendingTextCreation.splice(0, MAX_TEXT_PER_FRAME);
    batch.forEach(system => createSystemText(system));
    
    // Schedule next batch if there are more pending
    if (pendingTextCreation.length > 0) {
        requestAnimationFrame(processPendingText);
    }
}

function queueTextCreation(system) {
    if (!system.hasText && !pendingTextCreation.includes(system)) {
        pendingTextCreation.push(system);
        if (pendingTextCreation.length === 1) {
            requestAnimationFrame(processPendingText);
        }
    }
}

// Initialize module with references to shared state
export function initSystems(deps) {
    // Update both local and global state
    globalState.camera = deps.camera;
    globalState.targetPoint = deps.targetPoint;
    globalState.systems = deps.systems;
    globalState.powerColors = deps.powerColors;
    globalState.font = deps.font;
    globalState.scene = deps.scene;
    globalState.textObjects = deps.textObjects || [];

    // Update local references
    camera = globalState.camera;
    targetPoint = globalState.targetPoint;
    systems = globalState.systems;
    systemNameRadius = deps.systemNameRadius;
    textVisibilityRadius = deps.systemNameRadius;  // Set text visibility radius to match system name radius
    MAX_Y_BUBBLE = deps.MAX_Y_BUBBLE;
    powerColors = globalState.powerColors;
    loadedChunks = deps.loadedChunks;
    systemWorker = deps.systemWorker;
    font = globalState.font;
    scene = globalState.scene;
    textObjects = globalState.textObjects;

    // Create circle instances
    const circleGeometry = new THREE.CircleGeometry(0.65, 16);
    const circleMaterial = new THREE.MeshBasicMaterial({ 
        side: THREE.DoubleSide,
        transparent: true,
        opacity: 1.0,
        depthWrite: false
    });

    // Create instanced mesh for all systems
    const maxInstances = 26000;  // Set a reasonable maximum
    globalState.circleInstances = new THREE.InstancedMesh(circleGeometry, circleMaterial, maxInstances);
    circleInstances = globalState.circleInstances;
    circleInstances.renderOrder = 1;
    scene.add(circleInstances);

    // Initialize all instances with default positions and colors
    const hideMatrix = new THREE.Matrix4();
    hideMatrix.makeScale(0.00001, 0.00001, 0.00001);
    hideMatrix.setPosition(new THREE.Vector3(100000, 100000, 100000));

    const defaultColor = new THREE.Color(0xc0c0c0);
    for (let i = 0; i < maxInstances; i++) {
        circleInstances.setMatrixAt(i, hideMatrix);
        circleInstances.setColorAt(i, defaultColor);
    }

    // Mark matrices as needing update
    circleInstances.instanceMatrix.needsUpdate = true;
    if (circleInstances.instanceColor) circleInstances.instanceColor.needsUpdate = true;

    // Start loading power state geometries
    loadPowerStateGeometries();

    // Try to initialize power glow now that we have targetPoint
    PowerVisualizerExtra.initializeSystems(systems, camera);
}

// Function to create text label for a system
function createSystemText(system) {
    if (isMovingToTarget) return;
    if (!font || system.hasText) return;

    // Check zoom level - only create text when zoomed in closer than 80
    const zoomLevel = camera.position.distanceTo(targetPoint);
    if (zoomLevel > 80) return;

    // Check if system is within textVisibilityRadius
    const systemPos = new THREE.Vector3(system.x, system.y, system.z);
    const distanceToTarget = targetPoint.distanceTo(systemPos);
    if (textVisibilityRadius > 0 && distanceToTarget > textVisibilityRadius) return;

    // Create text geometry and material only once per system
    const textMaterial = new THREE.MeshBasicMaterial({ 
        color: 0xffffff, 
        depthTest: false,
        transparent: true,
        opacity: 1.0
    });

    // Use stored uppercase name
    const textGeometry = new TextGeometry("   " + system.name.toUpperCase(), {
        font: font,
        size: 2.0,
        depth: 0.1,
        curveSegments: 3,
        bevelEnabled: false
    });

    textGeometry.computeBoundingBox();
    const textHeight = textGeometry.boundingBox.max.y - textGeometry.boundingBox.min.y;
    
    // Calculate spacing based on distance to camera
    const calculateSpacing = (systemPos) => {
        const distanceToCamera = camera.position.distanceTo(systemPos);
        const minDistance = 20;  // When closer than this, max spacing
        const maxDistance = 100; // When further than this, min spacing
        const minSpacing = 1; 
        const maxSpacing = 2;

        const clampedDistance = Math.max(minDistance, Math.min(maxDistance, distanceToCamera));
        const factor = 1 - (clampedDistance - minDistance) / (maxDistance - minDistance);
        return minSpacing + (maxSpacing - minSpacing) * factor;
    };
    
    let spacing = calculateSpacing(systemPos);

    // Center text vertically and position it to the right of the star
    textGeometry.translate(spacing, -textHeight/2, 0);
    
    // Create mesh and group
    const textMesh = new THREE.Mesh(textGeometry, textMaterial);
    const textGroup = new THREE.Group();
    textGroup.position.copy(new THREE.Vector3(system.x, system.y, system.z));
    textGroup.renderOrder = 3; // Ensure text renders on top
    
    // Add to scene
    textGroup.add(textMesh);
    scene.add(textGroup);
    
    // Store text object with complete system data
    textObjects.push({ 
        mesh: textMesh, 
        group: textGroup,
        position: new THREE.Vector3(system.x, system.y, system.z),
        textHeight, 
        systemName: system.name,
        systemCoords: {
            x: system.x,
            y: system.y,
            z: system.z
        },
        debugPlane: null  // Initialize debugPlane property
    });
    
    system.hasText = true;
    //console.log('Created text for system:', system.name); // Debug log
}

// Add these helper functions to track text object stats
export function getTextObjectStats() {
    if (!textObjects) return { total: 0, visible: 0 };
    
    const total = textObjects.length;
    const visible = textObjects.filter(obj => obj.group && obj.group.visible).length;
    
    return { total, visible };
}

export function updateTextVisibility() {
    if (!textObjects) return;

    textObjects.forEach(({ mesh, group, position }) => {
        const distanceToCenter = targetPoint.distanceTo(position);
        const distanceToCamera = camera.position.distanceTo(position);
        const maxYDistance = systemNameRadius * MAX_Y_BUBBLE;
        
        // Check all visibility conditions
        const inYBubble = isInYBubble(position.y, targetPoint.y, maxYDistance);
        const inMainRadius = distanceToCenter <= systemNameRadius;
        const inExtendedRadius = TEXT_CUTOFF_FAR > 0 && 
                               distanceToCenter <= (systemNameRadius + TEXT_CUTOFF_FAR);
        
        if (inYBubble && (inMainRadius || inExtendedRadius)) {
            // Text size constants
            const BASE_TEXT_SIZE = 0.7;  // Standard fixed size
            
            // Calculate front/back scale factor based on distance from target point
            const targetToCamera = camera.position.clone().sub(targetPoint);
            const targetToText = position.clone().sub(targetPoint);
            const relativeDistance = targetToText.dot(targetToCamera.normalize());

            // Calculate base scale that counteracts perspective projection
            let scaleFactor = BASE_TEXT_SIZE * (distanceToCamera / 100);
            
            // Calculate scale based on front/back position
            const maxRelativeDistance = systemNameRadius;
            const relativeFactor = Math.min(Math.abs(relativeDistance) / maxRelativeDistance, 1.0);
            const frontBackScale = relativeDistance > 0 
                ? THREE.MathUtils.lerp(1.0, 1.25, relativeFactor)  // Scale up to 1.5x for text in front
                : THREE.MathUtils.lerp(1.0, 0.75, relativeFactor); // Scale down to 0.75x for text behind
            
            scaleFactor *= frontBackScale;
            mesh.scale.set(scaleFactor, scaleFactor, scaleFactor);
            
            // Calculate opacity based on distance from target point
            let opacity = THREE.MathUtils.mapLinear(
                distanceToCenter,
                10,  // Start fade at 2 ly
                20, // Complete fade by 40 ly
                1.0,
                0.0
            );
            opacity = Math.max(0, Math.min(1, opacity));  // Clamp between 0 and 1
            
            mesh.material.opacity = opacity;
            group.visible = opacity > 0.01;
            group.quaternion.copy(camera.quaternion);
        } else {
            group.visible = false;
        }
    });
}

// Helper to get grid cell key for a position at specific level
function getGridKey(x, y, z, gridSize) {
    const gx = Math.floor(x / gridSize);
    const gy = Math.floor(y / gridSize);
    const gz = Math.floor(z / gridSize);
    return `${gx},${gy},${gz}`;
}

// Update spatial index when systems are loaded
export function updateSpatialIndex(system) {
    // Add system to all grid levels
    GRID_LEVELS.forEach((gridSize, level) => {
        const key = getGridKey(system.x, system.y, system.z, gridSize);
        if (!gridCaches[level].has(key)) {
            gridCaches[level].set(key, new Set());
        }
        gridCaches[level].get(key).add(system);
    });
}

// Get systems within radius of a point using hierarchical spatial index
function getSystemsInRadius(center, radius) {
    const systems = new Set();
    const radiusSquared = radius * radius;
    
    // Find appropriate grid level based on radius
    let levelIndex = GRID_LEVELS.length - 1; // Start with smallest grid size by default
    for (let i = 0; i < GRID_LEVELS.length; i++) {
        if (radius > GRID_LEVELS[i] * 2) {
            levelIndex = i;
            break;
        }
    }
    
    // Always use smallest grid size when zoomed in close
    const zoomLevel = camera.position.distanceTo(targetPoint);
    if (zoomLevel <= 50) {
        levelIndex = GRID_LEVELS.length - 1;
    }
    
    const gridSize = GRID_LEVELS[levelIndex];
    const gridCache = gridCaches[levelIndex];
    
    // Calculate grid bounds
    const minX = Math.floor((center.x - radius) / gridSize);
    const maxX = Math.floor((center.x + radius) / gridSize);
    const minY = Math.floor((center.y - radius) / gridSize);
    const maxY = Math.floor((center.y + radius) / gridSize);
    const minZ = Math.floor((center.z - radius) / gridSize);
    const maxZ = Math.floor((center.z + radius) / gridSize);
    
    // Cache center coordinates
    const cx = center.x;
    const cy = center.y;
    const cz = center.z;
    
    // Reusable vectors for distance calculations
    const cellCenter = new THREE.Vector3();
    const cellToCenter = new THREE.Vector3();
    
    // Check each grid cell
    for (let x = minX; x <= maxX; x++) {
        cellCenter.x = x * gridSize + gridSize/2;
        const dx = cellCenter.x - cx;
        const dx2 = dx * dx;
        if (dx2 > radiusSquared * 1.5) continue;
        
        for (let y = minY; y <= maxY; y++) {
            cellCenter.y = y * gridSize + gridSize/2;
            const dy = cellCenter.y - cy;
            const dy2 = dy * dy;
            const dxy2 = dx2 + dy2;
            if (dxy2 > radiusSquared * 1.5) continue;
            
            for (let z = minZ; z <= maxZ; z++) {
                cellCenter.z = z * gridSize + gridSize/2;
                const dz = cellCenter.z - cz;
                const dz2 = dz * dz;
                const dist2 = dxy2 + dz2;
                
                // Skip cell if too far
                if (dist2 > radiusSquared * 1.5) continue;
                
                // Get systems in this cell
                const key = `${x},${y},${z}`;
                const cellSystems = gridCache.get(key);
                if (!cellSystems) continue;
                
                // Process systems in cell
                cellSystems.forEach(system => {
                    const dx = system.x - cx;
                    const dy = system.y - cy;
                    const dz = system.z - cz;
                    const distSquared = dx * dx + dy * dy + dz * dz;
                    if (distSquared <= radiusSquared) {
                        systems.add(system);
                    }
                });
            }
        }
    }
    
    return Array.from(systems);
}

// Add function to toggle system skipping
export function toggleSystemSkipping(enabled) {
    skipSystemsWhenZoomedOut = enabled;
    updateSystemPoints();
}

// Modify updateSystemPoints to use skip ratio while keeping existing code
export function updateSystemPoints() {
    if (!circleInstances || !systems) return;

    debugLog('time', 'Total updateSystemPoints');

    const zoomLevel = camera.position.distanceTo(targetPoint);
    
    debugLog('time', 'Spatial Culling');
    // 1. Spatial Culling with Skip Ratio
    let visibilityRadius;
    if (zoomLevel >= MAX_ZOOM_LEVEL) {
        visibilityRadius = MAX_RADIUS;
    } else if (zoomLevel <= MIN_ZOOM_LEVEL) {
        visibilityRadius = MIN_RADIUS;
    } else {
        const t = Math.pow((zoomLevel - MIN_ZOOM_LEVEL) / (MAX_ZOOM_LEVEL - MIN_ZOOM_LEVEL), 2);
        visibilityRadius = MIN_RADIUS + (MAX_RADIUS - MIN_RADIUS) * t;
    }

    // Get visible systems using spatial index
    const visibleSystems = getSystemsInRadius(targetPoint, visibilityRadius);
    debugLog('timeEnd', 'Spatial Culling');
    debugLog('log', 'Visible systems:', visibleSystems.length);

    let processedCount = 0;
    // 2. Use cached hide matrix - only set position once
    hideMatrix.makeScale(0.00001, 0.00001, 0.00001);
    hideMatrix.setPosition(hidePosition);

    // Initialize instance arrays for batch updates if needed
    if (showPowerStateIcons && globalState.strongholdInstances && !instanceArrays.stronghold) {
        instanceArrays.stronghold = globalState.strongholdInstances;
        instanceArrays.strongholdCarrier = globalState.strongholdCarrierInstances;
        instanceArrays.fortified = globalState.fortifiedInstances;
        instanceArrays.exploited = globalState.exploitedInstances;
        instanceArrays.unoccupied = globalState.unoccupiedInstances;
        instanceArrays.expansion = globalState.expansionInstances;
        instanceArrays.contested = globalState.contestedInstances;
    }

    debugLog('time', 'Hide Matrices');
    // Hide only previously active instances
    if (showPowerStateIcons && instanceArrays.stronghold) {
        // Store current active instances to hide
        const previousActive = {};
        Object.keys(activeInstances).forEach(type => {
            previousActive[type] = new Set(activeInstances[type]);
            activeInstances[type].clear();
        });

        // Hide previously active instances that are no longer visible
        Object.entries(previousActive).forEach(([type, indices]) => {
            const instance = instanceArrays[type];
            if (instance) {
                indices.forEach(idx => {
                    instance.setMatrixAt(idx, hideMatrix);
                });
            }
        });
    } else if (circleInstances) {
        // For circle instances, use a similar approach
        const previousActive = new Set(activeInstances.circle || new Set());
        activeInstances.circle = new Set();
        previousActive.forEach(idx => {
            circleInstances.setMatrixAt(idx, hideMatrix);
        });
    }
    debugLog('timeEnd', 'Hide Matrices');

    // Determine skip ratio based on zoom level
    let currentSkipRatio = 1;
    if (skipSystemsWhenZoomedOut) {
        if (zoomLevel > MAX_ZOOM_LEVEL) {
            currentSkipRatio = systemSkipRatio.LOW_ZOOM;
        } else if (zoomLevel > FADE_START_ZOOM) {
            currentSkipRatio = systemSkipRatio.MEDIUM_ZOOM;
        }
    }

    debugLog('time', 'Process Systems');
    let colorCacheHits = 0;
    let colorCacheMisses = 0;
    let skippedSystems = 0;
    let processedSystems = 0;

    // Process visible systems
    visibleSystems.forEach(system => {
        if (system.instanceId === undefined) return;

        // Apply skip ratio early to avoid unnecessary processing
        processedCount++;
        
        // Only skip non-important systems when zoomed out
        const isImportantSystem = system.power_state === 'Stronghold' || 
                                system.power_state === 'Contested' ||
                                system.power_state === 'Fortified';
                                
        if (currentSkipRatio > 1 && !isImportantSystem && processedCount % currentSkipRatio !== 0) {
            skippedSystems++;
            return;
        }

        processedSystems++;

        // Reuse position vector and calculate distances once
        position.set(system.x, system.y, system.z);
        const distanceToCamera = camera.position.distanceTo(position);
        const distanceToTarget = position.distanceTo(targetPoint);

        // Check for text creation
        if (!isMovingToTarget && !system.hasText && 
            distanceToTarget <= textVisibilityRadius &&
            zoomLevel <= 80) {  // Only create text when zoomed in enough
            queueTextCreation(system);
        }

        // Early exit if system is too far
        if (distanceToTarget > visibilityRadius) {
            skippedSystems++;
            return;
        }

        // Color caching - only calculate if not cached or power state changed
        const colorCacheKey = `${system.id64}_${system.power_state}_${system.controlling_power}`;
        let cachedColor = systemColorCache.get(colorCacheKey);
        
        if (!cachedColor) {
            colorCacheMisses++;
            if (system.power_state === 'Contested') {
                baseColor.setHex(powerColors[system.controlling_power] || 0xc0c0c0);
            } else if (system.power_state === 'Prepared' || system.power_state === 'InPrepareRadius' || !system.controlling_power) {
                baseColor.setHex(0xc0c0c0);
            } else if (system.controlling_power && powerColors[system.controlling_power]) {
                baseColor.setHex(powerColors[system.controlling_power]);
            } else {
                baseColor.setHex(0xc0c0c0);
            }
            systemColorCache.set(colorCacheKey, baseColor.getHex());
            cachedColor = baseColor.getHex();
        } else {
            colorCacheHits++;
        }

        baseColor.setHex(cachedColor);

        // Calculate opacity
        let opacity = 1.0;
        if (distanceToTarget > STAR_VISIBLE_RADIUS) {
            opacity = 1 - ((distanceToTarget - STAR_VISIBLE_RADIUS) / (visibilityRadius - STAR_VISIBLE_RADIUS));
        }
        
        baseColor.multiplyScalar(opacity);

        // Calculate scale using reusable vector
        const baseScale = showPowerStateIcons ? 0.4 : 0.6;
        const scaleFactor = showPowerStateIcons ? 
            baseScale : 
            THREE.MathUtils.clamp(distanceToCamera / 200, baseScale * 0.6, baseScale);
        scale.set(scaleFactor, scaleFactor, scaleFactor);

        matrix.compose(position, camera.quaternion, scale);

        // Update appropriate instance
        if (showPowerStateIcons && instanceArrays.stronghold) {
            let targetInstances = null;

            switch(system.power_state) {
                case 'Exploited': targetInstances = instanceArrays.exploited; break;
                case 'Stronghold': targetInstances = system.hasStrongholdCarrier ? 
                    instanceArrays.strongholdCarrier : instanceArrays.stronghold; break;
                case 'Fortified': targetInstances = instanceArrays.fortified; break;
                case 'Contested': targetInstances = instanceArrays.contested; break;
                case 'Prepared':
                case 'InPrepareRadius': targetInstances = instanceArrays.expansion; break;
                default: targetInstances = instanceArrays.unoccupied;
            }

            if (targetInstances) {
                targetInstances.setMatrixAt(system.powerStateInstanceId, matrix);
                targetInstances.setColorAt(system.powerStateInstanceId, baseColor);
                // Track active instance
                const instanceType = getInstanceType(system.power_state, system.hasStrongholdCarrier);
                activeInstances[instanceType].add(system.powerStateInstanceId);
            } else {
                circleInstances.setMatrixAt(system.instanceId, matrix);
                circleInstances.setColorAt(system.instanceId, baseColor);
                // Track active circle instance
                if (!activeInstances.circle) activeInstances.circle = new Set();
                activeInstances.circle.add(system.instanceId);
            }
        } else {
            circleInstances.setMatrixAt(system.instanceId, matrix);
            circleInstances.setColorAt(system.instanceId, baseColor);
            // Track active circle instance
            if (!activeInstances.circle) activeInstances.circle = new Set();
            activeInstances.circle.add(system.instanceId);
        }
    });
    debugLog('timeEnd', 'Process Systems');
    debugLog('log', 'Performance stats:', {
        processedSystems,
        skippedSystems,
        colorCacheHits,
        colorCacheMisses,
        cacheHitRate: colorCacheHits / (colorCacheHits + colorCacheMisses)
    });

    debugLog('time', 'Matrix Updates');
    // Batch update all instance matrices at once
    if (showPowerStateIcons && instanceArrays.stronghold) {
        Object.values(instanceArrays).forEach(instance => {
            if (instance) {
                instance.instanceMatrix.needsUpdate = true;
                if (instance.instanceColor) instance.instanceColor.needsUpdate = true;
            }
        });
    } else {
        circleInstances.instanceMatrix.needsUpdate = true;
        if (circleInstances.instanceColor) circleInstances.instanceColor.needsUpdate = true;
    }
    debugLog('timeEnd', 'Matrix Updates');
    debugLog('timeEnd', 'Total updateSystemPoints');
}

export function getChunkKey(x, y, z) {
    const chunkX = Math.floor(x / CHUNK_SIZE);
    const chunkY = Math.floor(y / CHUNK_SIZE);
    const chunkZ = Math.floor(z / CHUNK_SIZE);
    return `${chunkX},${chunkY},${chunkZ}`;
}

// Keep loadSystemsInView as a stub for potential future use
export function loadSystemsInView() {
    // All systems are loaded at startup
    return;
}

// Helper function
export function isInYBubble(systemY, targetY, maxYDistance) {
   return Math.abs(systemY - targetY) <= maxYDistance;
}

// Function to determine if a star should be visible based on zoom level and distance from target
export function isStarVisible(starPosition, targetPoint, zoomLevel) {
    // At high zoom levels, show all stars
    if (zoomLevel >= MAX_ZOOM_LEVEL) return true;
    
    // Calculate the current visibility radius based on zoom level
    let visibilityRadius;
    if (zoomLevel <= MIN_ZOOM_LEVEL) {
        visibilityRadius = MIN_RADIUS;
    } else if (zoomLevel >= FADE_START_ZOOM) {
        visibilityRadius = MAX_RADIUS;
    } else {
        // Interpolate radius between MIN_RADIUS and MAX_RADIUS based on zoom level
        const t = (zoomLevel - MIN_ZOOM_LEVEL) / (FADE_START_ZOOM - MIN_ZOOM_LEVEL);
        visibilityRadius = MIN_RADIUS + (MAX_RADIUS - MIN_RADIUS) * t;
    }
    
    // Check if star is within the calculated radius from target
    const distanceToTarget = starPosition.distanceTo(targetPoint);
    return distanceToTarget <= visibilityRadius;
}


// Export function to set movement state
export function setMovingToTarget(state) {
    isMovingToTarget = state;
    if (state) {
        // When starting movement, clear any pending text creation
        pendingTextCreation = [];
    }
}

// Add toggle function for power state icons
export function togglePowerStateIcons(show) {
    showPowerStateIcons = show;
    
    // Try to create power state instances if needed
    if (show && !globalState.strongholdInstances && powerStateGeometriesLoaded) {
        createPowerStateInstances();
    }
    
    // Update visibility of power state instances
    if (globalState.strongholdInstances) {
        globalState.strongholdInstances.visible = show;
        globalState.strongholdCarrierInstances.visible = show;
        globalState.fortifiedInstances.visible = show;
        globalState.exploitedInstances.visible = show;
        globalState.unoccupiedInstances.visible = show;
        globalState.expansionInstances.visible = show;
        globalState.contestedInstances.visible = show;
    }
    
    // Always show circle instances when power icons are off
    if (circleInstances) {
        circleInstances.visible = !show;
    }
    
    // Force update of all system points
    updateSystemPoints();
}

// Export function to get visible systems for other modules
export function getVisibleSystems() {
    const zoomLevel = camera.position.distanceTo(targetPoint);
    const visibilityRadius = zoomLevel <= 50 ? 50 : 
                           zoomLevel <= 100 ? 100 : 
                           STAR_VISIBLE_RADIUS;
    return getSystemsInRadius(targetPoint, visibilityRadius);
}

// Export function to get circleInstances
export function getCircleInstances() {
    return circleInstances;
}

// Helper function to get instance type
function getInstanceType(powerState, hasStrongholdCarrier) {
    switch(powerState) {
        case 'Exploited': return 'exploited';
        case 'Stronghold': return hasStrongholdCarrier ? 'strongholdCarrier' : 'stronghold';
        case 'Fortified': return 'fortified';
        case 'Contested': return 'contested';
        case 'Prepared':
        case 'InPrepareRadius': return 'expansion';
        default: return 'unoccupied';
    }
}