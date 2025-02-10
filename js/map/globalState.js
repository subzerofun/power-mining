// Global state object to share across modules
export const globalState = {
    // Core state
    systems: {},  // Main systems data
    camera: null,
    targetPoint: null,
    scene: null,
    font: null,
    powerColors: null,

    // Instance counters
    nextExploitedId: 0,
    nextStrongholdId: 0,
    nextStrongholdCarrierId: 0,
    nextFortifiedId: 0,
    nextUnoccupiedId: 0,
    nextExpansionId: 0,
    nextContestedId: 0,

    // Power state instances
    strongholdInstances: null,
    fortifiedInstances: null,
    exploitedInstances: null,
    unoccupiedInstances: null,
    strongholdCarrierInstances: null,
    expansionInstances: null,
    contestedInstances: null,

    // System visualization
    circleInstances: null,
    textObjects: [],

    // Power state geometry
    powerStateGeometriesLoaded: false,
    geometries: null,

    // Spatial indexing
    spatialIndex: new Map(), // Map of "x,y,z" grid cell keys to arrays of systems

    // Marker state
    markerState: {
        mesh: null
    },
    firstClickMarker: null,
    pulsingMarker: null,
    selectedSystem: null
};

// Constants for data loading
export const POWER_DATA = '/json/powerplay.json.gz';  // Change this to .json or .gz as needed
export const DEBUG_LOADING = false;  // Set to false to disable dummy loading

// Constants for star visibility
export const MAX_ZOOM_LEVEL = 200;    // Start reducing visibility at this zoom level
export const MIN_ZOOM_LEVEL = 20;     // Minimum zoom level for visibility
export const MAX_RADIUS = 1000;       // Show all stars beyond this radius
export const MIN_RADIUS = 40;         // Minimum visibility radius when zoomed in
export const FADE_START_ZOOM = 150;   // Start reducing visible radius at this zoom level
export const FADE_MARGIN = 60;        // Additional distance over which stars fade out
export const STAR_VISIBLE_RADIUS = 120;  // Base visibility radius for stars

// Constants for text visibility
export const TEXT_CUTOFF_FRONT = 40;  // Don't draw text closer than this
export const TEXT_CUTOFF_FAR = 1000;  // Additional fade distance beyond systemNameRadius
export const MAX_TEXT_PER_FRAME = 5;  // Batch text creation limit

// Spatial grid size
export const SPATIAL_GRID_SIZE = 25;  // Reduced from 100 to ensure we don't miss systems
export const CHUNK_SIZE = 100;
