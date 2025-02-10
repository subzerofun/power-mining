import * as THREE from '../threejs/three.module.js';

// Grid configuration
export const GRID_CELL_SIZE = 50;  // Size of each grid cell in ly

// Spatial grid storage
let spatialGrid = new Map(); // Map of "x,y,z" cell keys to arrays of systems

// Get the grid cell key for a position
export function getGridKey(x, y, z) {
    const gridX = Math.floor(x / GRID_CELL_SIZE);
    const gridY = Math.floor(y / GRID_CELL_SIZE);
    const gridZ = Math.floor(z / GRID_CELL_SIZE);
    return `${gridX},${gridY},${gridZ}`;
}

// Get all grid cells that could contain systems within radius of a point
export function getNeighborKeys(x, y, z, radius) {
    const minX = Math.floor((x - radius) / GRID_CELL_SIZE);
    const maxX = Math.floor((x + radius) / GRID_CELL_SIZE);
    const minY = Math.floor((y - radius) / GRID_CELL_SIZE);
    const maxY = Math.floor((y + radius) / GRID_CELL_SIZE);
    const minZ = Math.floor((z - radius) / GRID_CELL_SIZE);
    const maxZ = Math.floor((z + radius) / GRID_CELL_SIZE);
    
    const keys = [];
    for (let gx = minX; gx <= maxX; gx++) {
        for (let gy = minY; gy <= maxY; gy++) {
            for (let gz = minZ; gz <= maxZ; gz++) {
                keys.push(`${gx},${gy},${gz}`);
            }
        }
    }
    return keys;
}

// Build or rebuild the spatial grid from a set of systems
export function buildSpatialGrid(systems) {
    spatialGrid.clear();
    Object.values(systems).forEach(system => {
        const key = getGridKey(system.x, system.y, system.z);
        if (!spatialGrid.has(key)) {
            spatialGrid.set(key, []);
        }
        spatialGrid.get(key).push(system);
    });
}

// Get systems near a point within a radius
export function getSystemsNearPoint(point, radius) {
    const neighborKeys = getNeighborKeys(point.x, point.y, point.z, radius);
    const nearbySystems = [];
    
    neighborKeys.forEach(key => {
        const cellSystems = spatialGrid.get(key);
        if (cellSystems) {
            nearbySystems.push(...cellSystems);
        }
    });
    
    return nearbySystems;
}

// Convert coordinates to match Elite's system
const x = parseFloat(parts[2]);  // Elite's Z -> Three.js X
const y = parseFloat(parts[1]);  // Elite's Y -> Three.js Y
const z = parseFloat(parts[0]);  // Elite's X -> Three.js Z

// Move camera to coordinates
moveToCoordinates(x, y, z); 