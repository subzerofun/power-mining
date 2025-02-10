import * as THREE from '../threejs/three.module.js';
import { POWER_COLORS } from '../search_format.js';

// Constants for circle appearance
export const CIRCLE_CONFIG = {
    OPACITY: 0.8,
    BASE_SIZE: 0.7,
    STRONGHOLD_SCALE: 1.7,
    FORTIFIED_SCALE: 1.4,
    EXPLOITED_SCALE: 1.0,
    CONTESTED_SCALE: 1.2,
    UNOCCUPIED_SCALE: 0.8,
    SEGMENTS: 32
};

// Shared geometry for all circles
const circleGeometry = new THREE.CircleGeometry(CIRCLE_CONFIG.BASE_SIZE, CIRCLE_CONFIG.SEGMENTS);
const materialCache = new Map();

export function createCircleIcon(powerState, controllingPower) {
    // Get color based on power state and controlling power
    let color = '#DDD';  // Default color for uncontrolled states
    
    if (controllingPower && ['Stronghold', 'Fortified', 'Exploited'].includes(powerState)) {
        color = POWER_COLORS[controllingPower] || '#DDD';
    }
    
    // Use cached material if it exists
    const cacheKey = color;
    if (!materialCache.has(cacheKey)) {
        materialCache.set(cacheKey, new THREE.MeshBasicMaterial({
            color: new THREE.Color(color),
            transparent: true,
            opacity: CIRCLE_CONFIG.OPACITY,
            side: THREE.DoubleSide,
            depthWrite: false
        }));
    }

    // Create mesh and set scale based on power state
    const mesh = new THREE.Mesh(circleGeometry, materialCache.get(cacheKey));
    const scale = getScaleForState(powerState);
    mesh.scale.set(scale, scale, 1);
    mesh.renderOrder = 1;
    
    return mesh;
}

function getScaleForState(powerState) {
    switch(powerState) {
        case 'Stronghold':
        case 'Stronghold-Carrier':
            return CIRCLE_CONFIG.STRONGHOLD_SCALE;
        case 'Fortified':
            return CIRCLE_CONFIG.FORTIFIED_SCALE;
        case 'Exploited':
            return CIRCLE_CONFIG.EXPLOITED_SCALE;
        case 'Contested':
            return CIRCLE_CONFIG.CONTESTED_SCALE;
        default:
            return CIRCLE_CONFIG.UNOCCUPIED_SCALE;
    }
}

export function cleanup() {
    materialCache.forEach(material => material.dispose());
    materialCache.clear();
} 