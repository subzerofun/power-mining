import * as THREE from '../threejs/three.module.js';
import { isInYBubble } from './systems.js';

let scene;
let camera;
let targetPoint;
let gridGroup;
let systemNameRadius;
let MAX_Y_BUBBLE;
let systems = {};  // Initialize systems as empty object

// Object pools
const linePool = [];
const markerPool = [];

// Active objects
const activeLines = new Map();
const activeMarkers = new Map();

// Shared geometries and materials
const LINE_GEOMETRY = new THREE.CylinderGeometry(0.02, 0.02, 1, 3);
const LINE_MATERIAL = new THREE.MeshBasicMaterial({
    color: 0x00ffff,
    transparent: true,
    depthWrite: false
});

const MARKER_GEOMETRY = new THREE.CircleGeometry(0.8, 16);
const MARKER_MATERIAL = new THREE.MeshBasicMaterial({
    color: 0x00ffff,
    transparent: true,
    side: THREE.DoubleSide,
    depthWrite: false
});

export function initLineMarkers(deps) {
    scene = deps.scene;
    camera = deps.camera;
    targetPoint = deps.targetPoint;
    gridGroup = deps.gridGroup;
    systemNameRadius = deps.systemNameRadius;
    MAX_Y_BUBBLE = deps.MAX_Y_BUBBLE;
    systems = deps.systems || {};  // Store systems reference, default to empty object
}

function getLineFromPool() {
    let line = linePool.pop();
    if (!line) {
        line = new THREE.Mesh(LINE_GEOMETRY, LINE_MATERIAL.clone());
        scene.add(line);
    }
    return line;
}

function getMarkerFromPool() {
    let marker = markerPool.pop();
    if (!marker) {
        marker = new THREE.Mesh(MARKER_GEOMETRY, MARKER_MATERIAL.clone());
        marker.renderOrder = 2;
        scene.add(marker);
    }
    return marker;
}

function returnToPool(systemId) {
    const line = activeLines.get(systemId);
    if (line) {
        line.visible = false;
        linePool.push(line);
        activeLines.delete(systemId);
    }

    const marker = activeMarkers.get(systemId);
    if (marker) {
        marker.visible = false;
        markerPool.push(marker);
        activeMarkers.delete(systemId);
    }
}

function updateLine(line, systemPos, gridY, opacity) {
    line.visible = true;
    line.material.opacity = opacity;

    const endPos = new THREE.Vector3(systemPos.x, gridY, systemPos.z);
    const midPos = new THREE.Vector3().addVectors(systemPos, endPos).multiplyScalar(0.5);
    const length = systemPos.distanceTo(endPos);

    line.scale.set(1, length, 1);
    line.position.copy(midPos);
    line.lookAt(systemPos);
    line.rotateX(Math.PI / 2);
}

function updateMarker(marker, systemPos, gridY, opacity, distanceToTarget) {
    marker.visible = true;
    marker.material.opacity = opacity;

    const circleYPos = (systemPos.y > gridY) ? gridY + 0.12 : gridY;
    marker.position.set(systemPos.x, circleYPos, systemPos.z);
    marker.rotation.x = Math.PI / 2;

    const scale = distanceToTarget * 0.02;
    marker.scale.set(scale, scale, scale);
}

export function updateLineMarkers(currentSystems) {
    if (!currentSystems || !scene || !camera || !targetPoint || !gridGroup) return;  // Early return if dependencies not ready
    systems = currentSystems;  // Update systems reference
    
    const distanceToTarget = camera.position.distanceTo(targetPoint);
    
    // Early culling
    if (distanceToTarget > 50) {
        // Return all active objects to pool
        for (const systemId of activeLines.keys()) {
            returnToPool(systemId);
        }
        return;
    }

    // Track which systems still need markers
    const neededSystems = new Set();
    const maxYDistance = systemNameRadius * MAX_Y_BUBBLE;

    // Update or create markers for systems in range
    for (const system of Object.values(systems)) {
        if (!system) continue;  // Skip if system is undefined
        const systemPos = new THREE.Vector3(system.x, system.y, system.z);
        const distCenter = targetPoint.distanceTo(systemPos);

        if (distCenter <= systemNameRadius && isInYBubble(system.y, targetPoint.y, maxYDistance)) {
            neededSystems.add(system.id64 || system.name);
            const opacity = THREE.MathUtils.mapLinear(distCenter, 0, systemNameRadius, 1, 0.2);

            // Update or create line
            let line = activeLines.get(system.id64 || system.name);
            if (!line) {
                line = getLineFromPool();
                activeLines.set(system.id64 || system.name, line);
            }
            updateLine(line, systemPos, gridGroup.position.y, opacity);

            // Update or create marker
            let marker = activeMarkers.get(system.id64 || system.name);
            if (!marker) {
                marker = getMarkerFromPool();
                activeMarkers.set(system.id64 || system.name, marker);
            }
            updateMarker(marker, systemPos, gridGroup.position.y, opacity, distanceToTarget);
        }
    }

    // Return unused objects to pool
    for (const systemId of activeLines.keys()) {
        if (!neededSystems.has(systemId)) {
            returnToPool(systemId);
        }
    }
}

export function updateSystemNameRadius(radius) {
    systemNameRadius = radius;
}

export function cleanup() {
    // Return all objects to pool
    for (const systemId of activeLines.keys()) {
        returnToPool(systemId);
    }

    // Clear pools
    while (linePool.length > 0) {
        const line = linePool.pop();
        scene.remove(line);
        line.material.dispose();
    }

    while (markerPool.length > 0) {
        const marker = markerPool.pop();
        scene.remove(marker);
        marker.material.dispose();
    }
}

// When creating line markers, convert Elite coordinates to Three.js coordinates
function createLineMarker(system) {
    // Convert Elite coordinates to Three.js coordinates
    const position = new THREE.Vector3(
        system.x,      // Keep X (East/West)
        system.y,      // Keep Y (Up/Down)
        -system.z      // Flip Z to match Three.js South/North
    );

    // Create line marker at converted position
    const lineMarker = new THREE.Group();
    // ... rest of the function ...
} 