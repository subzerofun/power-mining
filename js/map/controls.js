import * as THREE from '../threejs/three.module.js';
import { createCustomGrid } from './grid.js';
import { PowerVisualizer } from './visualize.js';
import { setDragging } from './clickHandler.js';
import { toggleGasClouds, PowerVisualizerExtra } from './visualizeExtra.js';
import { toggleBackground } from './visualizeMore.js';

let camera;
let controls;
let targetPoint;
let gridGroup;
let keysPressed = new Set();
let isMoving = false;
let moveAnimation;
let isOverSearchContainer = false;
let lastMouseY = 0;
let isRightDragging = false;
let initialCameraOffset = null;
let renderer;
let systems;
let scene;
let powerColors;

// Star visibility settings
let STAR_CUTOFF_FRONT = 40;
let STAR_VISIBLE_RADIUS = 100;
let STAR_CUTOFF_FAR = 1000;
let visibleStarsRadius = 1000;

// Add gas clouds toggle handler
const gasCloudButton = document.getElementById('toggleGasClouds');
let gasCloudState = true;  // On by default

export function initControls(deps) {
    camera = deps.camera;
    controls = deps.controls;
    targetPoint = deps.targetPoint;
    gridGroup = deps.gridGroup;
    renderer = deps.renderer;
    systems = deps.systems;
    scene = deps.scene;
    powerColors = deps.powerColors;

    setupKeyboardControls();
    setupMouseControls();
    setupUIControls();
}

function setupKeyboardControls() {
    window.addEventListener('keydown', (event) => {
        // Check if we're focused in the search input
        const isSearchFocused = document.activeElement === document.querySelector('#system');
        
        // If we're focused in search, let the autocomplete handle arrow keys
        if (isSearchFocused) {
            return;
        }
        
        // Only handle arrow keys for map movement when not over search container
        if (!isOverSearchContainer && ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'w', 'a', 's', 'd'].includes(event.key) && !event.repeat) {
            keysPressed.add(event.key);
            startMovement();
        }
    });

    window.addEventListener('keyup', (event) => {
        keysPressed.delete(event.key);
        if (keysPressed.size === 0) {
            isMoving = false;
            cancelAnimationFrame(moveAnimation);
        }
    });
}

function setupMouseControls() {
    // Add smooth zoom handling
    let zoomVelocity = 0;
    const ZOOM_ACCELERATION = 0.15;
    const ZOOM_DECELERATION = 0.85;
    const MAX_ZOOM_SPEED = 2.0;
    const BASE_ZOOM_SPEED = 0.0025;  // Base sensitivity for zoom
    let isZooming = false;
    let zoomAnimation;

    function smoothZoom() {
        if (Math.abs(zoomVelocity) > 0.001) {
            const zoomDelta = zoomVelocity;
            const currentDistance = camera.position.distanceTo(controls.target);
            const newDistance = currentDistance * (1 - zoomDelta);

            // Limit minimum and maximum zoom
            if (newDistance >= controls.minDistance && newDistance <= controls.maxDistance) {
                camera.position.sub(controls.target);
                camera.position.multiplyScalar(1 - zoomDelta);
                camera.position.add(controls.target);
            }

            // Apply deceleration
            zoomVelocity *= ZOOM_DECELERATION;
            zoomAnimation = requestAnimationFrame(smoothZoom);
        } else {
            isZooming = false;
            cancelAnimationFrame(zoomAnimation);
        }
    }

    // Override OrbitControls' zoom handling
    controls.enableZoom = true;
    controls.zoomSpeed = 0.5;  // Reduced from default 1.0
    
    // Add custom wheel event listener
    renderer.domElement.addEventListener('wheel', (event) => {
        if (isOverSearchContainer) return;
        
        event.preventDefault();
        
        // Calculate zoom direction and speed using BASE_ZOOM_SPEED
        const zoomDelta = event.deltaY * -BASE_ZOOM_SPEED;
        
        // Scale zoom speed based on current distance
        const distanceScale = Math.log10(camera.position.distanceTo(controls.target)) * 0.5;
        const scaledZoomDelta = zoomDelta * Math.max(0.5, distanceScale);
        
        // Add to current velocity with acceleration
        zoomVelocity += scaledZoomDelta * ZOOM_ACCELERATION;
        
        // Clamp velocity to max speed
        zoomVelocity = Math.max(-MAX_ZOOM_SPEED, Math.min(MAX_ZOOM_SPEED, zoomVelocity));
        
        if (!isZooming) {
            isZooming = true;
            smoothZoom();
        }
    }, { passive: false });

    renderer.domElement.addEventListener('mousedown', (event) => {
        if (isOverSearchContainer) return;
        
        if (event.button === 2) {
            event.preventDefault();
            isRightDragging = true;
            setDragging(true);
            lastMouseY = event.clientY;
            initialCameraOffset = camera.position.clone().sub(targetPoint);
            controls.enabled = false;
        }
    });

    renderer.domElement.addEventListener('mousemove', (event) => {
        if (isOverSearchContainer) return;
        
        if (isRightDragging) {
            const deltaY = event.clientY - lastMouseY;
            const moveAmount = deltaY * 0.5;
            
            gridGroup.position.y += moveAmount;
            targetPoint.y = gridGroup.position.y;
            
            camera.position.copy(targetPoint).add(initialCameraOffset);
            controls.target.copy(targetPoint);
            
            camera.updateMatrix();
            camera.updateMatrixWorld();
            
            lastMouseY = event.clientY;
        }
    });

    renderer.domElement.addEventListener('mouseup', (event) => {
        if (event.button === 2) {
            isRightDragging = false;
            setDragging(false);
            initialCameraOffset = null;
            controls.enabled = true;
        }
    });

    renderer.domElement.addEventListener('mouseleave', () => {
        isRightDragging = false;
        setDragging(false);
        initialCameraOffset = null;
        controls.enabled = true;
    });

    renderer.domElement.addEventListener('contextmenu', (event) => {
        if (!isOverSearchContainer) {
            event.preventDefault();
        }
    });
}

function startMovement() {
    if (isMoving) return;
    isMoving = true;
    
    // Create velocity object to track current speed
    const velocity = {
        x: 0,
        y: 0,
        z: 0
    };
    
    // Constants for smooth movement
    const ACCELERATION = 0.25;  // How quickly we reach max speed
    const MAX_SPEED = 3.0;      // Maximum movement speed multiplier
    const DECELERATION = 0.15;   // Reduced from 1.0 for smoother movement
    
    function animate() {
        // Calculate move speed based on camera distance
        const distanceToTarget = camera.position.distanceTo(targetPoint);
        const baseSpeed = Math.max(4, distanceToTarget * 0.01);
        
        // Get camera's view direction and right vector
        const cameraDirection = new THREE.Vector3();
        camera.getWorldDirection(cameraDirection);
        cameraDirection.y = 0;  // Keep movement in XZ plane
        cameraDirection.normalize();
        
        // Calculate camera's right vector
        const cameraRight = new THREE.Vector3();
        cameraRight.crossVectors(new THREE.Vector3(0, 1, 0), cameraDirection).normalize();
        
        // Store camera's current relative position to target
        const cameraOffset = camera.position.clone().sub(targetPoint);
        
        // Calculate desired movement direction
        const desiredMovement = new THREE.Vector3();
        
        if (keysPressed.has('ArrowLeft') || keysPressed.has('a')) {
            desiredMovement.add(cameraRight);
        }
        if (keysPressed.has('ArrowRight') || keysPressed.has('d')) {
            desiredMovement.sub(cameraRight);
        }
        if (keysPressed.has('ArrowUp') || keysPressed.has('w')) {
            desiredMovement.add(cameraDirection);
        }
        if (keysPressed.has('ArrowDown') || keysPressed.has('s')) {
            desiredMovement.sub(cameraDirection);
        }

        // Normalize desired movement if it exists
        if (desiredMovement.length() > 0) {
            desiredMovement.normalize();
            
            // Accelerate smoothly
            velocity.x += (desiredMovement.x * ACCELERATION - velocity.x * DECELERATION);
            velocity.y += (desiredMovement.y * ACCELERATION - velocity.y * DECELERATION);
            velocity.z += (desiredMovement.z * ACCELERATION - velocity.z * DECELERATION);
            
            // Clamp velocity to max speed
            const speed = new THREE.Vector3(velocity.x, velocity.y, velocity.z).length();
            if (speed > MAX_SPEED) {
                const scale = MAX_SPEED / speed;
                velocity.x *= scale;
                velocity.y *= scale;
                velocity.z *= scale;
            }
        } else {
            // Decelerate smoothly when no keys are pressed
            velocity.x *= (1 - DECELERATION);
            velocity.y *= (1 - DECELERATION);
            velocity.z *= (1 - DECELERATION);
        }

        // Apply movement with current velocity
        if (Math.abs(velocity.x) > 0.001 || Math.abs(velocity.y) > 0.001 || Math.abs(velocity.z) > 0.001) {
            const movement = new THREE.Vector3(velocity.x, velocity.y, velocity.z)
                .multiplyScalar(baseSpeed);
            
            // Update target and camera positions
            targetPoint.add(movement);
            camera.position.copy(targetPoint).add(cameraOffset);
            controls.target.copy(targetPoint);
            
            moveAnimation = requestAnimationFrame(animate);
        } else {
            // Stop animation if velocity is negligible
            isMoving = false;
            cancelAnimationFrame(moveAnimation);
        }
    }
    
    moveAnimation = requestAnimationFrame(animate);
}

function setupUIControls() {
    // Star visibility controls
    document.getElementById('cutoffFront').addEventListener('input', (e) => {
        STAR_CUTOFF_FRONT = parseFloat(e.target.value);
    });

    document.getElementById('visibleRadius').addEventListener('input', (e) => {
        STAR_VISIBLE_RADIUS = Math.round(parseFloat(e.target.value));
    });

    document.getElementById('farCutoff').addEventListener('input', (e) => {
        STAR_CUTOFF_FAR = parseFloat(e.target.value);
    });

    document.getElementById('togglePowerStates').addEventListener('click', () => {
        PowerVisualizer.showPowerStates = !PowerVisualizer.showPowerStates;
        const button = document.getElementById('togglePowerStates');
        button.textContent = PowerVisualizer.showPowerStates ? 'Power Icons' : 'Icons off';
    });

    // Power visualization controls
    document.getElementById('togglePowerLines').addEventListener('click', () => {
        PowerVisualizer.showPowerLines = !PowerVisualizer.showPowerLines;
        const button = document.getElementById('togglePowerLines');
        button.textContent = PowerVisualizer.showPowerLines ? 'Lines 1' : 'Lines 1 off';
        PowerVisualizer.createPowerLines(systems);
    });

    document.getElementById('togglePowerVolumes').addEventListener('click', () => {
        const button = document.getElementById('togglePowerVolumes');
        
        // Clean up first
        PowerVisualizer.cleanupPowerGradients();
        
        // Toggle state
        PowerVisualizer.showPowerVolumes = !PowerVisualizer.showPowerVolumes;
        button.textContent = PowerVisualizer.showPowerVolumes ? 'Nebula Off' : 'Nebula On';
        
        // Only create new if toggled on
        if (PowerVisualizer.showPowerVolumes) {
            PowerVisualizer.createPowerGradients(systems, camera);
        }
    });

    document.getElementById('togglePowerRegions').addEventListener('click', () => {
        PowerVisualizer.showPowerRegions = !PowerVisualizer.showPowerRegions;
        const button = document.getElementById('togglePowerRegions');
        button.textContent = PowerVisualizer.showPowerRegions ? 'Power Regions Off' : 'Power Regions On';
        PowerVisualizer.createPowerRegions(systems);
    });

    // Power Mesh toggle button
    const powerMeshButton = document.getElementById('togglePowerMesh');
    powerMeshButton.addEventListener('click', () => {
        PowerVisualizer.showPowerMesh = !PowerVisualizer.showPowerMesh;
        powerMeshButton.textContent = PowerVisualizer.showPowerMesh ? 'Power Mesh Off' : 'Power Mesh On';
        
        if (PowerVisualizer.showPowerMesh) {
            PowerVisualizer.createPowerTerritoryMesh(systems, camera);
        } else {
            PowerVisualizer.cleanupPowerMesh();
        }
    });

    document.getElementById('toggleOrganicPowerLines').addEventListener('click', () => {
        PowerVisualizer.showOrganicPowerLines = !PowerVisualizer.showOrganicPowerLines;
        const button = document.getElementById('toggleOrganicPowerLines');
        button.textContent = PowerVisualizer.showOrganicPowerLines ? 'Lines 2 Off' : 'Lines 2 On';
        PowerVisualizer.createOrganicPowerLines(systems);
    });

    document.getElementById('togglePowerTerritories').addEventListener('click', () => {
        PowerVisualizer.showPowerTerritories = !PowerVisualizer.showPowerTerritories;
        const button = document.getElementById('togglePowerTerritories');
        button.textContent = PowerVisualizer.showPowerTerritories ? 'Volumetric Off' : 'Volumetric On';
        
        // Clear existing volumetric clouds first
        PowerVisualizer.clearObjects(PowerVisualizer.powerVolumeObjects);
        
        // Only create new volumetric clouds if toggled on
        if (PowerVisualizer.showPowerTerritories) {
            PowerVisualizer.createVolumetricPowerClouds(systems, camera);
        }
    });

    document.getElementById('toggleGeodesicTerritories').addEventListener('click', () => {
        PowerVisualizer.showGeodesicTerritories = !PowerVisualizer.showGeodesicTerritories;
        const button = document.getElementById('toggleGeodesicTerritories');
        button.textContent = PowerVisualizer.showGeodesicTerritories ? 'Geodesic Off' : 'Geodesic On';
        
        // Clean up first using the dedicated cleanup function
        PowerVisualizerExtra.cleanupGeodesicTerritories();
        
        // Only create new if toggled on
        if (PowerVisualizer.showGeodesicTerritories) {
            PowerVisualizer.createGeodesicTerritories(systems, camera);
        }
    });

    // Gas clouds toggle handler
    gasCloudButton.addEventListener('click', () => {
        gasCloudState = !gasCloudState;
        gasCloudButton.textContent = gasCloudState ? 'Nebulae On' : 'Nebulae Off';
        toggleGasClouds(scene, gasCloudState);
    });

    document.getElementById('togglePowerGlow').addEventListener('click', () => {
        PowerVisualizerExtra.showPowerGlow = !PowerVisualizerExtra.showPowerGlow;
        const button = document.getElementById('togglePowerGlow');
        button.textContent = PowerVisualizerExtra.showPowerGlow ? 'Power Glow On' : 'Power Glow Off';
        PowerVisualizerExtra.powerGlow(systems, camera);
    });

    document.getElementById('toggleBackground').addEventListener('click', () => {
        const showBackground = toggleBackground();
        const button = document.getElementById('toggleBackground');
        button.textContent = showBackground ? '360° Background' : 'Background Off';
    });
}

export function setOverContainer(value) {
    isOverSearchContainer = value;
}

export function cleanup() {
    keysPressed.clear();
    isMoving = false;
    if (moveAnimation) {
        cancelAnimationFrame(moveAnimation);
    }
}

export function getCameraInfo() {
    if (!camera || !controls || !targetPoint) return null;
    
    const zoomLevel = camera.position.distanceTo(targetPoint);
    const cameraDirection = new THREE.Vector3();
    camera.getWorldDirection(cameraDirection);
    
    // Calculate angles in degrees
    const angleX = THREE.MathUtils.radToDeg(Math.atan2(cameraDirection.y, cameraDirection.z));
    const angleY = THREE.MathUtils.radToDeg(Math.atan2(cameraDirection.x, cameraDirection.z));
    
    return {
        x: camera.position.x.toFixed(2),
        y: camera.position.y.toFixed(2),
        z: camera.position.z.toFixed(2),
        angleX: angleX.toFixed(1),
        angleY: angleY.toFixed(1),
        zoom: zoomLevel.toFixed(1)
    };
} 