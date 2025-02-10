import * as THREE from '../threejs/three.module.js';
import { handleSystemClick, handleEmptySpaceClick, handleFirstClick, stopPulseAnimation } from './systemView.js';
import { SVGLoader } from '../threejs/jsm/loaders/SVGLoader.js';
import { moveToSystem } from './camera.js';
import { globalState } from './globalState.js';
import { showSystemInfo } from './systemInfo.js';

let camera;
let renderer;
let systems;
let circleInstances;
let textObjects;
let findSystemCallback;
let scene;
let targetPoint;
let isDragging = false;
let isOverContainer = false;
let preSelectedSystem = null;  // Track system waiting for second click
let markerGeometry = null;  // Store the SVG marker geometry
let pulsingMarker = null;  // Store the pulsing circle marker
let pulseAnimation = null;  // Store the pulse animation
let lastSystemInfo = null;  // Store last system info
let currentMarkedSystem = null;

const raycaster = new THREE.Raycaster();
const mouse = new THREE.Vector2();
let hoveredInstance = null;

let collisionInstancedMesh = null;
const collisionGeometry = new THREE.SphereGeometry(0.6, 24, 24);

// Material for visible spheres
const sphereMaterial = new THREE.ShaderMaterial({
    uniforms: {
        baseOpacity: { value: 0.5 },
        color: { value: new THREE.Color(0x00FFFF) },
        time: { value: 0.0 }
    },
    vertexShader: `
        varying vec3 vNormal;
        varying vec3 vViewPosition;
        
        void main() {
            vNormal = normalize(normalMatrix * normal);
            vec4 mvPosition = modelViewMatrix * instanceMatrix * vec4(position, 1.0);
            vViewPosition = -mvPosition.xyz;
            gl_Position = projectionMatrix * mvPosition;
        }
    `,
    fragmentShader: `
        uniform vec3 color;
        uniform float baseOpacity;
        varying vec3 vNormal;
        varying vec3 vViewPosition;
        
        void main() {
            vec3 normal = normalize(vNormal);
            vec3 viewDir = normalize(vViewPosition);
            float fresnel = pow(1.0 - abs(dot(normal, viewDir)), 2.0);
            
            float gradient = fresnel;
            float opacity = mix(0.0, 1.0, gradient);
            
            if (gradient > 0.8) opacity = 1.0;
            else if (gradient > 0.6) opacity = mix(0.7, 1.0, (gradient - 0.6) / 0.2);
            else if (gradient > 0.4) opacity = mix(0.3, 0.7, (gradient - 0.4) / 0.2);
            else opacity = mix(0.0, 0.3, gradient / 0.4);
            
            gl_FragColor = vec4(color, baseOpacity * opacity);
        }
    `,
    transparent: true,
    side: THREE.FrontSide,
    depthTest: false,
    depthWrite: false,
    blending: THREE.AdditiveBlending
});

// Material for invisible spheres (only for collision)
const invisibleMaterial = new THREE.MeshBasicMaterial({
    visible: false,
    transparent: true,
    opacity: 0,
    side: THREE.FrontSide,
    depthTest: false,
    depthWrite: false
});

// Material for hover effect
const hoverMaterial = new THREE.ShaderMaterial({
    uniforms: {
        baseOpacity: { value: 0.5 },
        color: { value: new THREE.Color(0x00FFFF) },
        time: { value: 0.0 }
    },
    vertexShader: `
        uniform float time;
        varying vec3 vNormal;
        varying vec3 vViewPosition;
        
        void main() {
            vNormal = normalize(normalMatrix * normal);
            
            vec3 pos = position;
            float scale = 1.0 + 0.5 * (1.0 + sin(time * 3.0) * 0.2);
            pos *= scale;
            
            vec4 mvPosition = modelViewMatrix * vec4(pos, 1.0);
            vViewPosition = -mvPosition.xyz;
            gl_Position = projectionMatrix * mvPosition;
        }
    `,
    fragmentShader: `
        uniform vec3 color;
        uniform float baseOpacity;
        varying vec3 vNormal;
        varying vec3 vViewPosition;
        
        void main() {
            vec3 normal = normalize(vNormal);
            vec3 viewDir = normalize(vViewPosition);
            float fresnel = pow(1.0 - abs(dot(normal, viewDir)), 2.0);
            
            float gradient = fresnel;
            float opacity = mix(0.0, 1.0, gradient);
            
            if (gradient > 0.8) opacity = 1.0;
            else if (gradient > 0.6) opacity = mix(0.7, 1.0, (gradient - 0.6) / 0.2);
            else if (gradient > 0.4) opacity = mix(0.3, 0.7, (gradient - 0.4) / 0.2);
            else opacity = mix(0.0, 0.3, gradient / 0.4);
            
            gl_FragColor = vec4(color * 1.5, baseOpacity * opacity * 1.2);
        }
    `,
    transparent: true,
    side: THREE.FrontSide,
    depthTest: false,
    depthWrite: false,
    blending: THREE.AdditiveBlending
});

// Single hover sphere for effect
let hoverSphere = null;

// Constants for star visibility
export const MAX_ZOOM_LEVEL = 300;    // Show all stars at this zoom level and above
export const MIN_ZOOM_LEVEL = 10;     // Reduced from 30 to allow visibility at lower zoom
export const MAX_RADIUS = 250;        // Maximum radius in ly for star visibility
export const MIN_RADIUS = 120;        // Increased from 30 to match STAR_VISIBLE_RADIUS
export const FADE_START_ZOOM = 200;   // Start reducing visible radius at this zoom level
export const FADE_MARGIN = 50;        // Additional distance over which stars fade out

// Distance constants for text visibility
export const TEXT_CUTOFF_FRONT = 0;  // Reduced from 40 to allow closer viewing
export const TEXT_CUTOFF_FAR = 1000;    // Additional fade distance beyond systemNameRadius

export let STAR_VISIBLE_RADIUS = 50;  // This matches our new MIN_RADIUS

// Debug flag to show collision spheres
const SHOW_COLLISION_SPHERES = false;
// Debug flag to show text planes
const SHOW_TEXT_PLANES = false;
// Maximum radius in ly from target point for debug planes
const MAX_PLANE_RADIUS = 60;

// Create material for text debug planes
const textPlaneMaterial = new THREE.MeshBasicMaterial({
    color: 0x0066ff,
    transparent: false,
    opacity: 1,
    side: THREE.DoubleSide,
    depthTest: true,
    depthWrite: true,
    wireframe: true
});

// Add these variables at the top with other state variables
let lastUpdatePosition = new THREE.Vector3();
let lastUpdateZoom = 0;
const UPDATE_THRESHOLD = 1; // Distance in ly to trigger update
const ZOOM_UPDATE_THRESHOLD = 0.1; // 10% zoom change to trigger update

// Add these at the top with other state variables
let lastHighlightedText = null;  // Track last highlighted text
let systemsByCollisionIndex = new Map();  // Cache for quick system lookup
const tempVector = new THREE.Vector3();  // Reusable vector

// Add hover animation state
let hoveredInstanceId = -1;
let lastTime = performance.now() / 1000;

// Create pulsing circle marker
function createPulsingMarker() {
    // Create ring with shader material for smooth gradient
    const geometry = new THREE.RingGeometry(0.8, 0.8, 32);  // More segments for smoother circle
    const material = new THREE.ShaderMaterial({
        uniforms: {
            time: { value: 0.0 },
            color: { value: new THREE.Color(0x00ffff) },
            progress: { value: 0.0 }  // 0 to 1 for expansion and fade
        },
        vertexShader: `
            varying vec2 vUv;
            void main() {
                vUv = uv;
                gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
            }
        `,
        fragmentShader: `
            uniform vec3 color;
            uniform float progress;
            varying vec2 vUv;
            
            void main() {
                // Calculate distance from center of ring thickness
                float center = 0.5;
                float dist = abs(vUv.x - center);
                
                // Create gradient that peaks in middle of ring thickness
                float gradient = 1.0 - smoothstep(0.0, 0.5, dist);
                
                // Fade out based on progress
                float fade = 1.0 - progress;
                
                // Combine for final opacity
                float opacity = gradient * fade * 0.5;
                
                gl_FragColor = vec4(color, opacity);
            }
        `,
        transparent: true,
        side: THREE.DoubleSide,
        depthWrite: false
    });
    
    pulsingMarker = new THREE.Mesh(geometry, material);
    pulsingMarker.renderOrder = 3;
    pulsingMarker.visible = false;
    scene.add(pulsingMarker);
}

// Start pulse animation
function startPulseAnimation() {
    if (pulseAnimation) return;
    
    const startScale = 1.0;
    const endScale = 3.0;
    const duration = 2.0;
    
    // Create timeline for continuous ripples
    pulseAnimation = gsap.timeline({
        repeat: -1,
        onUpdate: () => {
            if (!pulsingMarker) return;
            
            // Update shader uniforms
            const material = pulsingMarker.material;
            material.uniforms.time.value = performance.now() / 1000;
            
            // Scale grows linearly while progress controls fade
            const currentProgress = pulseAnimation.progress();
            const normalizedProgress = (currentProgress % (1/3)) * 3; // 0-1 three times per timeline
            
            material.uniforms.progress.value = normalizedProgress;
            
            // Scale up the ring
            const scale = gsap.utils.interpolate(startScale, endScale, normalizedProgress);
            pulsingMarker.scale.set(scale, scale, scale);
        }
    });

    // Create three overlapping animations for continuous effect
    pulseAnimation.to({}, {
        duration: duration,
        repeat: 0
    }, 0);
    
    pulseAnimation.to({}, {
        duration: duration,
        repeat: 0
    }, duration / 3);
    
    pulseAnimation.to({}, {
        duration: duration,
        repeat: 0
    }, (duration * 2) / 3);
}

// Load SVG marker
async function loadMarkerSvg() {
    const loader = new SVGLoader();
    try {
        const data = await loader.loadAsync('/img/map/marker.svg');
        const paths = data.paths;
        
        // Create marker geometry from SVG paths
        const shapes = paths.flatMap(path => SVGLoader.createShapes(path));
        markerGeometry = new THREE.ShapeGeometry(shapes);
        
        // Initial small scale for the base size
        markerGeometry.scale(0.03, -0.03, 0.03);
        markerGeometry.center();
        
        // Create marker material
        const markerMaterial = new THREE.MeshBasicMaterial({
            color: 0xff9009,  // Orange
            side: THREE.DoubleSide,
            depthTest: false,  // Ensure it renders on top
            transparent: true,
            opacity: 1.0
        });
        
        // Create marker mesh and assign to shared state
        globalState.markerState.mesh = new THREE.Mesh(markerGeometry, markerMaterial);
        globalState.markerState.mesh.renderOrder = 100000;  // Extremely high render order
        globalState.markerState.mesh.visible = false;
        scene.add(globalState.markerState.mesh);
        
        console.log('Marker created and added to scene:', globalState.markerState.mesh);
    } catch (error) {
        console.error('Error loading marker SVG:', error);
    }
}

function shouldUpdateCollisionMeshes() {
    if (!camera || !targetPoint) return false;
    
    const currentZoom = camera.position.distanceTo(targetPoint);
    const zoomChange = Math.abs(currentZoom - lastUpdateZoom) / lastUpdateZoom;
    const positionChange = targetPoint.distanceTo(lastUpdatePosition);
    
    return zoomChange > ZOOM_UPDATE_THRESHOLD || positionChange > UPDATE_THRESHOLD;
}

function updateCollisionMeshes() {
    if (!systems || !shouldUpdateCollisionMeshes()) return;

    // If mesh exists but shouldn't be visible, remove it entirely
    /*
    if (collisionInstancedMesh && !SHOW_COLLISION_SPHERES) {
        scene.remove(collisionInstancedMesh);
        collisionInstancedMesh.geometry.dispose();
        collisionInstancedMesh.material.dispose();
        collisionInstancedMesh = null;
        return;
    }

    // Don't proceed if spheres shouldn't be shown
    if (!SHOW_COLLISION_SPHERES) return;
    */

    const startTime = performance.now();
    const matrix = new THREE.Matrix4();
    let visibleCount = 0;

    // Update last known positions
    lastUpdatePosition.copy(targetPoint);
    lastUpdateZoom = camera.position.distanceTo(targetPoint);

    // Clear the lookup cache
    systemsByCollisionIndex.clear();

    // First pass: count visible systems
    Object.values(systems).forEach(system => {
        if (isSystemVisible(system)) {
            visibleCount++;
        }
    });

    // If no visible systems, clean up everything
    if (visibleCount === 0) {
        if (collisionInstancedMesh) {
            scene.remove(collisionInstancedMesh);
            collisionInstancedMesh.geometry.dispose();
            collisionInstancedMesh.material.dispose();
            collisionInstancedMesh = null;
        }
        if (hoverSphere) {
            scene.remove(hoverSphere);
            hoverSphere.geometry.dispose();
            hoverSphere.material.dispose();
            hoverSphere = null;
        }
        return;
    }

    // Create or recreate mesh if needed with new count
    if (!collisionInstancedMesh || collisionInstancedMesh.count !== visibleCount) {
        if (collisionInstancedMesh) {
            scene.remove(collisionInstancedMesh);
            collisionInstancedMesh.geometry.dispose();
            collisionInstancedMesh.material.dispose();
            collisionInstancedMesh = null;
        }
        
        // Always create collision mesh for clicking, but use different materials based on SHOW_COLLISION_SPHERES
        collisionInstancedMesh = new THREE.InstancedMesh(
            collisionGeometry,
            SHOW_COLLISION_SPHERES ? sphereMaterial : invisibleMaterial,
            visibleCount
        );
        collisionInstancedMesh.visible = true;  // Always visible for collision detection
        scene.add(collisionInstancedMesh);
    }

    // Create hover sphere if needed
    if (!hoverSphere) {
        hoverSphere = new THREE.Mesh(collisionGeometry, hoverMaterial);
        hoverSphere.visible = false;
        scene.add(hoverSphere);
    }

    // Second pass: update visible systems
    let instanceIndex = 0;
    Object.values(systems).forEach(system => {
        if (isSystemVisible(system)) {
            system.collisionIndex = instanceIndex;
            matrix.compose(
                tempVector.set(system.x, system.y, system.z),
                new THREE.Quaternion(),
                new THREE.Vector3(1.0, 1.0, 1.0)
            );
            collisionInstancedMesh.setMatrixAt(instanceIndex, matrix);
            systemsByCollisionIndex.set(instanceIndex, system);
            instanceIndex++;
        } else {
            system.collisionIndex = -1;
        }
    });

    collisionInstancedMesh.instanceMatrix.needsUpdate = true;

    const endTime = performance.now();
    const updateTime = endTime - startTime;
    if (updateTime > 16.67) {
        console.warn(`Collision mesh update took ${updateTime.toFixed(2)}ms`);
    }
}

// Export for animation loop
export { updateCollisionMeshes };

export function initClickHandler(deps) {
    camera = deps.camera;
    renderer = deps.renderer;
    systems = deps.systems;
    circleInstances = deps.circleInstances;
    textObjects = deps.textObjects;
    findSystemCallback = deps.findSystem;
    scene = deps.scene;
    targetPoint = deps.targetPoint;

    // Create initial collision mesh
    updateCollisionMeshes();
    
    createPulsingMarker();
    loadMarkerSvg();
    
    setupEventListeners();
}

export function updateDependencies(deps) {
    cleanupOutOfRangePlanes();
    cleanup(); // Clean up old collision meshes
    if (deps.circleInstances) circleInstances = deps.circleInstances;
    if (deps.textObjects) textObjects = deps.textObjects;
    if (deps.systems) systems = deps.systems;
}

export function setDragging(value) {
    isDragging = value;
}

export function setOverContainer(value) {
    isOverContainer = value;
}

function updateCursor(isHovering) {
    renderer.domElement.style.cursor = isHovering ? 'pointer' : 'default';
}

// Helper function to check if a system is visible
function isSystemVisible(system) {
    if (!system || !targetPoint) return false;
    
    const position = new THREE.Vector3(system.x, system.y, system.z);
    const distanceToCenter = targetPoint.distanceTo(position);
    const distanceToCamera = camera.position.distanceTo(position);
    const zoomLevel = camera.position.distanceTo(targetPoint);
    
    // Use the same visibility rules as stars
    return distanceToCamera >= TEXT_CUTOFF_FRONT && 
           distanceToCamera <= STAR_VISIBLE_RADIUS * 2 && 
           distanceToCenter <= STAR_VISIBLE_RADIUS &&
           zoomLevel <= MAX_ZOOM_LEVEL;
}

// Helper to create or update text debug plane
function updateTextDebugPlane(textObj) {
    // Always clean up existing plane first if it exists
    if (textObj.debugPlane) {
        scene.remove(textObj.debugPlane);
        textObj.debugPlane.geometry.dispose();
        textObj.debugPlane.material.dispose();
        textObj.debugPlane = null;
    }

    // Check if system is in range before creating new plane
    const distance = targetPoint.distanceTo(new THREE.Vector3(
        textObj.systemCoords.x,
        textObj.systemCoords.y,
        textObj.systemCoords.z
    ));
    
    if (distance > MAX_PLANE_RADIUS) return;

    // Only create plane if text is actually visible
    if (!textObj.mesh || !textObj.mesh.visible || !isTextVisible(textObj)) return;

    // Compute actual text bounds
    if (!textObj.mesh.geometry.boundingBox) {
        textObj.mesh.geometry.computeBoundingBox();
    }

    const bounds = textObj.mesh.geometry.boundingBox;
    
    // Create plane geometry matching text size exactly, but 10% longer
    const width = (bounds.max.x - bounds.min.x) * 1.1;  // 10% longer
    const height = bounds.max.y - bounds.min.y;
    const planeGeometry = new THREE.PlaneGeometry(width, height);
    textObj.debugPlane = new THREE.Mesh(planeGeometry, textPlaneMaterial.clone());
    scene.add(textObj.debugPlane);

    // Get the text's world position
    textObj.group.updateWorldMatrix(true, false);
    const worldPosition = new THREE.Vector3();
    textObj.mesh.getWorldPosition(worldPosition);

    // Update plane to match text exactly
    textObj.debugPlane.position.copy(worldPosition);
    textObj.debugPlane.quaternion.copy(camera.quaternion);
    textObj.debugPlane.scale.copy(textObj.mesh.scale);
    
    // Move along the plane's local x-axis by 50% of its width
    const right = new THREE.Vector3(1, 0, 0).applyQuaternion(textObj.debugPlane.quaternion);
    const width2 = (bounds.max.x - bounds.min.x) * textObj.mesh.scale.x * 1.1;
    textObj.debugPlane.position.addScaledVector(right, (width2 * 0.5) * 1.1);
    
    // Show plane
    textObj.debugPlane.visible = SHOW_TEXT_PLANES;
    textObj.debugPlane.updateMatrixWorld();
}

// Helper function to check if text is visible
function isTextVisible(textObj) {
    if (!textObj || !textObj.mesh || !targetPoint) return false;
    
    const position = new THREE.Vector3(
        textObj.systemCoords.x,
        textObj.systemCoords.y,
        textObj.systemCoords.z
    );
    const distanceToCamera = camera.position.distanceTo(position);
    const distanceToTarget = targetPoint.distanceTo(position);
    const zoomLevel = camera.position.distanceTo(targetPoint);
    
    // Match the exact visibility rules from systems.js
    return zoomLevel <= 45 && 
           distanceToCamera >= 10 && 
           distanceToCamera <= 60 && 
           distanceToTarget <= 50 &&
           textObj.mesh.visible &&
           textObj.group.visible;
}

function cleanupOutOfRangePlanes() {
    if (!textObjects || !targetPoint) return;
    
    textObjects.forEach(textObj => {
        if (textObj.debugPlane) {
            const distance = targetPoint.distanceTo(new THREE.Vector3(
                textObj.systemCoords.x,
                textObj.systemCoords.y,
                textObj.systemCoords.z
            ));
            
            // Clean up plane if:
            // 1. Text is out of range
            // 2. Text is not visible
            // 3. Text mesh is not visible
            const shouldRemove = distance > MAX_PLANE_RADIUS || 
                               !isTextVisible(textObj) || 
                               !textObj.mesh.visible || 
                               !textObj.group.visible;
            
            if (shouldRemove) {
                scene.remove(textObj.debugPlane);
                textObj.debugPlane.geometry.dispose();
                textObj.debugPlane.material.dispose();
                delete textObj.debugPlane;
            }
        }
    });
}

function handleMouseMove(event) {
    if (isDragging || isOverContainer) {
        updateCursor(false);
        if (hoverSphere) hoverSphere.visible = false;
        return;
    }

    const rect = renderer.domElement.getBoundingClientRect();
    mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouse, camera);

    let foundIntersection = false;

    // Reset only the last highlighted text
    if (lastHighlightedText && lastHighlightedText.mesh && lastHighlightedText.mesh.material) {
        lastHighlightedText.mesh.material.color.setHex(0xffffff);
        lastHighlightedText = null;
    }

    // Reset hover state
    hoveredInstanceId = -1;

    if (collisionInstancedMesh) {
        const intersects = raycaster.intersectObject(collisionInstancedMesh);
        if (intersects.length > 0) {
            const instanceId = intersects[0].instanceId;
            const system = systemsByCollisionIndex.get(instanceId);
            
            if (system) {
                hoveredInstance = system.instanceId;
                hoveredInstanceId = instanceId;
                foundIntersection = true;
                
                // Update hover sphere position and make visible
                hoverSphere.position.set(system.x, system.y, system.z);
                hoverSphere.visible = true;

                // Find corresponding text object efficiently
                const textObj = textObjects.find(t => {
                    return t.systemCoords.x === system.x && 
                           t.systemCoords.y === system.y && 
                           t.systemCoords.z === system.z;
                });

                if (textObj && textObj.mesh && textObj.mesh.visible && isTextVisible(textObj)) {
                    textObj.mesh.material.color.setHex(0x00ffff);
                    lastHighlightedText = textObj;
                    if (SHOW_TEXT_PLANES) {
                        updateTextDebugPlane(textObj);
                    }
                }
            }
        } else {
            hoverSphere.visible = false;
        }
    }

    // Only check text objects if debug planes are enabled
    if (!foundIntersection && textObjects && SHOW_TEXT_PLANES) {
        for (const textObj of textObjects) {
            if (textObj.mesh && textObj.mesh.visible && isTextVisible(textObj)) {
                if (textObj.debugPlane) {
                    const intersects = raycaster.intersectObject(textObj.debugPlane);
                    if (intersects.length > 0) {
                        foundIntersection = true;
                        textObj.mesh.material.color.setHex(0x00ffff);
                        lastHighlightedText = textObj;
                        break;  // Exit loop once we find an intersection
                    }
                }
            }
        }
    }

    updateCursor(foundIntersection);
}

function setupEventListeners() {
    renderer.domElement.addEventListener('mousemove', handleMouseMove);
    renderer.domElement.addEventListener('click', handleClick);

    // Add close button listener
    const closeButton = document.getElementById('close-system-info');
    if (closeButton) {
        closeButton.addEventListener('click', () => {
            document.getElementById('system-info').style.display = 'none';
            handleEmptySpaceClick();
            preSelectedSystem = null;  // Clear pre-selection on window close
        });
    }
}

function handleSystemPreSelection(system) {
    // Get the full system data from the systems object
    const fullSystemData = systems[system.name.toLowerCase()];
    if (!fullSystemData) {
        console.warn('Could not find full system data for:', system.name);
        return;
    }
    
    document.getElementById('system').value = system.name;
    showSystemInfo(fullSystemData);
    handleFirstClick(system);  // Show cyan circle on first click
    preSelectedSystem = fullSystemData;  // Store the full data
}

function handleSystemFinalSelection(system) {
    handleSystemClick(system);  // Place marker
    updateMarkerPosition(system);  // Keep markers visible
    moveToSystem(system);  // Use moveToSystem instead of findSystemCallback
    preSelectedSystem = null;  // Clear pre-selection
}

function handleClick(event) {
    if (isDragging || isOverContainer) return;

    const rect = renderer.domElement.getBoundingClientRect();
    mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouse, camera);

    // Check collisions with systems
    if (collisionInstancedMesh && collisionInstancedMesh.visible) {
        const intersects = raycaster.intersectObject(collisionInstancedMesh);
        if (intersects.length > 0) {
            const instanceId = intersects[0].instanceId;
            const system = Object.values(systems).find(s => s.collisionIndex === instanceId);
            
            if (system) {
                if (preSelectedSystem && 
                    preSelectedSystem.x === system.x && 
                    preSelectedSystem.y === system.y && 
                    preSelectedSystem.z === system.z) {
                    // Second click on same system
                    handleSystemClick(system);
                    moveToSystem(system);
                    preSelectedSystem = null;
                } else {
                    // First click on a system
                    handleFirstClick(system);
                    showSystemInfo(system);
                    preSelectedSystem = system;
                }
                return;
            }
        }
    }

    // Check text objects if no collision found
    let textClicked = false;
    textObjects.forEach(textObj => {
        if (textObj.mesh && textObj.mesh.visible && isTextVisible(textObj)) {
            if (textObj.debugPlane) {
                const intersects = raycaster.intersectObject(textObj.debugPlane);
                if (intersects.length > 0) {
                    const system = Object.values(systems).find(s => 
                        s.x === textObj.systemCoords.x && 
                        s.y === textObj.systemCoords.y && 
                        s.z === textObj.systemCoords.z
                    );
                    if (system) {
                        if (preSelectedSystem && 
                            preSelectedSystem.x === system.x && 
                            preSelectedSystem.y === system.y && 
                            preSelectedSystem.z === system.z) {
                            handleSystemClick(system);
                            moveToSystem(system);
                            preSelectedSystem = null;
                        } else {
                            handleFirstClick(system);
                            showSystemInfo(system);
                            preSelectedSystem = system;
                        }
                        textClicked = true;
                        return;
                    }
                }
            }
        }
    });

    // If nothing was clicked, handle empty space click
    if (!textClicked) {
        handleEmptySpaceClick();
        preSelectedSystem = null;
    }
}

// Update the marker position
export function updateMarkerPosition(system) {
    if (!system || !globalState.markerState.mesh) return;
    
    // Update our persistent reference
    currentMarkedSystem = system;
    
    // Get system position in world space
    const systemPos = new THREE.Vector3(system.x, system.y, system.z);
    
    // Project system position to screen space
    const screenPos = systemPos.clone().project(camera);
    
    // Add fixed offset in screen space (0.05 is 5% of screen height)
    const SCREEN_OFFSET = 0.15;
    screenPos.y += SCREEN_OFFSET;
    
    // Unproject back to world space
    const worldPos = screenPos.unproject(camera);
    
    // Position marker at the unprojected position
    globalState.markerState.mesh.position.copy(worldPos);
    
    // Scale marker based on distance to maintain readable size
    const distance = camera.position.distanceTo(systemPos);
    const scale = distance * 0.03;
    
    globalState.markerState.mesh.visible = true;
    globalState.markerState.mesh.quaternion.copy(camera.quaternion);
    globalState.markerState.mesh.scale.setScalar(scale);
    globalState.markerState.mesh.renderOrder = 999999;
}

// Hide the markers
function hideMarkers() {
    if (pulsingMarker) {
        pulsingMarker.visible = false;
        stopPulseAnimation();
    }
    if (globalState.markerState.mesh) {
        globalState.markerState.mesh.visible = false;
    }
}

// Update cleanup to include marker cleanup
export function cleanup() {
    cleanupOutOfRangePlanes();
    
    stopPulseAnimation();
    
    if (pulsingMarker) {
        scene.remove(pulsingMarker);
        pulsingMarker.geometry.dispose();
        pulsingMarker.material.dispose();
        pulsingMarker = null;
    }
    
    if (globalState.markerState.mesh) {
        scene.remove(globalState.markerState.mesh);
        if (markerGeometry) markerGeometry.dispose();
        if (globalState.markerState.mesh.material) globalState.markerState.mesh.material.dispose();
        globalState.markerState.mesh = null;
    }
    
    if (collisionInstancedMesh) {
        scene.remove(collisionInstancedMesh);
        collisionInstancedMesh.geometry.dispose();
        collisionInstancedMesh.material.dispose();
        collisionInstancedMesh = null;
    }
    
    if (hoverSphere) {
        scene.remove(hoverSphere);
        hoverSphere.geometry.dispose();
        hoverSphere.material.dispose();
        hoverSphere = null;
    }
    
    if (systems) {
        Object.values(systems).forEach(system => {
            if (system.collisionMesh) {
                scene.remove(system.collisionMesh);
                system.collisionMesh.geometry.dispose();
                system.collisionMesh.material.dispose();
                delete system.collisionMesh;
            }
        });
    }
    if (textObjects) {
        textObjects.forEach(textObj => {
            if (textObj.debugPlane) {
                scene.remove(textObj.debugPlane);
                textObj.debugPlane.geometry.dispose();
                textObj.debugPlane.material.dispose();
                delete textObj.debugPlane;
            }
        });
    }
    currentMarkedSystem = null;
}

function animate() {
    if (pulsingMarker && pulsingMarker.visible) {
        const scale = pulseScale * baseScale;
        pulsingMarker.scale.set(scale, scale, scale);
    }
    
    // Always update hover effects regardless of SHOW_COLLISION_SPHERES
    if (hoverSphere && hoverSphere.visible) {
        hoverMaterial.uniforms.time.value = performance.now() / 1000;
    }
    
    if (currentMarkedSystem && globalState.markerState.mesh && camera) {
        updateMarkerPosition(currentMarkedSystem);
    }
    
    requestAnimationFrame(animate);
}

animate(); // Start the animation loop
