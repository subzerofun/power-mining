import * as THREE from '../threejs/three.module.js';
//import { createCustomGrid } from './grid.js';
import { setMovingToTarget, updateSystemPoints, updateTextVisibility } from './systems.js';
import { updateLineMarkers } from './lineMarkers.js';   
import { updateActiveSystem } from './systemView.js';
import { PowerVisualizerExtra } from '/js/map/visualizeExtra.js';

let camera;
let controls;
let targetPoint;
let gridGroup;
let GRIDSWITCH;
let systems;

export function initCamera(deps) {
    camera = deps.camera;
    controls = deps.controls;
    targetPoint = deps.targetPoint;
    gridGroup = deps.gridGroup;
    GRIDSWITCH = deps.GRIDSWITCH;
    systems = deps.systems;
}

export function moveToSystem(system, onComplete) {
    if (!system || !camera || !controls) return;

    // Set movement flag and disable unnecessary updates
    setMovingToTarget(true);
    
    // Temporarily disable OrbitControls
    controls.enabled = false;

    // Store initial camera offset from target point
    const cameraOffset = camera.position.clone().sub(targetPoint);
    const currentDistance = cameraOffset.length();
    const targetDistance = 20;  // Target zoom level
    
    // Create proxy object for animation - use coordinates directly
    const proxy = {
        x: targetPoint.x,
        y: targetPoint.y,
        z: targetPoint.z,
        progress: 0
    };

    // Pre-calculate if we need to switch grid density
    const willCrossGridThreshold = (currentDistance > GRIDSWITCH && targetDistance < GRIDSWITCH) || 
                                 (currentDistance < GRIDSWITCH && targetDistance > GRIDSWITCH);

    // Animate using the proxy object - use system coordinates directly
    gsap.to(proxy, {
        x: system.x,  // Use coordinates directly - they're already in Elite format
        y: system.y,
        z: system.z,
        progress: 1,
        duration: 2,
        ease: 'power2.inOut',
        onUpdate: () => {
            // Update targetPoint using proxy values directly
            targetPoint.set(proxy.x, proxy.y, proxy.z);
            
            // Interpolate the camera distance
            const newDistance = THREE.MathUtils.lerp(currentDistance, targetDistance, proxy.progress);
            
            // Move camera maintaining same direction but adjusting distance
            camera.position.copy(targetPoint).add(cameraOffset.clone().normalize().multiplyScalar(newDistance));
            controls.target.copy(targetPoint);
            
            // Only essential updates during movement
            updateTextVisibility();
            updateLineMarkers(systems);
           

            // Only update grid at specific progress points if needed
            if (willCrossGridThreshold && (proxy.progress === 0.5 || proxy.progress === 1)) {
                //createCustomGrid();
            }
        },
        onComplete: () => {
            // Re-enable controls and updates
            controls.enabled = true;
            setMovingToTarget(false);
            
            // Do full update only after movement is complete
            updateActiveSystem();
            updateSystemPoints();
            updateTextVisibility();
            updateLineMarkers(systems);
            PowerVisualizerExtra.updatePowerGlow(camera);
            
            // Final grid update if needed
            if (camera.position.distanceTo(controls.target) < GRIDSWITCH) {
                //createCustomGrid();
            }

            if (onComplete) onComplete();
        }
    });

    // Animate the grid's Y position
    gsap.to(gridGroup.position, {
        y: system.y,
        duration: 2,
        ease: 'power2.inOut'
    });
}

export function updateCameraTarget() {
    if (!camera || !controls) return;
    
    controls.update();
    
    // Update grid text scales
    if (gridGroup) {
        gridGroup.children.forEach(child => {
            if (child.updateScale) {
                const distanceToTarget = camera.position.distanceTo(controls.target);
                child.updateScale(distanceToTarget);
            }
        });
    }
} 