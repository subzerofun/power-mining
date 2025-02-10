import * as THREE from '../threejs/three.module.js';
import { updateMarkerPosition } from './clickHandler.js';
import { globalState } from './globalState.js';

let scene = null;
let camera = null;
let pulseAnimation = null;

function createFirstClickMarker() {
    const geometry = new THREE.CircleGeometry(0.8, 24);
    const material = new THREE.ShaderMaterial({
        uniforms: {
            color: { value: new THREE.Color(0x00ffff) },
            progress1: { value: 0.0 },
            progress2: { value: -0.5 }  // Start second ring at -50% to properly stagger
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
            uniform float progress1;
            uniform float progress2;
            varying vec2 vUv;
            
            float getRingOpacity(float dist, float progress) {
                // Calculate distance from the expanding wave front
                float ringPos = progress * 3.0;  // Expand to 3x size
                float distFromRing = abs(dist - ringPos);
                
                // Create a thicker ring that fades based on distance from wave front
                float ringWidth = 0.35;  // Increased width of the expanding ring
                float ringOpacity = smoothstep(ringWidth, 0.0, distFromRing);
                
                // Fade out as the ring expands
                float fadeOut = 1.0 - smoothstep(0.8, 1.0, progress);
                
                return ringOpacity * fadeOut;
            }
            
            void main() {
                // Calculate distance from center
                vec2 center = vec2(0.5, 0.5);
                float dist = length(vUv - center) * 2.0;  // Normalize to 0-1 range
                
                // Calculate opacity for both expanding rings
                float opacity1 = getRingOpacity(dist, progress1);
                float opacity2 = getRingOpacity(dist, max(0.0, progress2));  // Only show when progress2 > 0
                
                // Combine the rings
                float finalOpacity = opacity1 + opacity2;
                
                gl_FragColor = vec4(color, finalOpacity * 0.5);
            }
        `,
        transparent: true,
        side: THREE.DoubleSide,
        depthTest: false,
        depthWrite: false,
        blending: THREE.AdditiveBlending
    });

    const mesh = new THREE.Mesh(geometry, material);
    mesh.renderOrder = 2;
    mesh.visible = false;
    return mesh;
}

function createSecondClickMarker() {
    // Create SVG marker (will be loaded later)
    const mesh = new THREE.Mesh();
    mesh.renderOrder = 999999;
    mesh.visible = false;
    return mesh;
}

function startPulseAnimation() {
    if (pulseAnimation) return;
    
    const duration = 4.0;  // Duration for each wave
    
    // Create timeline for continuous ripples
    pulseAnimation = gsap.timeline({
        repeat: -1,
        onUpdate: () => {
            if (!globalState.firstClickMarker || !globalState.firstClickMarker.visible) return;
            
            const material = globalState.firstClickMarker.material;
            const currentTime = pulseAnimation.time();
            
            // First wave goes from 0 to 1
            const progress1 = (currentTime % duration) / duration;
            
            // Second wave starts at -0.5 and follows the first wave
            const progress2 = ((currentTime % duration) / duration) - 0.5;
            
            material.uniforms.progress1.value = progress1;
            material.uniforms.progress2.value = progress2;
        }
    });

    // Single animation that runs continuously
    pulseAnimation.to({}, {
        duration: duration,
        repeat: -1,
        ease: "none"
    });
}

export function stopPulseAnimation() {
    if (pulseAnimation) {
        pulseAnimation.kill();
        pulseAnimation = null;
        // Reset scale when stopping
        if (globalState.firstClickMarker) {
            globalState.firstClickMarker.scale.setScalar(1);
            if (globalState.firstClickMarker.children[0]) {
                globalState.firstClickMarker.children[0].scale.setScalar(1);
            }
        }
    }
}

export function initSystemView(deps) {
    console.log("Initializing system view with deps:", deps);
    scene = deps.scene;
    camera = deps.camera;
    
    // Create and add both markers to scene
    globalState.firstClickMarker = createFirstClickMarker();
    scene.add(globalState.firstClickMarker);
}

export function handleFirstClick(system) {
    if (!globalState.firstClickMarker) return;
    
    // Show cyan circle centered on system - use coordinates directly
    globalState.firstClickMarker.position.copy(new THREE.Vector3(system.x, system.y, system.z));
    globalState.firstClickMarker.visible = true;
    globalState.firstClickMarker.quaternion.copy(camera.quaternion);
    
    // Apply smooth scaling for cyan circle based on distance
    const distance = camera.position.distanceTo(globalState.firstClickMarker.position);
    const scale = distance <= 10 ? 1.0 : Math.max(0.3, 1.0 - (distance - 10) / 240);
    globalState.firstClickMarker.scale.setScalar(scale);
    
    startPulseAnimation();
    
    // Hide SVG marker if visible
    if (globalState.markerState.mesh) {
        globalState.markerState.mesh.visible = false;
    }
    
    globalState.selectedSystem = system;
}

export function handleSystemClick(systemOrTextObject) {
    if (!globalState.markerState.mesh || !globalState.firstClickMarker) {
        console.error("Markers are null!");
        return;
    }

    let system;
    if (systemOrTextObject.systemCoords) {
        system = {
            x: systemOrTextObject.systemCoords.x,
            y: systemOrTextObject.systemCoords.y,
            z: systemOrTextObject.systemCoords.z,
            name: systemOrTextObject.systemName
        };
    } else {
        system = systemOrTextObject;
    }

    globalState.selectedSystem = system;
    updateMarkerPosition(system);
}

export function handleEmptySpaceClick() {
    // Only hide the SVG marker
    if (globalState.markerState.mesh) {
        globalState.markerState.mesh.visible = false;
    }
   // Keep firstClickMarker (cyan circle) visible and pulsing
}

export function updateActiveSystem() {
    if (!camera) return;
    /*
    console.log('updateActiveSystem running', {
        marker: markerState.mesh ? 'exists' : 'null',
        markerVisible: markerState.mesh ? markerState.mesh.visible : 'n/a',
        selectedSystem: selectedSystem ? selectedSystem.name : 'null'
    });
    */

    if (globalState.firstClickMarker && globalState.firstClickMarker.visible) {
        const distance = camera.position.distanceTo(globalState.firstClickMarker.position);
        const scale = distance * 0.03;
        globalState.firstClickMarker.scale.setScalar(scale);
        globalState.firstClickMarker.quaternion.copy(camera.quaternion);
    }

    if (globalState.markerState.mesh && globalState.markerState.mesh.visible && globalState.selectedSystem) {
       //console.log('Calling updateMarkerPosition from updateActiveSystem');
        updateMarkerPosition(globalState.selectedSystem);
    }
    
}

export function getSelectedSystem() {
    return globalState.selectedSystem;
}

// Function to handle search results
export function handleSearchResult(system) {
    // First, show the cyan circle
    handleFirstClick(system);
    
    // Then show the orange marker
    handleSystemClick(system);
}
