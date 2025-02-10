import * as THREE from '../threejs/three.module.js';

let scene = null;
let cubeTexture = null;
let showBackground = true;

export function initBackgroundToggle(sceneRef, cubeTextureRef) {
    scene = sceneRef;
    cubeTexture = cubeTextureRef;
}

export function toggleBackground() {
    showBackground = !showBackground;
    if (showBackground) {
        scene.background = cubeTexture;
    } else {
        scene.background = new THREE.Color(0x000000);
    }
    return showBackground;
}
