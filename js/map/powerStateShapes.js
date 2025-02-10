import * as THREE from '../threejs/three.module.js';
import { SVGLoader } from '../threejs/jsm/loaders/SVGLoader.js';
import { mergeGeometries } from '../threejs/jsm/utils/BufferGeometryUtils.js';

// Constants for power state influence radii
export const INFLUENCE_RADII = {
    'Stronghold': 30,
    'Stronghold-Carrier': 30,
    'Fortified': 20,
    'Exploited': 10,
    'Unoccupied': 0
};

const SHAPE_SIZE = 2.8; // Base size for shapes
const loader = new SVGLoader();
const geometries = {};

// Load and convert SVG to geometry
function loadSvgGeometry(path) {
    return new Promise((resolve, reject) => {
        loader.load(
            path,
            (data) => {
                const tempGeometries = [];

                // ----- 1) Convert each path to shapes -----
                data.paths.forEach((svgPath, index) => {
                    // createShapes() turns each path's subPath(s) into one or more THREE.Shape objects
                    const shapes = SVGLoader.createShapes(svgPath);

                    shapes.forEach((shape) => {
                        const geometry = new THREE.ShapeGeometry(shape);
                        tempGeometries.push(geometry);
                    });
                });

                // ----- 2) Merge geometries into one if you like -----
                let finalGeometry;
                if (tempGeometries.length === 0) {
                    console.warn('No geometry created for', path);
                    finalGeometry = createFallbackGeometry();
                } else if (tempGeometries.length === 1) {
                    finalGeometry = tempGeometries[0];
                } else {
                    // Use the helper from three/examples/jsm/utils/BufferGeometryUtils
                    // to merge them properly, preserving the shape outlines.
                    finalGeometry = mergeGeometries(tempGeometries, false);
                }

                // ----- 3) Apply scaling/centering if desired -----
                // Example: scale to a certain size, flip Y (by negating scale),
                // and center in the scene.
                finalGeometry.scale(SHAPE_SIZE / 40, -SHAPE_SIZE / 40, SHAPE_SIZE / 40);
                finalGeometry.center();

                resolve(finalGeometry);
            },
            undefined, // onProgress callback not needed
            (error) => {
                console.error('Error loading SVG:', error);
                reject(error);
            }
        );
    });
}

// Initialize all geometries
export async function initGeometries() {
    const svgFiles = {
        'Stronghold-Carrier': '/img/map/stronghold-carrier.svg',
        'Stronghold': '/img/map/stronghold.svg',
        'Fortified': '/img/map/fortified.svg',
        'Exploited': '/img/map/exploited.svg',
        'Unoccupied': '/img/map/unoccupied.svg',
        'Expansion': '/img/map/unoccupied-expansion.svg',
        'Contested': '/img/map/contested.svg',
        'marker': '/img/map/marker.svg'  // Add marker SVG
    };

    try {
        for (const [state, path] of Object.entries(svgFiles)) {
            geometries[state] = await loadSvgGeometry(path);
        }
        console.log('All power state geometries loaded');
        return true;  // Signal successful loading
    } catch (error) {
        console.error('Failed to load some geometries:', error);
        return false;  // Signal loading failure
    }
}

// Initialize geometries when module loads but don't wait
const geometriesLoading = initGeometries();

export function waitForGeometries() {
    return geometriesLoading;
}

// Fallback geometry in case SVGs are still loading
function createFallbackGeometry() {
    // Create a tiny point instead of a circle - effectively invisible
    const shape = new THREE.Shape();
    shape.moveTo(0, 0);
    shape.lineTo(0.0001, 0.0001);
    shape.lineTo(0.0001, 0);
    return new THREE.ShapeGeometry(shape);
}

export function getGeometryForPowerState(powerState) {
    // Return unoccupied as fallback while loading or if state not found
    return geometries[powerState] || geometries['Unoccupied'] || createFallbackGeometry();
}

// Power capitals handling
class PowerCapitalIcons {
    constructor() {
        this.capitalSystems = new Map();
        this.meshes = new Map();
        this.textureLoader = new THREE.TextureLoader();
        this.geometry = new THREE.PlaneGeometry(1, 1);
        this.baseScale = 60; // Base icon size in pixels
    }

    async initialize(scene) {
        const response = await fetch('/data/power-capitals.csv');
        const csvText = await response.text();
        const lines = csvText.split('\n').slice(1); // Skip header

        // Configure texture loader for proper color space
        this.textureLoader.encoding = THREE.sRGBEncoding;

        // Load all textures first
        const texturePromises = lines.map(line => {
            const [power, capital, icon, x, y, z, id64] = line.split(',');
            return this.textureLoader.loadAsync(`/img/power-icons/${icon}`).then(texture => {
                texture.encoding = THREE.sRGBEncoding;
                texture.colorSpace = 'srgb';
                return {
                    power, texture, position: {
                        x: -parseFloat(x), // Negate x for Elite coordinates
                        y: parseFloat(y),
                        z: parseFloat(z)
                    },
                    id64: parseInt(id64)
                };
            });
        });

        // Wait for all textures to load
        const capitals = await Promise.all(texturePromises);

        // Create meshes for each capital
        capitals.forEach(({ power, texture, position, id64 }) => {
            const material = new THREE.MeshBasicMaterial({
                map: texture,
                transparent: true,
                depthTest: false,
                depthWrite: false,
                opacity: 1  // Slightly reduce opacity
            });
            material.toneMapped = false;  // Prevent automatic tone mapping

            const mesh = new THREE.Mesh(this.geometry, material);
            mesh.position.set(position.x, position.y, position.z);
            mesh.renderOrder = 9999; // Ensure icons render on top

            scene.add(mesh);
            this.meshes.set(power, mesh);
            this.capitalSystems.set(id64, true);
        });
    }

    updateIconsForCamera(camera) {
        const pixelsPerUnit = this.baseScale;
        
        this.meshes.forEach(mesh => {
            // Make icon face camera
            mesh.quaternion.copy(camera.quaternion);
            
            // Scale to maintain constant screen size
            const distance = camera.position.distanceTo(mesh.position);
            const scale = distance * (pixelsPerUnit / 1000);
            mesh.scale.set(scale, scale, scale);
        });
    }

    isCapitalSystem(id64) {
        return this.capitalSystems.has(id64);
    }
}

// Create singleton instance
export const powerCapitals = new PowerCapitalIcons();

// Export initialization and update functions
export function initPowerCapitals(scene) {
    return powerCapitals.initialize(scene);
}

export function updatePowerCapitals(camera) {
    powerCapitals.updateIconsForCamera(camera);
} 