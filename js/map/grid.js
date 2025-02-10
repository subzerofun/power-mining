import * as THREE from '../threejs/three.module.js';
import { Line2 } from '../threejs/jsm/lines/Line2.js';
import { LineMaterial } from '../threejs/jsm/lines/LineMaterial.js';
import { LineGeometry } from '../threejs/jsm/lines/LineGeometry.js';
import { TextGeometry } from '../threejs/jsm/geometries/TextGeometry.js';

// Debug flag
const DEBUG_PERF = false;

function debugLog(type, ...args) {
    if (!DEBUG_PERF) return;
    if (type === 'time') {
        console.time(...args);
    } else if (type === 'timeEnd') {
        console.timeEnd(...args);
    } else {
        console.log(...args);
    }
}

// Shared state between map.html and grid.js
let gridGroup = null;
let font = null;
let fontLoaded = false;
let camera = null;
let controls = null;
let GRIDSWITCH = 50;
let targetPoint = null;  // Will be set by initGrid

// Add at top after other constants
const LINE_OFFSET = 0.015;  // Offset for line thickness
const THICK_LINE_COPIES = 8;  // Copies for 100ly lines
const MEDIUM_LINE_COPIES = 4; // Copies for 10ly lines (far view)
const MEDIUM_LINE_COPIES_LOCAL = 10; // Copies for 10ly lines (local view)
const THIN_LINE_COPIES = 2;   // Copies for 1ly lines
const USE_THICK_LINES = true; // Toggle for performance

// Global opacity settings
const OPACITY_THICK = 0.03;  // For 100ly lines
const OPACITY_MEDIUM = 0.03; // For 10ly lines in far view
const OPACITY_MEDIUM_LOCAL = 0.04; // For 10ly lines in local view
const OPACITY_THIN = 0.04;   // For 1ly lines

// Cache for grid materials and geometries
const gridCache = {
    materials: {
        thick: null,
        thin: null
    },
    geometries: new Map(),
    lines: new Map(),
    lastUpdate: 0,
    UPDATE_INTERVAL: 0  // Only update grid every 100ms
};

// Add new grid buffers
let thickGridBuffer = null;
let thinGridBuffer = null;



// Helper to get or create a line geometry
function getOrCreateLineGeometry(start, end) {
    debugLog('time', 'getOrCreateLineGeometry');
    const key = `${start.join(',')},${end.join(',')}`;
    let geometry;
    if (gridCache.geometries.has(key)) {
        geometry = gridCache.geometries.get(key);
    } else {
        const positions = [...start, ...end];
        geometry = new LineGeometry();
        geometry.setPositions(positions);
        gridCache.geometries.set(key, geometry);
    }
    debugLog('timeEnd', 'getOrCreateLineGeometry');
    return geometry;
}

// Helper to get or create a line
function getOrCreateLine(start, end, isThick) {
    debugLog('time', 'getOrCreateLine');
    const key = `${start.join(',')},${end.join(',')}`;
    const cache = gridCache.lines;
    let line;
    
    if (cache.has(key)) {
        line = cache.get(key);
    } else {
        const geometry = getOrCreateLineGeometry(start, end);
        const material = isThick ? gridCache.materials.thick : gridCache.materials.thin;
        line = new Line2(geometry, material);
        line.computeLineDistances();
        cache.set(key, line);
    }
    debugLog('timeEnd', 'getOrCreateLine');
    return line;
}

// Initialize module with required dependencies
export function initGrid(deps) {
    debugLog('time', 'initGrid');
    gridGroup = deps.gridGroup;
    font = deps.font;
    fontLoaded = deps.fontLoaded;
    targetPoint = deps.targetPoint;
    camera = deps.camera;
    controls = deps.controls;
    GRIDSWITCH = deps.GRIDSWITCH;

    // Create reusable materials
    gridCache.materials.thick = new LineMaterial({
        color: 0x00ffff,
        linewidth: 4,
        transparent: true,
        opacity: 0.1,
        depthWrite: false,
        fog: true,
        blending: THREE.AdditiveBlending,
        resolution: new THREE.Vector2(window.innerWidth, window.innerHeight)
    });

    gridCache.materials.thin = new LineMaterial({
        color: 0x00ffff,
        linewidth: 2,
        transparent: true,
        opacity: 0.1,
        depthWrite: false,
        fog: true,
        blending: THREE.AdditiveBlending,
        resolution: new THREE.Vector2(window.innerWidth, window.innerHeight)
    });
    debugLog('timeEnd', 'initGrid');
}

// Check if module is initialized
function checkInitialized() {
    if (!gridGroup || !camera || !controls || !targetPoint) {
        throw new Error('Grid module not initialized. Call initGrid first.');
    }
}

export function createThickGridLines(interval, divisions, color, thickness, opacity = 0.1) {
    debugLog('time', 'createThickGridLines');
    checkInitialized();
    
    const halfSize = divisions * interval / 2;
    const lines = [];

    debugLog('time', '- Creating X lines');
    // Create X lines (parallel to X axis)
    for (let i = -halfSize; i <= halfSize; i += interval) {
        const start = [-halfSize, 0, i];
        const end = [halfSize, 0, i];
        const line = getOrCreateLine(start, end, thickness >= 10);
        lines.push(line);
    }
    debugLog('timeEnd', '- Creating X lines');

    debugLog('time', '- Creating Z lines');
    // Create Z lines (parallel to Z axis)
    for (let i = -halfSize; i <= halfSize; i += interval) {
        const start = [i, 0, -halfSize];
        const end = [i, 0, halfSize];
        const line = getOrCreateLine(start, end, thickness >= 10);
        lines.push(line);
    }
    debugLog('timeEnd', '- Creating Z lines');

    debugLog('timeEnd', 'createThickGridLines');
    return lines;
}

// Function to update grid text Y coordinate
function updateGridTextY() {
    if (!gridGroup) return;
    gridGroup.children.forEach(child => {
        if (child.geometry && child.geometry.type === 'TextGeometry') {
            const x = child.userData.tileX;  // Add same offset as createCustomGrid
            const z = child.userData.tileZ;
            const text = `${x} : ${Math.round(targetPoint.y)} : ${z}`;
            
            // Only update the text content if it has changed
            if (child.userData.currentText !== text) {
                child.userData.currentText = text;
                // Dispose old geometry
                if (child.geometry) child.geometry.dispose();
                
                // Create new geometry with updated text
                child.geometry = new TextGeometry(text, {
                    font: font,
                    size: 0.85,
                    depth: 0.05,
                    curveSegments: 3,
                    bevelEnabled: false
                });
                
                // Center geometry
                child.geometry.computeBoundingBox();
                const textWidth = child.geometry.boundingBox.max.x - child.geometry.boundingBox.min.x;
                const textHeight = child.geometry.boundingBox.max.y - child.geometry.boundingBox.min.y;
                child.geometry.translate(-textWidth/2, -textHeight/2, 0);
            }
        }
    });
}

// Export the update function
export { updateGridTextY };

function createGridText(x, z, gridY, padding = 2) {
    /*
    checkInitialized();
    if (!fontLoaded) return null;  // Don't create text if font isn't loaded yet

    // Swap x and z for display, and use gridGroup's Y position
    const text = `${z} : ${Math.round(gridGroup.position.y)} : ${x}`;
    const textMaterial = new THREE.MeshBasicMaterial({ 
        color: 0x00ffff,
        transparent: true,
        opacity: 0.2,
        depthWrite: false,
        side: THREE.DoubleSide,
        blending: THREE.AdditiveBlending
    });

    const textGeometry = new TextGeometry(text, {
        font: font,
        size: 0.85,
        depth: 0.05,
        curveSegments: 3,
        bevelEnabled: false
    });

    textGeometry.computeBoundingBox();
    const textWidth = textGeometry.boundingBox.max.x - textGeometry.boundingBox.min.x;
    const textHeight = textGeometry.boundingBox.max.y - textGeometry.boundingBox.min.y;

    // Center geometry horizontally and vertically
    textGeometry.translate(-textWidth/2, -textHeight/2, 0);
    
    const textMesh = new THREE.Mesh(textGeometry, textMaterial);
    textMesh.renderOrder = 3;

    // Store the original coordinates and current Y for updates
    textMesh.userData = {
        tileX: x,
        tileZ: z,
        currentY: Math.round(gridGroup.position.y),
        textWidth: textWidth,
        textHeight: textHeight
    };

    // Create a function to update text scale using distance to orbit target
    textMesh.updateScale = function(distanceToTarget) {
        // Hide text if zoom level is too high
        if (distanceToTarget > 450) {
            this.visible = false;
            return;
        }
        this.visible = true;

        const scale = distanceToTarget * 0.02;
        this.scale.set(scale, scale, scale);

        // Calculate position offset based on current scale to maintain fixed margin
        const scaledWidth = this.userData.textWidth * scale;

        // The margin should be proportional to the tile size
        if (this.userData.tileSize === 100) {
            const tileSize = 100;
            const marginRatio = 0.25;  // 5% of tile size for margin
            const margin = tileSize * marginRatio;
            // Position in top right corner
            this.position.set(
                this.userData.tileX + tileSize - margin * distanceToTarget * 0.0024,  // Right edge
                0,
                this.userData.tileZ - margin * 1.4 * distanceToTarget * 0.0023  // Top edge
            );
        } else {
            const tileSize = 10;
            const marginRatio = 0.05;
            const margin = tileSize * marginRatio;
            // Position in top right corner
            this.position.set(
                this.userData.tileX + tileSize - margin * distanceToTarget * 0.12,  // Right edge
                0,
                this.userData.tileZ - margin * distanceToTarget * 0.16  // Top edge
            );
        }

        // Update Y value if changed
        const newY = Math.round(targetPoint.y);
        if (this.userData.currentY !== newY) {
            this.userData.currentY = newY;
            if (this.geometry) this.geometry.dispose();
            let text;
            if (this.userData.tileSize === 100) {
                text = `${this.userData.tileX + 100} : ${newY} : ${this.userData.tileZ}`;
            } else if (this.userData.tileSize === 10) 
            { 
                text = `${this.userData.tileX + 10} : ${newY} : ${this.userData.tileZ}`;
            } else {
                text = `${this.userData.tileX} : ${newY} : ${this.userData.tileZ}`;
            }

            this.geometry = new TextGeometry(text, {
                font: font,
                size: 0.85,
                depth: 0.05,
                curveSegments: 3,
                bevelEnabled: false
            });
            this.geometry.computeBoundingBox();
            const newWidth = this.geometry.boundingBox.max.x - this.geometry.boundingBox.min.x;
            // Right-align by translating by the full width, and vertically center
            this.geometry.translate(-newWidth +2, -this.userData.textHeight/2, 0);
        }
    };

    // Set initial rotations
    textMesh.rotation.x = -Math.PI / 2;  // Lay flat
    textMesh.rotation.z = -Math.PI / 2;  // Rotate 90° counterclockwise

    return textMesh;
    */
}

export function createCustomGrid() {
    debugLog('time', 'Total createCustomGrid');
    checkInitialized();

    const zoomLevel = camera.position.distanceTo(controls.target);

    // Create or update grid buffers
    if (!thickGridBuffer || !thinGridBuffer) {
        // Create materials
        const thickMaterial = new THREE.LineBasicMaterial({
            color: 0x00ffff,
            transparent: true,
            opacity: OPACITY_THICK,
            depthWrite: false,
            blending: THREE.AdditiveBlending
        });

        const thinMaterial = new THREE.LineBasicMaterial({
            color: 0x00ffff,
            transparent: true,
            opacity: zoomLevel > GRIDSWITCH ? OPACITY_MEDIUM : OPACITY_MEDIUM_LOCAL,
            depthWrite: false,
            blending: THREE.AdditiveBlending
        });

        // Create buffers
        thickGridBuffer = new THREE.LineSegments(
            new THREE.BufferGeometry(),
            thickMaterial
        );
        thinGridBuffer = new THREE.LineSegments(
            new THREE.BufferGeometry(),
            thinMaterial
        );

        gridGroup.add(thickGridBuffer);
        gridGroup.add(thinGridBuffer);
    } else {
        // Update opacity based on zoom level
        thinGridBuffer.material.opacity = zoomLevel > GRIDSWITCH ? OPACITY_MEDIUM : OPACITY_MEDIUM_LOCAL;
    }

    // Update grid positions
    if (zoomLevel > GRIDSWITCH) {
        // Create main grid (10ly and 100ly)
        const positions100 = [];
        const positions10 = [];
        const halfSize = 5000;

        // Create 100ly grid
        for (let i = -halfSize; i <= halfSize; i += 100) {
            if (USE_THICK_LINES) {
                for (let copy = 0; copy < THICK_LINE_COPIES; copy++) {
                    const offset = LINE_OFFSET * copy;
                    positions100.push(-halfSize, 0, i + offset, halfSize, 0, i + offset); // X lines
                    positions100.push(i + offset, 0, -halfSize, i + offset, 0, halfSize); // Z lines
                }
            } else {
                positions100.push(-halfSize, 0, i, halfSize, 0, i); // X lines
                positions100.push(i, 0, -halfSize, i, 0, halfSize); // Z lines
            }
        }

        // Create 10ly grid
        for (let i = -halfSize; i <= halfSize; i += 10) {
            if (USE_THICK_LINES) {
                for (let copy = 0; copy < MEDIUM_LINE_COPIES; copy++) {
                    const offset = LINE_OFFSET * copy;
                    positions10.push(-halfSize, 0, i + offset, halfSize, 0, i + offset); // X lines
                    positions10.push(i + offset, 0, -halfSize, i + offset, 0, halfSize); // Z lines
                }
            } else {
                positions10.push(-halfSize, 0, i, halfSize, 0, i); // X lines
                positions10.push(i, 0, -halfSize, i, 0, halfSize); // Z lines
            }
        }

        thickGridBuffer.geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions100, 3));
        thinGridBuffer.geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions10, 3));
    } else {
        // Local grid
        const centerX = Math.round(targetPoint.x);
        const centerZ = Math.round(targetPoint.z);
        const gridSize = 100;
        const halfSize = gridSize / 2;

        const thickPositions = [];  // For 10ly lines
        const thinPositions = [];   // For 1ly lines

        // Create grid lines
        for (let x = centerX - halfSize; x <= centerX + halfSize; x++) {
            if (x % 10 === 0) {
                if (USE_THICK_LINES) {
                    for (let copy = 0; copy < MEDIUM_LINE_COPIES_LOCAL; copy++) {
                        const offset = LINE_OFFSET * copy;
                        thickPositions.push(x + offset, 0, centerZ - halfSize, x + offset, 0, centerZ + halfSize);
                    }
                } else {
                    thickPositions.push(x, 0, centerZ - halfSize, x, 0, centerZ + halfSize);
                }
            } else {
                if (USE_THICK_LINES) {
                    for (let copy = 0; copy < THIN_LINE_COPIES; copy++) {
                        const offset = LINE_OFFSET * copy;
                        thinPositions.push(x + offset, 0, centerZ - halfSize, x + offset, 0, centerZ + halfSize);
                    }
                } else {
                    thinPositions.push(x, 0, centerZ - halfSize, x, 0, centerZ + halfSize);
                }
            }
        }

        for (let z = centerZ - halfSize; z <= centerZ + halfSize; z++) {
            if (z % 10 === 0) {
                if (USE_THICK_LINES) {
                    for (let copy = 0; copy < MEDIUM_LINE_COPIES_LOCAL; copy++) {
                        const offset = LINE_OFFSET * copy;
                        thickPositions.push(centerX - halfSize, 0, z + offset, centerX + halfSize, 0, z + offset);
                    }
                } else {
                    thickPositions.push(centerX - halfSize, 0, z, centerX + halfSize, 0, z);
                }
            } else {
                if (USE_THICK_LINES) {
                    for (let copy = 0; copy < THIN_LINE_COPIES; copy++) {
                        const offset = LINE_OFFSET * copy;
                        thinPositions.push(centerX - halfSize, 0, z + offset, centerX + halfSize, 0, z + offset);
                    }
                } else {
                    thinPositions.push(centerX - halfSize, 0, z, centerX + halfSize, 0, z);
                }
            }
        }

        thickGridBuffer.geometry.setAttribute('position', new THREE.Float32BufferAttribute(thickPositions, 3));
        thinGridBuffer.geometry.setAttribute('position', new THREE.Float32BufferAttribute(thinPositions, 3));
    }

    debugLog('timeEnd', 'Total createCustomGrid');
}

