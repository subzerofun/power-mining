// Power visualization extra functions for Elite Dangerous map
import * as THREE from '../threejs/three.module.js';
import { STAR_VISIBLE_RADIUS, MAX_RADIUS } from './globalState.js';

// Constants for star visibility (copied from systems.js)
const MAX_ZOOM_LEVEL = 300;    // Show all stars at this zoom level and above
const MIN_ZOOM_LEVEL = 30;     // Minimum zoom level for visibility calculation
const MIN_RADIUS = 30;         // Minimum radius in ly for star visibility (at zoom 30)
const FADE_START_ZOOM = 200;   // Start reducing visible radius at this zoom level

// Global variables
let gasCloudMeshes = [];
let showGasClouds = true;
let lightDirectionArrow = null;

// Export functions to be used by main visualizer
export const PowerVisualizerExtra = {
    scene: null,
    geodesicObjects: null,
    showGeodesicTerritories: false,
    powerGlowObjects: new Map(),
    showPowerGlow: true, // On by default
    systemsInitialized: false,
    targetPoint: null,  // Move targetPoint here

    init(mainVisualizer) {
        this.scene = mainVisualizer.scene;
        this.geodesicObjects = mainVisualizer.geodesicObjects;
        this.showGeodesicTerritories = mainVisualizer.showGeodesicTerritories;
        this.targetPoint = mainVisualizer.targetPoint;  // Set as object property
    },

    // Add this new function to be called from SystemsLoader after systems are loaded
    initializeSystems(systems, camera) {
        // Only check for systems now
        if (!systems) {
            console.log('Waiting for systems to be initialized...');
            return;
        }

        this.systemsInitialized = true;
        console.log('Systems initialized, creating power glow...');
        // Initial creation of power glow if enabled
        if (this.showPowerGlow) {
            this.powerGlow(systems, camera);
        }
    },

    // Add new cleanup function specifically for geodesic territories
    cleanupGeodesicTerritories() {
        // Stop any ongoing animations first
        if (this._geodesicAnimationStarted) {
            this._geodesicAnimationStarted = false;
            if (this._animationFrameId) {
                cancelAnimationFrame(this._animationFrameId);
                this._animationFrameId = null;
            }
        }
        
        // Clean up all objects and their resources
        if (this.geodesicObjects) {
            this.geodesicObjects.forEach((objects) => {
                objects.forEach((obj) => {
                    if (obj.mesh) {
                        // Remove from scene
                        this.scene.remove(obj.mesh);
                        
                        // Dispose of geometry
                        if (obj.mesh.geometry) {
                            obj.mesh.geometry.dispose();
                        }
                        
                        // Dispose of material and its textures
                        if (obj.mesh.material) {
                            if (Array.isArray(obj.mesh.material)) {
                                obj.mesh.material.forEach(material => {
                                    if (material.map) material.map.dispose();
                                    if (material.uniforms) {
                                        Object.values(material.uniforms).forEach(uniform => {
                                            if (uniform.value && uniform.value.dispose) {
                                                uniform.value.dispose();
                                            }
                                        });
                                    }
                                    material.dispose();
                                });
                            } else {
                                if (obj.mesh.material.map) obj.mesh.material.map.dispose();
                                if (obj.mesh.material.uniforms) {
                                    Object.values(obj.mesh.material.uniforms).forEach(uniform => {
                                        if (uniform.value && uniform.value.dispose) {
                                            uniform.value.dispose();
                                        }
                                    });
                                }
                                obj.mesh.material.dispose();
                            }
                        }
                    }
                });
            });
            this.geodesicObjects.clear();
        }
    },

    createGeodesicTerritories(systems, camera) {
        // Don't call clearObjects here - cleanup is handled separately
        if (!this.showGeodesicTerritories) return;

        // Create shared geometries - lower detail for better performance
        const geometries = {
            stronghold: new THREE.IcosahedronGeometry(30, 12),  // Reduced detail level
            fortified: new THREE.IcosahedronGeometry(20, 12),
            exploited: new THREE.IcosahedronGeometry(0, 12)
        };

        // Group systems by power
        const powerSystems = {};
        Object.values(systems).forEach(system => {
            if (!system.controlling_power || !system.power_state) return;
            if (!powerSystems[system.controlling_power]) {
                powerSystems[system.controlling_power] = [];
            }
            powerSystems[system.controlling_power].push(system);
        });

        // Create territories for each power
        Object.entries(powerSystems).forEach(([power, systemList]) => {
            const powerColor = window.powerColors[power];
            if (!powerColor) return;

            const color = new THREE.Color(powerColor);
            this.geodesicObjects.set(power, []);

            // Create shader material with edge glow and wireframe
            const sphereMaterial = new THREE.ShaderMaterial({
                uniforms: {
                    color: { value: color },
                    cameraPos: { value: camera.position }
                },
                vertexShader: `
                    varying vec3 vPosition;
                    varying vec3 vNormal;
                    varying vec3 vWorldPosition;
                    varying vec3 vBarycentric;
                    varying float vDepth;
                    attribute vec3 barycentric;
                    
                    void main() {
                        vPosition = position;
                        vNormal = normal;
                        vWorldPosition = (modelMatrix * vec4(position, 1.0)).xyz;
                        vBarycentric = barycentric;
                        vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
                        gl_Position = projectionMatrix * mvPosition;
                        vDepth = -mvPosition.z;  // Get view space depth
                    }
                `,
                fragmentShader: `
                    uniform vec3 color;
                    uniform vec3 cameraPos;
                    
                    varying vec3 vPosition;
                    varying vec3 vNormal;
                    varying vec3 vWorldPosition;
                    varying vec3 vBarycentric;
                    varying float vDepth;
                    
                    void main() {
                        vec3 viewDir = normalize(vWorldPosition - cameraPos);
                        float fresnel = pow(1.0 - abs(dot(normalize(vNormal), viewDir)), 3.0);  // Increased power for stronger edge glow
                        
                        // Calculate edge factor for wireframe
                        float edgeFactor = min(min(vBarycentric.x, vBarycentric.y), vBarycentric.z);
                        float wireframe = 1.0 - step(0.01, edgeFactor);
                        
                        // Combine fresnel and wireframe
                        vec3 finalColor = color;
                        float alpha = 0.03;  // Base opacity
                        
                        // Enhanced edge glow
                        if (fresnel > 0.3) {  // Lower threshold for more visible glow
                            alpha = mix(0.02, 0.6, (fresnel - 0.3) * 2.0);  // Increased max opacity
                            finalColor = mix(color, color * 1.5, (fresnel - 0.3) * 2.0);  // Stronger glow
                        }
                        
                        // Add wireframe only for front-facing edges
                        if (wireframe > 0.0) {
                            // Fade wireframe based on view angle and depth
                            float edgeVisibility = (1.0 - pow(abs(dot(viewDir, vNormal)), 10.0));
                            alpha = 0.3 * edgeVisibility;  // More transparent wireframe
                            finalColor = color * 1.3;  // Slightly brighter edges
                        }
                        
                        gl_FragColor = vec4(finalColor, alpha);
                    }
                `,
                transparent: true,
                side: THREE.FrontSide,
                depthWrite: true,  // Enable depth writing
                depthTest: true,   // Enable depth testing
                blending: THREE.NormalBlending
            });

            // Add barycentric coordinates for wireframe
            Object.values(geometries).forEach(geometry => {
                const positions = geometry.attributes.position.array;
                const vertexCount = positions.length / 3;
                const barycentric = new Float32Array(vertexCount * 3);
                
                for (let i = 0; i < vertexCount; i += 3) {
                    barycentric[i * 3] = 1;
                    barycentric[i * 3 + 1] = 0;
                    barycentric[i * 3 + 2] = 0;
                    
                    barycentric[(i + 1) * 3] = 0;
                    barycentric[(i + 1) * 3 + 1] = 1;
                    barycentric[(i + 1) * 3 + 2] = 0;
                    
                    barycentric[(i + 2) * 3] = 0;
                    barycentric[(i + 2) * 3 + 1] = 0;
                    barycentric[(i + 2) * 3 + 2] = 1;
                }
                
                geometry.setAttribute('barycentric', new THREE.BufferAttribute(barycentric, 3));
            });

            // Create spheres for each system
            systemList.forEach(system => {
                let type;
                switch(system.power_state.toLowerCase()) {
                    case 'stronghold': type = 'stronghold'; break;
                    case 'fortified': type = 'fortified'; break;
                    case 'exploited': type = 'exploited'; break;
                    default: return;
                }

                const sphereMesh = new THREE.Mesh(geometries[type], sphereMaterial.clone());
                sphereMesh.position.set(system.x, system.y, system.z);

                this.scene.add(sphereMesh);
                this.geodesicObjects.get(power).push({
                    mesh: sphereMesh
                });
            });
        });

        // Start animation for camera position updates with better cleanup
        if (!this._geodesicAnimationStarted) {
            this._geodesicAnimationStarted = true;
            const animate = () => {
                if (!this.showGeodesicTerritories || !this._geodesicAnimationStarted) {
                    this._geodesicAnimationStarted = false;
                    if (this._animationFrameId) {
                        cancelAnimationFrame(this._animationFrameId);
                        this._animationFrameId = null;
                    }
                    return;
                }

                this.geodesicObjects.forEach(objects => {
                    objects.forEach(obj => {
                        if (obj.mesh && obj.mesh.material.uniforms) {
                            obj.mesh.material.uniforms.cameraPos.value.copy(camera.position);
                        }
                    });
                });

                this._animationFrameId = requestAnimationFrame(animate);
            };
            animate();
        }

        // Clean up geometries
        Object.values(geometries).forEach(geometry => geometry.dispose());
    },

    updateGasClouds(camera) {
        if (!showGasClouds) return;

        const bubbleCenter = new THREE.Vector3(0, 0, 0);
        const currentZoomLevel = camera.position.distanceTo(bubbleCenter);
        
        // Simple threshold-based visibility
        const shouldHideNearCenter = currentZoomLevel < 300;
        
        gasCloudMeshes.forEach(mesh => {
            if (!mesh) return;  // Skip if mesh is invalid
            
            const distToCenter = mesh.position.distanceTo(bubbleCenter);
            const isNearCenter = distToCenter <= 100;
            
            // Simple visibility rule: hide if we're zoomed in AND near center
            mesh.visible = !(shouldHideNearCenter && isNearCenter);
        });
    },

    powerGlow(systems, camera) {
        if (!systems || !this.systemsInitialized) return;
        
        // Clear existing power glow objects
        this.powerGlowObjects.forEach(instance => {
            if (instance) {
                this.scene.remove(instance);
                if (instance.geometry) instance.geometry.dispose();
                if (instance.material) instance.material.dispose();
            }
        });
        this.powerGlowObjects.clear();
        
        // Return early if power glow is disabled
        if (!this.showPowerGlow) return;

        // Create radial gradient texture
        const canvas = document.createElement('canvas');
        canvas.width = 64;
        canvas.height = 64;
        const ctx = canvas.getContext('2d');
        const gradient = ctx.createRadialGradient(32, 32, 0, 32, 32, 32);
        gradient.addColorStop(0, 'rgba(255, 255, 255, 0.3)');
        gradient.addColorStop(0.5, 'rgba(255, 255, 255, 0.1)');
        gradient.addColorStop(1, 'rgba(255, 255, 255, 0)');
        ctx.fillStyle = gradient;
        ctx.fillRect(0, 0, 64, 64);
        const texture = new THREE.CanvasTexture(canvas);

        const geometry = new THREE.CircleGeometry(0.8, 16);
        const powerCounts = {};
        Object.values(systems).forEach(system => {
            if (!system.controlling_power || !system.power_state || system.instanceId === undefined) return;
            powerCounts[system.controlling_power] = Math.max(
                (powerCounts[system.controlling_power] || 0),
                system.instanceId + 1
            );
        });

        const cameraQuaternion = camera.quaternion.clone();
        const rotationQuaternion = new THREE.Quaternion();
        rotationQuaternion.setFromAxisAngle(new THREE.Vector3(1, 0, 0), -Math.PI / 2);
        cameraQuaternion.multiply(rotationQuaternion);

        Object.entries(powerCounts).forEach(([power, count]) => {
            const color = window.powerColors[power];
            if (!color) return;

            const material = new THREE.MeshBasicMaterial({ 
                color: new THREE.Color(color),
                transparent: true,
                depthTest: false,
                side: THREE.DoubleSide,
                map: texture,
                opacity: 0.15,
                blending: THREE.AdditiveBlending
            });

            const instance = new THREE.InstancedMesh(geometry, material, count);
            instance.renderOrder = 2;

            const defaultMatrix = new THREE.Matrix4();
            defaultMatrix.makeScale(0.00001, 0.00001, 0.00001);
            defaultMatrix.setPosition(new THREE.Vector3(100000, 100000, 100000));

            for (let i = 0; i < count; i++) {
                instance.setMatrixAt(i, defaultMatrix);
            }

            this.scene.add(instance);
            this.powerGlowObjects.set(power, instance);

            const matrix = new THREE.Matrix4();
            const scale = new THREE.Vector3();
            const position = new THREE.Vector3();

            let positionedCount = 0;
            Object.values(systems).forEach(system => {
                if (system.controlling_power !== power || !system.power_state || system.instanceId === undefined) return;

                let baseRadius;
                switch (system.power_state.toLowerCase()) {
                    case 'fortified': baseRadius = 9; break;
                    case 'exploited': baseRadius = 6; break;
                    case 'stronghold': baseRadius = 12; break;
                    default: return;
                }

                scale.set(baseRadius, baseRadius, baseRadius);
                position.set(system.x, system.y, system.z);

                if (positionedCount < 5) {
                    /*
                    console.log(`Setting position for ${power} system ${system.name}:`, {
                        x: system.x,
                        y: system.y,
                        z: system.z,
                        instanceId: system.instanceId,
                        baseRadius
                    });*/

                    const matrixElements = matrix.elements;
                    matrix.compose(position, cameraQuaternion, scale);

                    const pos = new THREE.Vector3();
                    matrix.decompose(pos, new THREE.Quaternion(), new THREE.Vector3());
                }

                matrix.compose(position, cameraQuaternion, scale);
                instance.setMatrixAt(system.instanceId, matrix);
                positionedCount++;
            });

            instance.instanceMatrix.needsUpdate = true;
            instance.userData.systems = Object.values(systems).filter(
                system => system.controlling_power === power && system.power_state && system.instanceId !== undefined
            );
        });
    },

    updatePowerGlow(camera) {
        // Early returns if required objects aren't available
        if (!this.showPowerGlow || !camera || !this.targetPoint) return;
        
        // Get zoom level based on distance to target point
        const zoomLevel = camera.position.distanceTo(this.targetPoint);
        
        // Calculate visibility radius based on zoom level
        let visibilityRadius;
        if (zoomLevel >= 150) {
            visibilityRadius = 200;  // Reduced from 1000 to be more reasonable
        } else if (zoomLevel <= 20) {
            visibilityRadius = 20;   // Increased minimum to match MIN_ZOOM_LEVEL
        } else {
            // Smooth transition between min and max radius
            const t = (zoomLevel - 20) / (150 - 20);
            visibilityRadius = 20 + t * (200 - 20);
        }
        
        const cameraQuaternion = camera.quaternion.clone();
        const rotationQuaternion = new THREE.Quaternion();
        rotationQuaternion.setFromAxisAngle(new THREE.Vector3(1, 0, 0), -Math.PI);
        cameraQuaternion.multiply(rotationQuaternion);

        const hideMatrix = new THREE.Matrix4();
        hideMatrix.makeScale(0.00001, 0.00001, 0.00001);
        hideMatrix.setPosition(new THREE.Vector3(100000, 100000, 100000));
        
        const matrix = new THREE.Matrix4();
        const scale = new THREE.Vector3();
        const position = new THREE.Vector3();
        
        this.powerGlowObjects.forEach((instance) => {
            if (!instance?.userData?.systems) return;
            
            instance.userData.systems.forEach((system) => {
                if (system.instanceId === undefined) return;
                
                // Calculate distance from system to target point
                const distanceToTarget = new THREE.Vector3(system.x, system.y, system.z)
                    .distanceTo(this.targetPoint);
                
                // Only show systems within the visibility radius of the target point
                if (distanceToTarget <= visibilityRadius) {
                    let baseRadius;
                    switch (system.power_state?.toLowerCase()) {
                        case 'fortified': baseRadius = 9; break;
                        case 'exploited': baseRadius = 6; break;
                        case 'stronghold': baseRadius = 12; break;
                        default: return;
                    }

                    // Only adjust scale below zoom level 140
                    if (zoomLevel <= 140) {
                        const zoomFactor = Math.max(0.6, zoomLevel / 140);
                        baseRadius *= zoomFactor;
                        
                        // Smoothly transition opacity from 0.1 to 0.7 as we zoom in
                        const opacityFactor = 1 - (zoomLevel / 140); // 0 at zoomLevel 140, 1 at zoomLevel 0
                        instance.material.opacity = 0.1 + (opacityFactor * 0.3); // Transition from 0.1 to 0.7
                    } else {
                        instance.material.opacity = 0.1; // Default opacity when zoomed out
                    }                    

                    scale.set(baseRadius, baseRadius, baseRadius);
                    position.set(system.x, system.y, system.z);
                    matrix.compose(position, cameraQuaternion, scale);
                    instance.setMatrixAt(system.instanceId, matrix);
                } else {
                    // Hide systems outside visibility radius
                    instance.setMatrixAt(system.instanceId, hideMatrix);
                }
            });
            
            instance.instanceMatrix.needsUpdate = true;
        });
    }
};

// Constants for gas cloud visualization
const GAS_CLOUD_CONFIG = {
    regionSize: 5000,        // Size of square region in ly
    yRange: 500,            // Total Y range (±200 from center)
    numClouds: 3000,         // Number of cloud points
    minCloudSize: 70,       // Minimum cloud size in ly
    maxCloudSize: 150,      // Maximum cloud size in ly
    colors: [
        '#5a403a',  // Lightened from rgb(75, 67, 64)
        '#4e3c32',  // Lightened from rgb(65, 54, 47)
        '#3e3027',  // Lightened from rgb(53, 46, 41)
        '#6c5249',  // Lightened from rgb(99, 90, 87)
        '#58402f'   // Lightened from rgb(78, 62, 53)
    ],
    galacticCore: new THREE.Vector3(5000, 0, 0)  // Position of Sagittarius A* in positive X (north)
};

function createLightDirectionArrow(scene, THREE) {
    // Remove existing arrow if any
    if (lightDirectionArrow) {
        scene.remove(lightDirectionArrow);
    }

    // Create arrow pointing in light direction (north = positive X)
    const arrowDir = new THREE.Vector3(0, 0, 1);
    const origin = new THREE.Vector3(0, 0, 0);
    const length = 1000;
    const headLength = 200;  // 20% of length
    const headWidth = 100;   // 10% of length
    const color = 0xff0000;  // Red

    lightDirectionArrow = new THREE.ArrowHelper(
        arrowDir,
        origin,
        length,
        color,
        headLength,
        headWidth
    );
    scene.add(lightDirectionArrow);
}

function createGasClouds(scene, THREE) {
    // Clear existing clouds
    gasCloudMeshes.forEach(mesh => scene.remove(mesh));
    gasCloudMeshes = [];

    // Create light direction arrow
    //createLightDirectionArrow(scene, THREE);

    const halfRegion = GAS_CLOUD_CONFIG.regionSize / 2;
    const halfYRange = GAS_CLOUD_CONFIG.yRange / 2;

    // Load textures
    const textureLoader = new THREE.TextureLoader();
    const perlinTextures = [
        textureLoader.load('img/noise/Perlin 7 - 256x256.png'),
        textureLoader.load('img/noise/Perlin 18 - 256x256.png'),
        textureLoader.load('img/noise/Perlin 23 - 256x256.png'),
        textureLoader.load('img/noise/Grainy 1 - 256x256.jpg')
    ];
    const grainyTextures = [
        textureLoader.load('img/noise/Grainy 2 - 256x256.png'),
        textureLoader.load('img/noise/Grainy 9 - 256x256.png'),
        textureLoader.load('img/noise/Grainy 10 - 256x256.png')
    ];
    const turbulenceTexture = textureLoader.load('img/noise/Turbulence 2 - 256x256.png');
    const finalNoiseTexture = textureLoader.load('img/noise/final-noise.png');

    // Set texture properties
    [...perlinTextures, ...grainyTextures, turbulenceTexture, finalNoiseTexture].forEach(texture => {
        texture.wrapS = texture.wrapT = THREE.RepeatWrapping;
    });

    // Create shared geometry
    const geometry = new THREE.SphereGeometry(1, 32, 32);

    // Fixed light direction from galactic core (positive X = north)
    const galacticCoreDirection = new THREE.Vector3(0, 0, 1).normalize();

    // Create clouds
    for (let i = 0; i < GAS_CLOUD_CONFIG.numClouds; i++) {
        const x = Math.random() * GAS_CLOUD_CONFIG.regionSize - halfRegion;
        const z = Math.random() * GAS_CLOUD_CONFIG.regionSize - halfRegion;
        const y = Math.random() * GAS_CLOUD_CONFIG.yRange - halfYRange;

        const yFactor = 1 - Math.abs(y) / halfYRange;
        const sizeRange = GAS_CLOUD_CONFIG.maxCloudSize - GAS_CLOUD_CONFIG.minCloudSize;
        const size = GAS_CLOUD_CONFIG.minCloudSize + (sizeRange * yFactor * Math.random());

        const cloudColor = new THREE.Color(GAS_CLOUD_CONFIG.colors[Math.floor(Math.random() * GAS_CLOUD_CONFIG.colors.length)]);
        
        // Select random textures for variation
        const perlinTex = perlinTextures[Math.floor(Math.random() * perlinTextures.length)];
        const grainyTex = grainyTextures[Math.floor(Math.random() * grainyTextures.length)];
        
        const material = new THREE.ShaderMaterial({
            uniforms: {
                color: { value: cloudColor },
                lightDir: { value: galacticCoreDirection },
                perlinTexture: { value: perlinTex },
                grainyTexture: { value: grainyTex },
                turbulenceTexture: { value: turbulenceTexture },
                finalNoiseTexture: { value: finalNoiseTexture },
                time: { value: Math.random() * 1000 },
                uvScale1: { value: new THREE.Vector2(3 + Math.random() * 2, 3 + Math.random() * 2) },
                uvScale2: { value: new THREE.Vector2(4 + Math.random() * 2, 4 + Math.random() * 2) },
                uvScale3: { value: new THREE.Vector2(2 + Math.random() * 1.5, 2 + Math.random() * 1.5) },
                uvOffset: { value: new THREE.Vector2(Math.random() * 20.0 - 10.0, Math.random() * 20.0 - 10.0) }  // Larger random offset per cloud
            },
            vertexShader: `
                uniform vec3 lightDir;
                uniform vec2 uvScale1;
                uniform vec2 uvScale2;
                uniform vec2 uvScale3;
                
                varying vec3 vNormal;
                varying vec3 vViewDir;
                varying vec3 vLightDir;
                varying vec2 vUv;
                varying vec3 vPosition;
                
                void main() {
                    vPosition = position;
                    vNormal = normalize(normalMatrix * normal);
                    vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
                    vViewDir = normalize(-mvPosition.xyz);
                    vLightDir = normalize(normalMatrix * lightDir);
                    
                    // Project view-space normal for camera-aligned noise
                    vec3 viewNormal = normalize((modelViewMatrix * vec4(normal, 0.0)).xyz);
                    vUv = viewNormal.xy * 0.5 + 0.5;
                    
                    gl_Position = projectionMatrix * mvPosition;
                }
            `,
            fragmentShader: `
                uniform vec3 color;
                uniform sampler2D perlinTexture;
                uniform sampler2D grainyTexture;
                uniform sampler2D turbulenceTexture;
                uniform sampler2D finalNoiseTexture;
                uniform float time;
                uniform vec2 uvScale1;
                uniform vec2 uvScale2;
                uniform vec2 uvScale3;
                uniform vec2 uvOffset;
                
                varying vec3 vNormal;
                varying vec3 vViewDir;
                varying vec3 vLightDir;
                varying vec2 vUv;
                varying vec3 vPosition;
                
                void main() {
                    vec3 normal = normalize(vNormal);
                    vec3 viewDir = normalize(vViewDir);
                    vec3 lightDir = normalize(vLightDir);
                    
                    // Calculate basic sphere alpha for perfect circle
                    float viewDot = abs(dot(normal, viewDir));
                    float sphereAlpha = pow(viewDot, 2.0);
                    
                    // Sample noise textures at multiple scales for more detail
                    vec2 noiseUv1 = vUv * uvScale1;
                    vec2 noiseUv2 = vUv * uvScale2;
                    vec2 turbUv = vUv * uvScale3;
                    
                    vec4 perlin1 = texture2D(perlinTexture, noiseUv1);
                    vec4 perlin2 = texture2D(perlinTexture, noiseUv1 * 3.5);
                    vec4 perlin3 = texture2D(perlinTexture, noiseUv1 * 5.0);
                    
                    vec4 grain1 = texture2D(grainyTexture, noiseUv2);
                    vec4 grain2 = texture2D(grainyTexture, noiseUv2 * 3.0);
                    vec4 grain3 = texture2D(grainyTexture, noiseUv2 * 4.5);
                    
                    // Sample detail and turbulence textures
                    vec4 detail = texture2D(grainyTexture, noiseUv2 * 6.0);
                    vec4 turbulence = texture2D(turbulenceTexture, turbUv);
                    vec4 turbDetail = texture2D(turbulenceTexture, turbUv * 3.0);
                    
                    // Create multi-layered noise with more dramatic mixing
                    float perlinNoise = perlin1.r * 0.5 + perlin2.r * 0.3 + perlin3.r * 0.2;
                    float grainNoise = grain1.r * 0.5 + grain2.r * 0.3 + grain3.r * 0.2;
                    
                    // Combine noises with more contrast
                    float combinedNoise = mix(perlinNoise, grainNoise, 0.5);
                    combinedNoise = pow(combinedNoise, 1.5);
                    
                    // Create turbulent shape variation
                    float turbulentShape = mix(turbulence.r, turbDetail.r, 0.5);
                    turbulentShape = pow(turbulentShape, 1.2);
                    
                    // Use detail texture for aggressive alpha variation
                    float detailNoise = smoothstep(0.3, 0.7, detail.r);
                    
                    // Calculate light contribution
                    float lightDot = dot(normal, lightDir);
                    float wrap = 0.5;
                    float scatter = smoothstep(-wrap, 1.0, lightDot);
                    float backLight = smoothstep(-1.0, -0.2, lightDot) * 0.6;
                    
                    // Combine lighting
                    float light = scatter + backLight;
                    light = pow(light, 0.5);
                    
                    // Add fresnel rim
                    float fresnel = pow(1.0 - abs(dot(normal, viewDir)), 3.0);
                    
                    // Color variation based on detail and turbulence
                    vec3 baseColor = color;
                    float colorVar = (detail.r - 0.5) * 0.4 + (turbulentShape - 0.5) * 0.2;
                    baseColor = baseColor * (1.0 + colorVar);
                    
                    // Final color with lighting
                    vec3 finalColor = baseColor * (1.0 + light * 2.0 + fresnel * 0.5);
                    finalColor += baseColor * vec3(1.0, 0.4, 0.2) * backLight * 0.5;
                    
                    // Combine all effects for final alpha
                    float noiseAlpha = smoothstep(0.1, 0.9, combinedNoise);
                    noiseAlpha = pow(noiseAlpha, 1.2);
                    
                    // Apply turbulent shape variation to alpha
                    float finalShape = mix(noiseAlpha, turbulentShape, 0.4);
                    float alpha = sphereAlpha * mix(0.2, 1.0, finalShape) * 0.7;
                    
                    // Apply detail variations
                    alpha *= mix(0.7, 1.0, detailNoise);
                    alpha *= mix(0.8, 1.2, turbulentShape); // Additional turbulent variation
                    
                    // Apply final 2D alpha variation with Grainy 1
                    vec2 finalFormUv = vUv * (uvScale3 * 0.15);  // Large scale variation
                    vec4 finalForm = texture2D(perlinTexture, finalFormUv);  // Using Grainy 1 from perlinTextures[3]
                    alpha *= mix(0.7, 1.3, finalForm.r);  // Strong variation for cloud-like edges
                    
                    // Apply second final form at different scale for more variation
                    vec2 finalFormUv2 = vUv * (uvScale3 * 0.25);
                    vec4 finalForm2 = texture2D(perlinTexture, finalFormUv2);
                    alpha *= mix(0.8, 1.2, finalForm2.r);
                    
                    // Apply final noise cutout in 3D space
                    vec3 noisePos = vPosition * 0.003;  // Scale for 3D space
                    vec2 finalNoiseUv = vec2(
                        dot(noisePos.yz, vec2(1.0)) + uvOffset.x,
                        dot(noisePos.xz, vec2(1.0)) + uvOffset.y
                    ) * 0.3;  // Scale the final UV
                    vec4 finalNoise = texture2D(finalNoiseTexture, finalNoiseUv);
                    
                    // More subtle cutout effect
                    float cutoutStrength = smoothstep(0.35, 0.65, finalNoise.r);  // Softer transition
                    alpha *= mix(0.6, 1.1, cutoutStrength);  // Less extreme range
                    
                    // Reduce overall alpha by 10%
                    alpha *= 0.5;
                    
                    gl_FragColor = vec4(finalColor, alpha);
                }
            `,
            transparent: true,
            depthWrite: false,
            depthTest: true,
            side: THREE.FrontSide,
            blending: THREE.NormalBlending
        });

        const mesh = new THREE.Mesh(geometry, material);
        mesh.position.set(x, y, z);
        mesh.scale.set(size, size, size);
        mesh.renderOrder = 1;
        
        gasCloudMeshes.push(mesh);
        scene.add(mesh);
    }
}

// Export functions for external use
export function initGasClouds(scene, THREE) {
    if (showGasClouds) {
        createGasClouds(scene, THREE);
    }
}

export function toggleGasClouds(scene, show) {
    showGasClouds = show;
    gasCloudMeshes.forEach(mesh => {
        mesh.visible = show;
    });
}

