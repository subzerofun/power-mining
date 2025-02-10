// Power visualization module for Elite Dangerous map
import * as THREE from '../threejs/three.module.js';
import { MarchingCubes } from '../threejs/jsm/objects/MarchingCubes.js';
import { PowerVisualizerExtra } from './visualizeExtra.js';

export const PowerVisualizer = {
    // State
    powerLineObjects: new Map(),
    powerVolumeObjects: {},
    powerRegionObjects: new Map(),
    organicPowerLineObjects: new Map(),
    powerTerritoryObjects: new Map(),
    powerMeshObjects: [],
    geodesicObjects: new Map(),  // Shared with PowerVisualizerExtra
    showPowerLines: false,
    showPowerVolumes: false,
    showPowerRegions: false,
    showOrganicPowerLines: false,
    showPowerTerritories: false,
    showPowerTerritoryMesh: false,
    _showGeodesicTerritories: false,  // Private state
    targetPoint: null,  // Add targetPoint property

    // Constants
    INFLUENCE_RADII: {
        'Stronghold': 30,
        'Stronghold-Carrier': 30,
        'Fortified': 20,
        'Exploited': 0
    },

    POWER_LINE_THICKNESS: {
        'Stronghold': 0.25,
        'Fortified': 0.125,
        'Exploited': 0.075
    },

    POWER_STATE_INFLUENCE: {
        'Stronghold': 2.0,
        'Fortified': 0.6,
        'Exploited': 0.3
    },

    POWER_LINE_DISTANCES: {
        'Stronghold': 30,
        'Stronghold-Carrier': 30,
        'Fortified': 20,
        'Exploited': 10
    },

    // Add new property for gradient objects
    gradientObjects: new Map(),

    scene: null,
    THREE: null,
    ConvexGeometry: null,
    powerObjects: new Map(),

    init(scene, THREE, ConvexGeometry) {
        this.scene = scene;
        this.THREE = THREE;
        this.ConvexGeometry = ConvexGeometry;
        this._showGeodesicTerritories = false;  // Initialize private state
        this.targetPoint = window.targetPoint;  // Get targetPoint from window
        
        // Initialize extra visualizer
        PowerVisualizerExtra.init(this);
        
        // Create reusable geometries and materials
        this.powerLineGeometry = new THREE.CylinderGeometry(1, 1, 1, 8);
        this.powerLineMaterial = new THREE.MeshBasicMaterial({
            transparent: true,
            opacity: 0.1,
            depthWrite: false
        });
    },

    // Power Lines
    createPowerLines(systems) {
        this.clearObjects(this.powerLineObjects);
        if (!this.showPowerLines) return;

        const lineCounts = new Map();
        const lineData = new Map();
        
        Object.values(systems).forEach(system1 => {
            if (!system1.controlling_power || !system1.power_state) return;
            
            const radius = this.INFLUENCE_RADII[system1.power_state];
            const power = system1.controlling_power;
            
            if (!lineCounts.has(power)) {
                lineCounts.set(power, 0);
                lineData.set(power, []);
            }

            Object.values(systems).forEach(system2 => {
                if (system1 === system2) return;
                
                const distance = Math.sqrt(
                    Math.pow(system1.x - system2.x, 2) +
                    Math.pow(system1.y - system2.y, 2) +
                    Math.pow(system1.z - system2.z, 2)
                );

                if (distance <= radius) {
                    lineCounts.set(power, lineCounts.get(power) + 1);
                    lineData.get(power).push({
                        start: system1,
                        end: system2,
                        distance: distance,
                        thickness: this.POWER_LINE_THICKNESS[system1.power_state]
                    });
                }
            });
        });

        lineData.forEach((lines, power) => {
            if (lines.length === 0) return;

            const instancedMesh = new THREE.InstancedMesh(
                this.powerLineGeometry,
                this.powerLineMaterial.clone(),
                lines.length
            );
            instancedMesh.material.color.setHex(window.powerColors[power] || 0xFFFFFF);
            instancedMesh.renderOrder = 1;

            const matrix = new THREE.Matrix4();
            const position = new THREE.Vector3();
            const quaternion = new THREE.Quaternion();
            const scale = new THREE.Vector3();

            lines.forEach((line, index) => {
                position.set(
                    (line.start.x + line.end.x) / 2,
                    (line.start.y + line.end.y) / 2,
                    (line.start.z + line.end.z) / 2
                );

                const direction = new THREE.Vector3(
                    line.end.x - line.start.x,
                    line.end.y - line.start.y,
                    line.end.z - line.start.z
                ).normalize();

                const up = new THREE.Vector3(0, 1, 0);
                quaternion.setFromUnitVectors(up, direction);

                scale.set(line.thickness, line.distance, line.thickness);

                matrix.compose(position, quaternion, scale);
                instancedMesh.setMatrixAt(index, matrix);
            });

            instancedMesh.instanceMatrix.needsUpdate = true;
            this.scene.add(instancedMesh);
            this.powerLineObjects.set(power, instancedMesh);
        });
    },

    // Power Volumes
    createPowerVolumes(systems, visibleStarsRadius, camera) {
        this.clearObjects(this.powerVolumeObjects);
        if (!this.showPowerVolumes) return;

        const powerSystems = {};
        Object.values(systems).forEach(system => {
            if (system.controlling_power) {
                if (!powerSystems[system.controlling_power]) {
                    powerSystems[system.controlling_power] = [];
                }
                powerSystems[system.controlling_power].push({
                    x: system.x,
                    y: system.y,
                    z: system.z
                });
            }
        });

        Object.entries(powerSystems).forEach(([power, systemList]) => {
            const powerColor = window.powerColors[power] || 0xFFFFFF;
            const powerColorBright = powerColor.clone().multiplyScalar(1.5);
            const meshes = [];
            this.powerVolumeObjects[power] = meshes;

            const visibleSystems = systemList.filter(system => {
                const distance = camera.position.distanceTo(new THREE.Vector3(system.x, system.y, system.z));
                return distance <= visibleStarsRadius;
            });

            visibleSystems.forEach(system => {
                const geometry = new THREE.SphereGeometry(this.INFLUENCE_RADII['Exploited'] / 4, 8, 8);
                const material = new THREE.MeshBasicMaterial({
                    color: powerColor,
                    transparent: true,
                    opacity: 0.01,
                    depthWrite: false
                });
                const sphere = new THREE.Mesh(geometry, material);
                sphere.position.set(system.x, system.y, system.z);
                sphere.renderOrder = 1;
                this.scene.add(sphere);
                meshes.push(sphere);
            });
        });
    },

    // Power Regions
    createPowerRegions(systems) {
        this.clearObjects(this.powerRegionObjects);
        if (!this.showPowerRegions) return;

        const powerSystems = {};
        Object.values(systems).forEach(system => {
            if (system.controlling_power) {
                if (!powerSystems[system.controlling_power]) {
                    powerSystems[system.controlling_power] = [];
                }
                powerSystems[system.controlling_power].push(
                    new THREE.Vector3(system.x, system.y, system.z)
                );
            }
        });

        Object.entries(powerSystems).forEach(([power, points]) => {
            if (points.length < 4) return;

            const powerColor = window.powerColors[power] || 0xFFFFFF;
            const meshes = [];
            this.powerRegionObjects.set(power, meshes);

            try {
                const geometry = new this.ConvexGeometry(points);
                
                const volumeMaterial = new THREE.MeshBasicMaterial({
                    color: powerColor,
                    transparent: true,
                    opacity: 0.05,
                    depthWrite: false,
                    side: THREE.DoubleSide
                });
                
                const wireframeMaterial = new THREE.LineBasicMaterial({
                    color: powerColor,
                    transparent: true,
                    opacity: 0.1,
                    depthWrite: false
                });

                const volumeMesh = new THREE.Mesh(geometry, volumeMaterial);
                const wireframe = new THREE.LineSegments(
                    new THREE.WireframeGeometry(geometry),
                    wireframeMaterial
                );

                volumeMesh.renderOrder = 1;
                wireframe.renderOrder = 2;

                this.scene.add(volumeMesh);
                this.scene.add(wireframe);
                meshes.push(volumeMesh, wireframe);
            } catch (e) {
                console.warn(`Could not create convex hull for power ${power}:`, e);
            }
        });
    },

    // Organic Power Lines
    createOrganicPowerLines(systems) {
        this.clearObjects(this.organicPowerLineObjects);
        if (!this.showOrganicPowerLines) return;

        const powerSystems = new Map();
        Object.values(systems).forEach(system => {
            if (!system.controlling_power || !system.power_state) return;
            
            if (!powerSystems.has(system.controlling_power)) {
                powerSystems.set(system.controlling_power, []);
            }
            powerSystems.get(system.controlling_power).push(system);
        });

        powerSystems.forEach((systemList, power) => {
            const connectionStrengths = new Map();
            const lineData = [];
            
            systemList.forEach(startSystem => {
                const startInfluence = this.POWER_STATE_INFLUENCE[startSystem.power_state] || 0;
                const maxDistance = this.INFLUENCE_RADII[startSystem.power_state] || 20;
                
                systemList.forEach(endSystem => {
                    if (startSystem === endSystem) return;
                    
                    const distance = Math.sqrt(
                        Math.pow(startSystem.x - endSystem.x, 2) +
                        Math.pow(startSystem.y - endSystem.y, 2) +
                        Math.pow(startSystem.z - endSystem.z, 2)
                    );

                    if (distance <= maxDistance) {
                        const endInfluence = this.POWER_STATE_INFLUENCE[endSystem.power_state] || 0;
                        const connectionKey = [
                            Math.min(startSystem.x, endSystem.x),
                            Math.min(startSystem.y, endSystem.y),
                            Math.min(startSystem.z, endSystem.z),
                            Math.max(startSystem.x, endSystem.x),
                            Math.max(startSystem.y, endSystem.y),
                            Math.max(startSystem.z, endSystem.z)
                        ].join(',');

                        const influence = (startInfluence + endInfluence) * (1 - distance / maxDistance);
                        connectionStrengths.set(
                            connectionKey, 
                            (connectionStrengths.get(connectionKey) || 0) + influence
                        );

                        lineData.push({
                            start: startSystem,
                            end: endSystem,
                            distance: distance,
                            key: connectionKey
                        });
                    }
                });
            });

            const maxStrength = Math.max(...connectionStrengths.values());

            if (lineData.length > 0) {
                const instancedMesh = new THREE.InstancedMesh(
                    this.powerLineGeometry,
                    this.powerLineMaterial.clone(),
                    lineData.length
                );
                instancedMesh.material.color.setHex(window.powerColors[power] || 0xFFFFFF);
                instancedMesh.renderOrder = 1;

                const matrix = new THREE.Matrix4();
                const position = new THREE.Vector3();
                const quaternion = new THREE.Quaternion();
                const scale = new THREE.Vector3();

                lineData.forEach((line, index) => {
                    const strength = connectionStrengths.get(line.key) / maxStrength;
                    const thickness = THREE.MathUtils.lerp(0.05, 0.05, strength);

                    position.set(
                        (line.start.x + line.end.x) / 2,
                        (line.start.y + line.end.y) / 2,
                        (line.start.z + line.end.z) / 2
                    );

                    const direction = new THREE.Vector3(
                        line.end.x - line.start.x,
                        line.end.y - line.start.y,
                        line.end.z - line.start.z
                    ).normalize();

                    const up = new THREE.Vector3(0, 1, 0);
                    quaternion.setFromUnitVectors(up, direction);

                    scale.set(thickness, line.distance, thickness);

                    matrix.compose(position, quaternion, scale);
                    instancedMesh.setMatrixAt(index, matrix);
                });

                instancedMesh.instanceMatrix.needsUpdate = true;
                this.scene.add(instancedMesh);
                this.organicPowerLineObjects.set(power, instancedMesh);
            }
        });
    },

    // Helper function to generate points around a system based on its influence radius
    generateInfluencePoints(system, radius) {
        const points = [];
        const pos = new THREE.Vector3(system.x, system.y, system.z);
        
        // Add center point
        points.push(pos.clone());
        
        // Add 6 points at the extremes of each axis at the influence radius
        points.push(new THREE.Vector3(pos.x + radius, pos.y, pos.z));
        points.push(new THREE.Vector3(pos.x - radius, pos.y, pos.z));
        points.push(new THREE.Vector3(pos.x, pos.y + radius, pos.z));
        points.push(new THREE.Vector3(pos.x, pos.y - radius, pos.z));
        points.push(new THREE.Vector3(pos.x, pos.y, pos.z + radius));
        points.push(new THREE.Vector3(pos.x, pos.y, pos.z - radius));
        
        // Add 8 points at the corners of a cube for better volume
        const r = radius * 0.707; // radius / sqrt(2) for corner points
        points.push(new THREE.Vector3(pos.x + r, pos.y + r, pos.z + r));
        points.push(new THREE.Vector3(pos.x + r, pos.y + r, pos.z - r));
        points.push(new THREE.Vector3(pos.x + r, pos.y - r, pos.z + r));
        points.push(new THREE.Vector3(pos.x + r, pos.y - r, pos.z - r));
        points.push(new THREE.Vector3(pos.x - r, pos.y + r, pos.z + r));
        points.push(new THREE.Vector3(pos.x - r, pos.y + r, pos.z - r));
        points.push(new THREE.Vector3(pos.x - r, pos.y - r, pos.z + r));
        points.push(new THREE.Vector3(pos.x - r, pos.y - r, pos.z - r));
        
        return points;
    },

    // Power Territories with influence radii
    createPowerTerritoryMesh(systems, scene, powerColors) {
        // Clean up existing meshes first
        this.cleanupPowerMesh();

        if (!this.showPowerTerritoryMesh) return;

        if (!this.powerMeshObjects) {
            this.powerMeshObjects = [];
        }

        const powerSystems = {};
        
        // Group systems by power
        Object.values(systems).forEach(system => {
            if (system.controlling_power && system.controlling_power !== "None") {
                if (!powerSystems[system.controlling_power]) {
                    powerSystems[system.controlling_power] = [];
                }
                powerSystems[system.controlling_power].push(system);
            }
        });

        // Find the bounds of all systems
        let minX = Infinity, minY = Infinity, minZ = Infinity;
        let maxX = -Infinity, maxY = -Infinity, maxZ = -Infinity;
        Object.values(systems).forEach(system => {
            minX = Math.min(minX, system.x);
            minY = Math.min(minY, system.y);
            minZ = Math.min(minZ, system.z);
            maxX = Math.max(maxX, system.x);
            maxY = Math.max(maxY, system.y);
            maxZ = Math.max(maxZ, system.z);
        });

        const width = maxX - minX;
        const height = maxY - minY;
        const depth = maxZ - minZ;
        const centerX = (maxX + minX) / 2;
        const centerY = (maxY + minY) / 2;
        const centerZ = (maxZ + minZ) / 2;

        const SCALE_FACTOR = 0.5;
        const clock = new THREE.Clock();

        // Create marching cubes effect for each power
        Object.entries(powerSystems).forEach(([power, systemList]) => {
            const resolution = 128;
            const powerColor = new THREE.Color(powerColors[power] || 0xffffff);
            const powerColorBright = powerColor.clone().multiplyScalar(1.5);
            
            // Base material for territory with normal blending
            const material = new THREE.MeshPhysicalMaterial({
                color: powerColor,
                emissive: powerColor,
                emissiveIntensity: 0.2,
                transparent: true,
                opacity: 0.35,
                metalness: 0.1,
                roughness: 0.6,
                ior: 1.2,
                side: THREE.FrontSide,
                depthWrite: true,
                depthTest: true,
                blending: THREE.NormalBlending,
                renderOrder: 0,
                fog: false
            });

            // Create edge highlight material for volume boundaries
            const glowMaterial = new THREE.MeshPhysicalMaterial({
                color: powerColorBright,
                emissive: powerColorBright,
                emissiveIntensity: 1.8,
                transparent: true,
                opacity: 0.25,
                metalness: 0.0,
                roughness: 0.4,
                side: THREE.BackSide,
                depthWrite: false,
                depthTest: true,
                blending: THREE.NormalBlending,
                renderOrder: 1,
                fog: false
            });

            material.onBeforeRender = function(renderer, scene, camera, geometry, object) {
                const viewVector = camera.position.clone().sub(object.position).normalize();
                const normal = new THREE.Vector3(0, 1, 0);
                const fresnelFactor = Math.pow(1.0 - Math.abs(viewVector.dot(normal)), 0.8);
                
                this.opacity = 0.3 + fresnelFactor * 0.15;
                this.emissiveIntensity = 0.2 + fresnelFactor * 0.2;
                
                const distance = camera.position.distanceTo(object.position);
                if (scene.fog) {
                    const fogFactor = 1.0 - Math.min(1, distance / scene.fog.far);
                    this.opacity *= fogFactor;
                }
            };

            glowMaterial.onBeforeRender = function(renderer, scene, camera, geometry, object) {
                const viewVector = camera.position.clone().sub(object.position).normalize();
                const fresnelFactor = Math.pow(1.0 - Math.abs(viewVector.y), 0.6);
                
                this.opacity = 0.2 + fresnelFactor * 0.3;
                this.emissiveIntensity = 1.8 + fresnelFactor * 1.2;
                
                const distance = camera.position.distanceTo(object.position);
                if (scene.fog) {
                    const fogFactor = 1.0 - Math.min(1, distance / scene.fog.far);
                    this.opacity *= fogFactor;
                }
            };

            // Create a second glow material for inner volume
            const innerGlowMaterial = glowMaterial.clone();
            innerGlowMaterial.side = THREE.FrontSide;
            innerGlowMaterial.opacity = 0.15;
            innerGlowMaterial.emissiveIntensity = 1.2;
            innerGlowMaterial.renderOrder = 2;

            innerGlowMaterial.onBeforeRender = function(renderer, scene, camera, geometry, object) {
                const viewVector = camera.position.clone().sub(object.position).normalize();
                const fresnelFactor = Math.pow(1.0 - Math.abs(viewVector.y), 0.6);
                
                this.opacity = 0.12 + fresnelFactor * 0.2;
                this.emissiveIntensity = 1.2 + fresnelFactor * 0.8;
                
                const distance = camera.position.distanceTo(object.position);
                if (scene.fog) {
                    const fogFactor = 1.0 - Math.min(1, distance / scene.fog.far);
                    this.opacity *= fogFactor;
                }
            };

            // Create base volume effect
            const effect = new MarchingCubes(resolution, material, true, true, 100000);
            effect.position.set(centerX, centerY, centerZ);
            effect.scale.set(width * SCALE_FACTOR, height * SCALE_FACTOR, depth * SCALE_FACTOR);

            // Create outer glow effect (thinner but more intense)
            const outerGlowEffect = new MarchingCubes(resolution, glowMaterial, true, true, 100000);
            outerGlowEffect.position.copy(effect.position);
            outerGlowEffect.scale.copy(effect.scale);
            outerGlowEffect.scale.multiplyScalar(1.001);  // Thinner edge region

            // Create inner glow effect (very close to base for subtle gradient)
            const innerGlowEffect = new MarchingCubes(resolution, innerGlowMaterial, true, true, 100000);
            innerGlowEffect.position.copy(effect.position);
            innerGlowEffect.scale.copy(effect.scale);
            innerGlowEffect.scale.multiplyScalar(0.9999);  // Very close to base

            // Initial generation of metaballs
            const numSystems = systemList.length;
            const subtract = 20;  // Much higher subtraction for very sharp edges
            const baseStrength = 0.6;  // Increased for more pronounced shapes
            const strength = baseStrength / ((Math.sqrt(numSystems) - 1) / 4 + 1);

            // Add metaballs to all effects with adjusted parameters for more angular look
            systemList.forEach((system) => {
                const ballx = (system.x - centerX) / width + 0.5;
                const bally = (system.y - centerY) / height + 0.5;
                const ballz = (system.z - centerZ) / depth + 0.5;

                const influenceRadius = this.POWER_LINE_DISTANCES[system.power_state] || 10;
                const localRadius = influenceRadius / Math.min(width, height, depth);
                const scaledStrength = strength * localRadius * 40;  // Much stronger influence

                // Add balls with extreme subtraction values for very angular shapes
                effect.addBall(ballx, bally, ballz, scaledStrength, subtract);
                outerGlowEffect.addBall(ballx, bally, ballz, scaledStrength * 1.03, subtract * 0.85);  // More extreme scaling
                innerGlowEffect.addBall(ballx, bally, ballz, scaledStrength * 0.97, subtract * 1.25);  // More extreme inner contrast
            });

            // Generate meshes once
            effect.update();
            outerGlowEffect.update();
            innerGlowEffect.update();

            this.scene.add(effect);
            this.scene.add(outerGlowEffect);
            this.scene.add(innerGlowEffect);

            // Store all three effects in powerMeshObjects
            this.powerMeshObjects.push([effect, outerGlowEffect, innerGlowEffect]);

            // Only animate materials
            if (this.showPowerTerritoryMesh) {
                const updateMaterials = () => {
                    if (!this.showPowerTerritoryMesh) return;
                    
                    // Materials will auto-update through their onBeforeRender functions
                    // No need to regenerate geometry
                    
                    requestAnimationFrame(updateMaterials);
                };
                updateMaterials();
            }
        });
    },

    // Utility functions
    clearObjects(objectMap) {
        objectMap.forEach((meshes, power) => {
            if (Array.isArray(meshes)) {
                meshes.forEach(mesh => {
                    if (mesh) this.scene.remove(mesh);
                });
            } else {
                if (meshes) this.scene.remove(meshes);
            }
        });
        objectMap.clear();
    },

    updateOpacity(camera, NEAR, SWEET, FAR, CUTOFF) {
        // Update power line opacity
        if (this.showPowerLines) {
            this.powerLineObjects.forEach((instance) => {
                const center = new THREE.Vector3();
                instance.geometry.computeBoundingSphere();
                center.copy(instance.geometry.boundingSphere.center);
                const distance = camera.position.distanceTo(center);
                
                if (distance < NEAR) {
                    instance.material.opacity = THREE.MathUtils.clamp(distance / NEAR, CUTOFF, 0.3);
                } else if (distance > SWEET) {
                    instance.material.opacity = THREE.MathUtils.clamp(0.3 - ((distance - SWEET) / (FAR - SWEET)), CUTOFF, 0.3);
                } else {
                    instance.material.opacity = 0.3;
                }
            });
        }

        // Update power volume opacity
        if (this.showPowerVolumes) {
            this.powerMeshObjects.forEach((meshes) => {
                meshes.forEach(mesh => {
                    const distance = camera.position.distanceTo(mesh.position);
                    if (distance < NEAR) {
                        mesh.material.opacity = THREE.MathUtils.clamp(distance / NEAR, CUTOFF, 0.1);
                    } else if (distance > SWEET) {
                        mesh.material.opacity = THREE.MathUtils.clamp(0.1 - ((distance - SWEET) / (FAR - SWEET)), CUTOFF, 0.1);
                    } else {
                        mesh.material.opacity = 0.1;
                    }
                });
            });
        }

        // Update power region opacity
        if (this.showPowerRegions) {
            this.powerRegionObjects.forEach((meshes) => {
                meshes.forEach(mesh => {
                    const center = new THREE.Vector3();
                    mesh.geometry.computeBoundingSphere();
                    center.copy(mesh.geometry.boundingSphere.center);
                    const distance = camera.position.distanceTo(center);
                    
                    const baseOpacity = mesh.material.type === 'LineBasicMaterial' ? 0.1 : 0.05;
                    
                    if (distance < NEAR) {
                        mesh.material.opacity = THREE.MathUtils.clamp(distance / NEAR, CUTOFF, baseOpacity);
                    } else if (distance > SWEET) {
                        mesh.material.opacity = THREE.MathUtils.clamp(baseOpacity - ((distance - SWEET) / (FAR - SWEET)), CUTOFF, baseOpacity);
                    } else {
                        mesh.material.opacity = baseOpacity;
                    }
                });
            });
        }

        // Update organic power line opacity
        if (this.showOrganicPowerLines) {
            this.organicPowerLineObjects.forEach((instance) => {
                const center = new THREE.Vector3();
                instance.geometry.computeBoundingSphere();
                center.copy(instance.geometry.boundingSphere.center);
                const distance = camera.position.distanceTo(center);
                
                if (distance < NEAR) {
                    instance.material.opacity = THREE.MathUtils.clamp(distance / NEAR, CUTOFF, 0.3);
                } else if (distance > SWEET) {
                    instance.material.opacity = THREE.MathUtils.clamp(0.3 - ((distance - SWEET) / (FAR - SWEET)), CUTOFF, 0.3);
                } else {
                    instance.material.opacity = 0.3;
                }
            });
        }
    },

    // Add to animate function or wherever you update your scene
    updateGradients(camera) {
        if (!this.showPowerVolumes) return;
        this.gradientObjects.forEach(obj => obj.update(camera));
    },

    // Add new cleanup function specifically for power gradients
    cleanupPowerGradients() {
        if (this.gradientObjects) {
            this.gradientObjects.forEach((obj) => {
                if (obj.mesh) {
                    this.scene.remove(obj.mesh);
                    if (obj.mesh.geometry) obj.mesh.geometry.dispose();
                    if (obj.mesh.material) {
                        if (obj.mesh.material.map) obj.mesh.material.map.dispose();
                        obj.mesh.material.dispose();
                    }
                }
            });
            this.gradientObjects.clear();
        }
    },

    createPowerGradients(systems, camera) {
        // Don't call clearObjects here anymore - cleanup is handled separately
        if (!this.showPowerVolumes) return;

        // Create shared geometry
        const geometries = {
            stronghold: new THREE.CircleGeometry(30, 16),  // Further reduced segments
            fortified: new THREE.CircleGeometry(20, 16),
            exploited: new THREE.CircleGeometry(10, 16)
        };

        // Create and reuse textures per power
        const powerTextures = new Map();

        // Group systems by power and state for instancing
        const instanceGroups = new Map(); // key: "power_state"
        const tmpVec3 = new THREE.Vector3();
        
        Object.values(systems).forEach(system => {
            if (!system.controlling_power || !system.power_state) return;
            const key = `${system.controlling_power}_${system.power_state.toLowerCase()}`;
            if (!instanceGroups.has(key)) {
                instanceGroups.set(key, []);
            }
            instanceGroups.get(key).push(system);
        });

        // Create instances for each power and state combination
        instanceGroups.forEach((systems, key) => {
            const [power, state] = key.split('_');
            const powerColor = window.powerColors[power];
            if (!powerColor) return;

            // Get or create texture
            let texture = powerTextures.get(power);
            if (!texture) {
                const canvas = document.createElement('canvas');
                canvas.width = 64;  // Even smaller texture
                canvas.height = 64;
                const ctx = canvas.getContext('2d');

                const gradient = ctx.createRadialGradient(32, 32, 0, 32, 32, 32);
                const color = new THREE.Color(powerColor);
                gradient.addColorStop(0, color.getStyle().replace(')', ',0.2)'));
                gradient.addColorStop(0.3, color.getStyle().replace(')', ',0.14)'));
                gradient.addColorStop(0.7, color.getStyle().replace(')', ',0.2)'));
                gradient.addColorStop(1, color.getStyle().replace(')', ',0)'));

                ctx.fillStyle = gradient;
                ctx.fillRect(0, 0, 64, 64);

                texture = new THREE.CanvasTexture(canvas);
                texture.needsUpdate = true;
                powerTextures.set(power, texture);
            }

            // Create material
            const material = new THREE.MeshBasicMaterial({
                map: texture,
                transparent: true,
                depthWrite: false,
                depthTest: true,
                side: THREE.DoubleSide,
                blending: THREE.NormalBlending,
                blendEquation: THREE.AddEquation,
                blendSrc: THREE.SrcAlphaFactor,
                blendDst: THREE.OneMinusSrcAlphaFactor,
                opacity: 0.1
            });

            // Create instanced mesh
            const geometry = geometries[state];
            const instancedMesh = new THREE.InstancedMesh(
                geometry,
                material,
                systems.length
            );
            instancedMesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);

            // Set up matrices for each instance
            const matrix = new THREE.Matrix4();
            const quaternion = new THREE.Quaternion();
            quaternion.copy(camera.quaternion);

            systems.forEach((system, index) => {
                tmpVec3.set(system.x, system.y, system.z);
                matrix.compose(tmpVec3, quaternion, new THREE.Vector3(1, 1, 1));
                instancedMesh.setMatrixAt(index, matrix);
            });

            instancedMesh.instanceMatrix.needsUpdate = true;
            this.scene.add(instancedMesh);

            // Store for updates
            this.gradientObjects.set(key, {
                mesh: instancedMesh,
                systems: systems,
                update: () => {
                    quaternion.copy(camera.quaternion);
                    systems.forEach((system, index) => {
                        tmpVec3.set(system.x, system.y, system.z);
                        matrix.compose(tmpVec3, quaternion, new THREE.Vector3(1, 1, 1));
                        instancedMesh.setMatrixAt(index, matrix);
                    });
                    instancedMesh.instanceMatrix.needsUpdate = true;
                }
            });
        });

        // Clean up geometries
        Object.values(geometries).forEach(geometry => {
            geometry.dispose();
        });
    },

    createVolumetricPowerClouds(systems, camera) {
        // Clean up existing volumetric clouds first
        this.cleanupVolumetricClouds();
        
        if (!this.showPowerTerritories) return;

        if (!this.powerVolumeObjects) {
            this.powerVolumeObjects = {};
        }

        // Create shared geometries
        const geometries = {
            stronghold: new THREE.SphereGeometry(30, 16, 12),
            fortified: new THREE.SphereGeometry(20, 16, 12),
            exploited: new THREE.SphereGeometry(10, 16, 12)
        };

        // Create base shader material
        const createMaterial = (color, radius) => new THREE.ShaderMaterial({
            uniforms: {
                color: { value: color },
                time: { value: 0 },
                cameraPos: { value: camera.position },
                radius: { value: radius }
            },
            vertexShader: `
                varying vec3 vPosition;
                varying vec3 vNormal;
                varying vec3 vWorldPosition;
                
                void main() {
                    vPosition = position;
                    vNormal = normal;
                    vWorldPosition = (modelMatrix * vec4(position, 1.0)).xyz;
                    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
                }
            `,
            fragmentShader: `
                uniform vec3 color;
                uniform float time;
                uniform vec3 cameraPos;
                uniform float radius;
                
                varying vec3 vPosition;
                varying vec3 vNormal;
                varying vec3 vWorldPosition;
                
                // Improved noise functions
                float rand(vec3 p) {
                    return fract(sin(dot(p, vec3(12.9898, 78.233, 45.5432))) * 43758.5453);
                }
                
                float noise(vec3 p) {
                    vec3 i = floor(p);
                    vec3 f = fract(p);
                    f = f * f * (3.0 - 2.0 * f); // Smooth interpolation
                    
                    float a = rand(i);
                    float b = rand(i + vec3(1.0, 0.0, 0.0));
                    float c = rand(i + vec3(0.0, 1.0, 0.0));
                    float d = rand(i + vec3(1.0, 1.0, 0.0));
                    float e = rand(i + vec3(0.0, 0.0, 1.0));
                    float f1 = rand(i + vec3(1.0, 0.0, 1.0));
                    float g = rand(i + vec3(0.0, 1.0, 1.0));
                    float h = rand(i + vec3(1.0, 1.0, 1.0));
                    
                    return mix(
                        mix(mix(a, b, f.x), mix(c, d, f.x), f.y),
                        mix(mix(e, f1, f.x), mix(g, h, f.x), f.y),
                        f.z
                    );
                }
                
                // Fractal Brownian Motion for more detail
                float fbm(vec3 p) {
                    float value = 0.0;
                    float amplitude = 0.5;
                    float frequency = 1.0;
                    
                    for(int i = 0; i < 4; i++) {  // 4 octaves of noise
                        value += amplitude * noise(p * frequency);
                        amplitude *= 0.5;
                        frequency *= 2.0;
                    }
                    
                    return value;
                }
                
                void main() {
                    vec3 viewDir = normalize(vWorldPosition - cameraPos);
                    
                    // Calculate distance from center for density
                    float dist = length(vPosition) / radius;
                    float density = 1.0 - dist;  // Higher in center
                    
                    // Detailed noise calculation
                    vec3 p = vPosition * 0.15 + time * 0.03;  // Adjusted scale and speed
                    float n = fbm(p);
                    
                    // Combine noise with density
                    float alpha = density * (n + 0.8) * 0.4;  // Base cloud density
                    
                    // Add fresnel for edge highlight and fade
                    float fresnel = pow(1.0 - abs(dot(normalize(vNormal), viewDir)), 3.0);
                    float edgeGlow = pow(fresnel, 4.0) * 0.2;  // Subtle edge highlight
                    
                    // Combine everything
                    alpha = alpha * (1.0 - fresnel * 0.5) + edgeGlow;  // Less edge fade, add glow
                    
                    gl_FragColor = vec4(color, alpha);
                }
            `,
            transparent: true,
            depthWrite: false,
            depthTest: true,
            side: THREE.FrontSide,
            blending: THREE.AdditiveBlending
        });

        // Sort systems by distance for better blending
        const sortedSystems = Object.values(systems)
            .filter(system => system.controlling_power && system.power_state)
            .sort((a, b) => {
                const distA = camera.position.distanceTo(new THREE.Vector3(a.x, a.y, a.z));
                const distB = camera.position.distanceTo(new THREE.Vector3(b.x, b.y, b.z));
                return distB - distA;
            });

        // Group by power after sorting
        const powerSystems = {};
        sortedSystems.forEach(system => {
            if (!powerSystems[system.controlling_power]) {
                powerSystems[system.controlling_power] = [];
            }
            powerSystems[system.controlling_power].push(system);
        });

        // Create clouds for each power
        Object.entries(powerSystems).forEach(([power, systemList]) => {
            const powerColor = window.powerColors[power];
            if (!powerColor) return;

            const color = new THREE.Color(powerColor);
            this.powerVolumeObjects[power] = [];

            systemList.forEach(system => {
                let geometry, radius;
                switch(system.power_state.toLowerCase()) {
                    case 'stronghold': 
                        geometry = geometries.stronghold;
                        radius = 30;
                        break;
                    case 'fortified': 
                        geometry = geometries.fortified;
                        radius = 20;
                        break;
                    case 'exploited': 
                        geometry = geometries.exploited;
                        radius = 10;
                        break;
                    default: return;
                }

                const material = createMaterial(color, radius);
                const mesh = new THREE.Mesh(geometry, material);
                mesh.position.set(system.x, system.y, system.z);
                mesh.renderOrder = Math.floor(camera.position.distanceTo(mesh.position));

                this.scene.add(mesh);
                this.powerVolumeObjects[power].push(mesh);
            });
        });

        // Clean up geometries
        Object.values(geometries).forEach(geometry => {
            geometry.dispose();
        });

        // Start animation if not already running
        if (!this._volumeAnimationStarted) {
            this._volumeAnimationStarted = true;
            const clock = new THREE.Clock();
            const animate = () => {
                if (!this.showPowerTerritories) {
                    this._volumeAnimationStarted = false;
                    return;
                }

                const time = clock.getElapsedTime();
                Object.values(this.powerVolumeObjects).flat().forEach(mesh => {
                    if (mesh && mesh.material.uniforms) {
                        mesh.material.uniforms.time.value = time;
                        mesh.material.uniforms.cameraPos.value.copy(camera.position);
                        // Update render order based on distance
                        mesh.renderOrder = Math.floor(camera.position.distanceTo(mesh.position));
                    }
                });

                requestAnimationFrame(animate);
            };
            animate();
        }
    },

    // Add proxy function to call the extra visualizer's function
    createGeodesicTerritories(systems, camera) {
        PowerVisualizerExtra.createGeodesicTerritories(systems, camera);
    },

    cleanup() {
        this.powerLineObjects.forEach((instance) => {
            if (instance) this.scene.remove(instance);
        });
        this.powerLineObjects.clear();

        this.powerMeshObjects.forEach((meshes) => {
            meshes.forEach((mesh) => {
                if (mesh) {
                    if (mesh.material) {
                        if (Array.isArray(mesh.material)) {
                            mesh.material.forEach(mat => {
                                if (mat.map) mat.map.dispose();
                                mat.dispose();
                            });
                        } else {
                            if (mesh.material.map) mesh.material.map.dispose();
                            mesh.material.dispose();
                        }
                    }
                    if (mesh.geometry) mesh.geometry.dispose();
                    this.scene.remove(mesh);
                }
            });
        });
        this.powerMeshObjects = [];

        this.powerRegionObjects.forEach((meshes) => {
            meshes.forEach((mesh) => {
                if (mesh) {
                    if (mesh.material) mesh.material.dispose();
                    if (mesh.geometry) mesh.geometry.dispose();
                    this.scene.remove(mesh);
                }
            });
        });
        this.powerRegionObjects.clear();

        this.organicPowerLineObjects.forEach((instance) => {
            if (instance) this.scene.remove(instance);
        });
        this.organicPowerLineObjects.clear();

        this.gradientObjects.forEach((obj) => {
            if (obj.mesh) {
                this.scene.remove(obj.mesh);
                obj.mesh.geometry.dispose();
                obj.mesh.material.dispose();
            }
        });
        this.gradientObjects.clear();

        // Use the shared clearObjects function for geodesicObjects
        PowerVisualizerExtra.clearObjects(this.geodesicObjects);
    },

    // Add getter/setter for geodesic territories state
    set showGeodesicTerritories(value) {
        this._showGeodesicTerritories = value;
        if (PowerVisualizerExtra) {
            PowerVisualizerExtra.showGeodesicTerritories = value;
        }
    },

    get showGeodesicTerritories() {
        return this._showGeodesicTerritories;
    },

    // Cleanup function for power mesh objects
    cleanupPowerMesh() {
        if (!this.powerMeshObjects) {
            this.powerMeshObjects = [];
        }
        this.powerMeshObjects.forEach(objects => {
            if (Array.isArray(objects)) {
                objects.forEach(obj => {
                    if (obj) {
                        if (obj.material) {
                            if (Array.isArray(obj.material)) {
                                obj.material.forEach(mat => {
                                    if (mat.map) mat.map.dispose();
                                    mat.dispose();
                                });
                            } else {
                                if (obj.material.map) obj.material.map.dispose();
                                obj.material.dispose();
                            }
                        }
                        if (obj.geometry) obj.geometry.dispose();
                        this.scene.remove(obj);
                    }
                });
            }
        });
        this.powerMeshObjects = [];
    },

    // Cleanup function for volumetric power clouds
    cleanupVolumetricClouds() {
        if (this._volumeAnimationStarted) {
            this._volumeAnimationStarted = false;
        }
        if (this.powerVolumeObjects) {
            Object.values(this.powerVolumeObjects).flat().forEach(mesh => {
                if (mesh) {
                    if (mesh.material) {
                        if (mesh.material.uniforms) {
                            Object.values(mesh.material.uniforms).forEach(uniform => {
                                if (uniform.value && uniform.value.dispose) {
                                    uniform.value.dispose();
                                }
                            });
                        }
                        mesh.material.dispose();
                    }
                    if (mesh.geometry) mesh.geometry.dispose();
                    this.scene.remove(mesh);
                }
            });
            this.powerVolumeObjects = {};
        }
    }
};

// Note on volumetric rendering:
// THREE.js supports several approaches for volumetric effects:
// 1. THREE.Fog/FogExp2 for simple volumetric effects
// 2. THREE.Points with custom shaders for particle clouds
// 3. Ray marching in custom shaders for true volumetric rendering
// 4. THREE.Volume for medical-style volume rendering
// 5. Instanced meshes with noise-based opacity
//
// For a cloud-like power volume visualization, we could:
// 1. Create spherical influence zones using ray marching
// 2. Use 3D noise textures for cloud-like appearance
// 3. Connect spheres using metaballs or marching cubes
// This would create a more organic, volumetric appearance. 