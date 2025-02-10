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