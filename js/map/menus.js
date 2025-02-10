// Controls menu state
let isControlsVisible = false;

// Initialize controls menu
export function initControlsMenu() {
    const controlsButton = document.getElementById('controls-visual');
    const controlsDiv = document.getElementById('controls');
    const closeButton = document.getElementById('close-display-info');

    // Set initial state
    controlsDiv.style.display = 'none';
    controlsDiv.style.opacity = '0';

    // Function to toggle menu state
    const toggleMenu = () => {
        isControlsVisible = !isControlsVisible;
        
        // Toggle active state on button
        controlsButton.classList.toggle('active-button', isControlsVisible);
        
        if (isControlsVisible) {
            // Show menu
            controlsDiv.style.display = 'flex';
            // Force a reflow to ensure the transition works
            controlsDiv.offsetHeight;
            controlsDiv.classList.add('open');
            controlsDiv.style.opacity = '1';
        } else {
            // Hide menu
            controlsDiv.classList.remove('open');
            controlsDiv.style.opacity = '0';
            // Wait for transition to complete before hiding
            setTimeout(() => {
                if (!isControlsVisible) { // Check state again in case it changed
                    controlsDiv.style.display = 'none';
                }
            }, 300); // Match transition duration
        }
    };

    // Add click handlers
    controlsButton.addEventListener('click', toggleMenu);
    closeButton.addEventListener('click', toggleMenu);
}

// Export menu state
export function isControlsMenuVisible() {
    return isControlsVisible;
}
