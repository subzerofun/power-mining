// Generic input handler for sliders and number inputs
class InputHandler {
    constructor(options) {
        this.input = document.getElementById(options.inputId);
        this.display = options.displayElement;
        this.suffix = options.suffix || '';
        this.isSlider = this.input?.type === 'range';
        
        if (this.input && this.display) {
            this.setupInput();
        }
    }

    updateValue(value) {
        if (this.isSlider) {
            const max = this.input.max;
            const percentage = (value / max) * 100;
            this.input.style.setProperty('--slider-percentage', `${percentage}%`);
        }
        this.display.textContent = `${value}${this.suffix}`;
    }

    setupInput() {
        // Set initial value
        this.updateValue(this.input.value);

        // Update on input and change
        const updateHandler = (e) => this.updateValue(e.target.value);
        this.input.addEventListener('input', updateHandler);
        this.input.addEventListener('change', updateHandler);
    }
}

// Initialize input handlers when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Distance slider
    new InputHandler({
        inputId: 'distance',
        displayElement: document.querySelector('.distance-value'),
        suffix: ' ly'
    });

    // Search results input
    const resultsInput = document.getElementById('limit');
    if (resultsInput) {
        // Create a display element for search results if it doesn't exist
        let resultsDisplay = resultsInput.nextElementSibling;
        if (!resultsDisplay || !resultsDisplay.classList.contains('input-value')) {
            resultsDisplay = document.createElement('span');
            resultsDisplay.className = 'input-value';
            resultsInput.parentNode.insertBefore(resultsDisplay, resultsInput.nextSibling);
        }

        new InputHandler({
            inputId: 'limit',
            displayElement: resultsDisplay
        });
    }
}); 