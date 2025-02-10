class SystemAutocomplete {
    constructor(inputElement, resultsElement) {
        this.input = inputElement;
        this.results = resultsElement;
        this.debounceTimer = null;
        this.suggestionTimer = null;
        this.selectedSystem = null;
        
        this.setupEventListeners();
        this.input.setAttribute('autocomplete', 'off');
    }

    setupEventListeners() {
        // Input event for typing
        this.input.addEventListener('input', () => {
            // Clear any existing suggestion immediately
            const existingSuggestion = document.querySelector('.input-suggestion');
            if (existingSuggestion) {
                existingSuggestion.remove();
            }
            
            clearTimeout(this.debounceTimer);
            clearTimeout(this.suggestionTimer);
            this.debounceTimer = setTimeout(() => this.handleInput(), 150); // Reduced from 300ms
        });

        // Handle keyboard navigation
        this.input.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                this.handleArrowKeys(e.key);
            } else if (e.key === 'Enter') {
                e.preventDefault();
                const inputValue = this.input.value.trim().toLowerCase();
                
                // First check for exact match in systems
                if (window.systems && inputValue) {
                    const matchingSystem = window.systems[inputValue];
                    if (matchingSystem) {
                        this.selectedSystem = matchingSystem;
                        this.input.value = matchingSystem.name;
                        const form = this.input.closest('form');
                        if (form) form.requestSubmit();
                        this.hideResults();  // Hide dropdown after submitting
                        return;
                    }
                }
                
                // If no exact match, then check for selected item in dropdown
                const selected = this.results.querySelector('.selected');
                if (selected) {
                    this.selectSystem(selected);
                    const form = this.input.closest('form');
                    if (form) form.requestSubmit();
                    this.hideResults();  // Hide dropdown after submitting
                    return;
                }
            } else if (e.key === 'Escape') {
                this.hideResults();
            } else if (e.key === 'ArrowRight') {
                e.preventDefault();
                this.handleArrowKeys('ArrowRight');
            }
        });

        // Close results when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.input.contains(e.target) && !this.results.contains(e.target)) {
                this.hideResults();
            }
        });
    }

    async handleInput() {
        const query = this.input.value.trim();
        if (query.length < 2) {
            this.hideResults();
            return;
        }

        try {
            const response = await fetch(`/autocomplete?q=${encodeURIComponent(query)}`);
            const systems = await response.json();
            
            // Store systems in window.systems for exact matching
            window.systems = {};
            systems.forEach(system => {
                window.systems[system.name.toLowerCase()] = system;
            });
            
            // Clear any existing suggestion timer
            clearTimeout(this.suggestionTimer);
            
            // Show results immediately but delay showing suggestion
            this.showResults(systems, false);
            
            // Show suggestion after a short delay
            this.suggestionTimer = setTimeout(() => {
                if (this.input.value.trim() === query) { // Only show if input hasn't changed
                    this.showSuggestion(systems[0]?.name, query);
                }
            }, 50);
        } catch (error) {
            console.error('Error fetching system suggestions:', error);
        }
    }

    showSuggestion(firstMatch, inputValue) {
        if (!firstMatch || !inputValue || !firstMatch.toLowerCase().startsWith(inputValue.toLowerCase())) {
            return;
        }

        // Remove any existing suggestions
        const existingSuggestion = document.querySelector('.input-suggestion');
        if (existingSuggestion) {
            existingSuggestion.remove();
        }

        const suggestionSpan = document.createElement('span');
        suggestionSpan.className = 'input-suggestion';
        suggestionSpan.textContent = firstMatch.slice(inputValue.length);
        suggestionSpan.style.position = 'absolute';
        suggestionSpan.style.color = '#666';
        suggestionSpan.style.fontStyle = 'normal';
        suggestionSpan.style.pointerEvents = 'none';
        suggestionSpan.style.backgroundColor = 'transparent';
        suggestionSpan.style.lineHeight = window.getComputedStyle(this.input).lineHeight;
        suggestionSpan.style.display = 'flex';
        suggestionSpan.style.alignItems = 'center';
        
        // Calculate text width using input's font
        const inputStyle = window.getComputedStyle(this.input);
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        context.font = inputStyle.font;
        const textWidth = context.measureText(inputValue).width;
        
        // Position relative to input's parent
        const inputRect = this.input.getBoundingClientRect();
        const parentRect = this.input.parentElement.getBoundingClientRect();
        const paddingLeft = parseFloat(inputStyle.paddingLeft);
        
        // Add suggestion span to input's parent and position it
        this.input.parentElement.appendChild(suggestionSpan);
        suggestionSpan.style.left = `${textWidth + paddingLeft}px`;
        suggestionSpan.style.top = '0';
        suggestionSpan.style.height = '100%';
    }

    showResults(systems, showSuggestion = true) {
        this.results.innerHTML = '';
        
        if (systems.length === 0) {
            this.hideResults();
            return;
        }

        // Add CSS to prevent container shift
        this.results.style.position = 'absolute';
        this.results.style.maxHeight = '200px';  // Limit dropdown height
        this.results.style.overflowY = 'auto';   // Enable scrolling
        this.results.style.width = '100%';       // Match input width
        this.results.style.zIndex = '1000';      // Ensure it's above other content

        systems.forEach((system, index) => {
            const div = document.createElement('div');
            div.className = 'autocomplete-item';
            div.textContent = system.name;
            div.dataset.coords = JSON.stringify(system.coords);
            
            if (index === 0) {
                div.classList.add('selected');
                div.style.backgroundColor = '#ff9e0433';
            }
            
            div.addEventListener('click', () => {
                this.selectSystem(div);
                // Submit the form after click selection
                const form = this.input.closest('form');
                if (form) {
                    form.requestSubmit();
                }
            });
            this.results.appendChild(div);
        });

        this.results.style.display = 'block';
    }

    hideResults() {
        this.results.style.display = 'none';
        this.results.innerHTML = '';
        // Remove any existing suggestions
        const suggestion = document.querySelector('.input-suggestion');
        if (suggestion) {
            suggestion.remove();
        }
    }

    handleArrowKeys(key) {
        const items = this.results.querySelectorAll('.autocomplete-item');
        const selected = this.results.querySelector('.selected');
        
        if (!selected || items.length === 0) return;

        if (key === 'ArrowRight') {
            // Complete with the first match
            const firstMatch = items[0];
            if (firstMatch) {
                this.input.value = firstMatch.textContent;
                this.hideResults();
                // Submit the search form immediately
                const form = this.input.closest('form');
                if (form) {
                    form.requestSubmit();
                }
                return;
            }
        }

        const currentIndex = Array.from(items).indexOf(selected);
        let nextIndex;

        if (key === 'ArrowDown') {
            nextIndex = (currentIndex + 1) % items.length;
        } else if (key === 'ArrowUp') {
            nextIndex = (currentIndex - 1 + items.length) % items.length;
        }

        selected.classList.remove('selected');
        selected.style.backgroundColor = '';
        items[nextIndex].classList.add('selected');
        items[nextIndex].style.backgroundColor = '#ff9e0433';
        items[nextIndex].scrollIntoView({ block: 'nearest' });
    }

    selectSystem(div) {
        // Keep the original case from the system name
        this.input.value = div.textContent;
        this.hideResults();
        
        // Store the coordinates if available
        if (div.dataset.coords) {
            this.selectedCoords = JSON.parse(div.dataset.coords);
        }
    }

    getSelectedSystem() {
        return this.selectedSystem;
    }
}

// System states list
const systemStates = [
    'Any',  // Add Any as first option
    'Blight',
    'Boom',
    'Bust',
    'Civil liberty',
    'Civil unrest',
    'Civil war',
    'Drought',
    'Election',
    'Expansion',
    'Famine',
    'Infrastructure Failure',
    'Investment',
    'Lockdown',
    'Natural Disaster',
    'None',
    'Outbreak',
    'Pirate attack',
    'Public Holiday',
    'Retreat',
    'Terrorist Attack',
    'War'
];

// Global set to track selected states
window.selectedSystemStates = new Set(['Any']);  // Initialize with Any

function setupSystemStateAutocomplete() {
    const stateInput = document.getElementById('systemStateInput');
    const stateResults = document.getElementById('systemStateAutocomplete');
    
    // Only proceed if both elements exist
    if (stateInput && stateResults) {
        // Initialize with Any selection
        updateSelectedSystemStates();
        stateInput.setAttribute('autocomplete', 'off');

        // Show all options immediately on focus
        stateInput.addEventListener('focus', () => {
            const availableStates = systemStates.filter(state => !selectedSystemStates.has(state));
            stateResults.innerHTML = availableStates
                .map(state => `<div class="autocomplete-item" data-name="${state}">${state}</div>`)
                .join('');
            stateResults.style.display = 'grid';
        });

        // Filter on input
        stateInput.addEventListener('input', () => {
            const value = stateInput.value.toLowerCase();
            const matches = systemStates.filter(state => 
                state.toLowerCase().includes(value) && 
                !selectedSystemStates.has(state)
            );

            if (matches.length > 0) {
                stateResults.innerHTML = matches
                    .map(state => `<div class="autocomplete-item" data-name="${state}">${state}</div>`)
                    .join('');
                stateResults.style.display = 'grid';
            } else {
                stateResults.style.display = 'none';
            }
        });

        // Handle selection
        stateResults.addEventListener('click', (e) => {
            const item = e.target.closest('.autocomplete-item');
            if (!item) return;
            
            const state = item.dataset.name;
            if (state === 'Any') {
                selectedSystemStates.clear();
                selectedSystemStates.add('Any');
            } else {
                selectedSystemStates.delete('Any');
                selectedSystemStates.add(state);
            }
            updateSelectedSystemStates();
            stateInput.value = '';
            stateResults.style.display = 'none';
        });

        // Close autocomplete when clicking outside
        document.addEventListener('click', (e) => {
            if (!stateInput.contains(e.target) && !stateResults.contains(e.target)) {
                stateResults.style.display = 'none';
            }
        });
    }
}

function updateSelectedSystemStates() {
    console.log('Updating selected states');
    const selectedDiv = document.querySelector('.selected-system-states');
    if (!selectedDiv) {
        console.error('Could not find selected-system-states div');
        return;
    }

    selectedDiv.innerHTML = Array.from(selectedSystemStates)
        .map(state => `
            <span class="system-state-tag">
                ${state}
                <span class="remove" data-state="${state}">&times;</span>
            </span>
        `)
        .join('');

    // Add click handlers for remove buttons
    selectedDiv.querySelectorAll('.remove').forEach(btn => {
        btn.addEventListener('click', () => {
            const state = btn.dataset.state;
            selectedSystemStates.delete(state);
            updateSelectedSystemStates();
        });
    });
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, initializing autocomplete...');
    
    // Initialize system autocomplete
    const input = document.getElementById('system');
    const results = document.querySelector('.autocomplete-results');
    if (input && results) {
        window.systemAutocomplete = new SystemAutocomplete(input, results);
    }
    
    // Only initialize system state if not in map view
    if (!document.querySelector('.search-menu.\\3dmap') && document.getElementById('systemStateInput')) {
        console.log('Found system state input, setting up...');
        setupSystemStateAutocomplete();
    }
}); 