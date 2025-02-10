class MapSystemAutocomplete {
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
            this.debounceTimer = setTimeout(() => this.handleInput(), 150);
        });

        // Handle keyboard navigation
        this.input.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                this.handleArrowKeys(e.key);
            } else if (e.key === 'Enter') {
                e.preventDefault();
                const inputValue = this.input.value.trim().toLowerCase();
                const selected = this.results.querySelector('.selected');
                
                // First check if we have a selected item in dropdown
                if (selected) {
                    this.selectSystem(selected);
                    const form = this.input.closest('form');
                    if (form) form.requestSubmit();
                    return;
                }
                
                // If no dropdown selection, check for exact match in systems
                if (window.systems && inputValue) {
                    const matchingSystem = window.systems[inputValue];
                    if (matchingSystem) {
                        this.selectedSystem = matchingSystem;
                        this.input.value = matchingSystem.name;
                        const form = this.input.closest('form');
                        if (form) form.requestSubmit();
                        return;
                    }
                }
            } else if (e.key === 'ArrowRight') {
                e.preventDefault();
                this.handleArrowKeys('ArrowRight');
            } else if (e.key === 'Escape') {
                this.hideResults();
            }
        });

        // Close results when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.input.contains(e.target) && !this.results.contains(e.target)) {
                this.hideResults();
            }
        });
    }

    handleInput() {
        const query = this.input.value.trim().toLowerCase();
        if (query.length < 2) {
            this.hideResults();
            return;
        }

        // Check if systems are loaded
        if (!window.systems || Object.keys(window.systems).length === 0) {
            console.log('Systems not loaded yet, please wait...');
            this.showResults([{
                name: 'Loading systems...',
                system: null
            }]);
            return;
        }

        // Find matching systems
        const matchingSystems = Object.entries(window.systems)
            .filter(([key]) => key.startsWith(query))
            .map(([_, system]) => ({
                name: system.name,
                system: system
            }))
            .slice(0, 10);

        // Show results
        this.showResults(matchingSystems);
        
        // Show suggestion after a short delay
        clearTimeout(this.suggestionTimer);
        this.suggestionTimer = setTimeout(() => {
            if (this.input.value.trim().toLowerCase() === query) {
                this.showSuggestion(matchingSystems[0]?.name, query);
            }
        }, 50);
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
        suggestionSpan.textContent = ' ' + firstMatch.slice(inputValue.length);
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
        const paddingLeft = parseFloat(inputStyle.paddingLeft);
        
        // Add suggestion span to input's parent and position it
        this.input.parentElement.appendChild(suggestionSpan);
        suggestionSpan.style.left = `${textWidth + paddingLeft}px`;
        suggestionSpan.style.top = '0';
        suggestionSpan.style.height = '100%';
    }

    showResults(systems) {
        this.results.innerHTML = '';
        
        if (systems.length === 0) {
            this.hideResults();
            return;
        }

        // Add CSS to prevent container shift
        this.results.style.position = 'absolute';
        this.results.style.maxHeight = '200px';
        this.results.style.overflowY = 'auto';
        this.results.style.width = '100%';
        this.results.style.zIndex = '1000';

        systems.forEach((system, index) => {
            const div = document.createElement('div');
            div.className = 'autocomplete-item';
            div.textContent = system.name;
            // Store the entire system object
            div.dataset.system = JSON.stringify(system.system);
            
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

    selectSystem(element) {
        if (!element) return;
        
        this.input.value = element.textContent;
        this.selectedSystem = JSON.parse(element.dataset.system);
        this.hideResults();
        
        // Show system info immediately
        if (this.selectedSystem) {
            //showSystemInfo(this.selectedSystem);
        }
    }

    getSelectedSystem() {
        return this.selectedSystem;
    }
}

// Remove automatic initialization since we'll initialize it explicitly after systems load