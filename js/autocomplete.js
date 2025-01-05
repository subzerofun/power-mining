class SystemAutocomplete {
    constructor(inputElement, resultsElement) {
        this.input = inputElement;
        this.results = resultsElement;
        this.debounceTimer = null;
        this.selectedSystem = null;
        
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Input event for typing
        this.input.addEventListener('input', () => {
            clearTimeout(this.debounceTimer);
            this.debounceTimer = setTimeout(() => this.handleInput(), 300);
        });

        // Handle keyboard navigation
        this.input.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                this.handleArrowKeys(e.key);
            } else if (e.key === 'Enter') {
                const selected = this.results.querySelector('.selected');
                if (selected) {
                    e.preventDefault();
                    this.selectSystem(selected);
                }
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

    async handleInput() {
        const query = this.input.value.trim();
        if (query.length < 2) {
            this.hideResults();
            return;
        }

        try {
            const response = await fetch(`/autocomplete?q=${encodeURIComponent(query)}`);
            const systems = await response.json();
            this.showResults(systems);
        } catch (error) {
            console.error('Error fetching system suggestions:', error);
        }
    }

    showResults(systems) {
        this.results.innerHTML = '';
        
        if (systems.length === 0) {
            this.hideResults();
            return;
        }

        systems.forEach((system, index) => {
            const div = document.createElement('div');
            div.className = 'autocomplete-item';
            div.textContent = system.name;
            div.dataset.coords = JSON.stringify(system.coords);
            
            div.addEventListener('click', () => this.selectSystem(div));
            
            if (index === 0) div.classList.add('selected');
            this.results.appendChild(div);
        });

        this.results.style.display = 'block';
    }

    hideResults() {
        this.results.style.display = 'none';
        this.results.innerHTML = '';
    }

    handleArrowKeys(key) {
        const items = this.results.querySelectorAll('.autocomplete-item');
        const selected = this.results.querySelector('.selected');
        
        if (!selected || items.length === 0) return;

        const currentIndex = Array.from(items).indexOf(selected);
        let nextIndex;

        if (key === 'ArrowDown') {
            nextIndex = (currentIndex + 1) % items.length;
        } else {
            nextIndex = (currentIndex - 1 + items.length) % items.length;
        }

        selected.classList.remove('selected');
        items[nextIndex].classList.add('selected');
        items[nextIndex].scrollIntoView({ block: 'nearest' });
    }

    selectSystem(element) {
        this.input.value = element.textContent;
        this.selectedSystem = {
            name: element.textContent,
            coords: JSON.parse(element.dataset.coords)
        };
        this.hideResults();
        
        // Dispatch custom event for system selection
        const event = new CustomEvent('systemSelected', {
            detail: this.selectedSystem
        });
        this.input.dispatchEvent(event);
    }

    getSelectedSystem() {
        return this.selectedSystem;
    }
}

// Initialize autocomplete when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    const input = document.getElementById('system');
    const results = document.querySelector('.autocomplete-results');
    window.systemAutocomplete = new SystemAutocomplete(input, results);
}); 