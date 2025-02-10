class SearchMenu {
    constructor() {
        this.activeButton = 'mine-for-merits';
        this.buttons = document.querySelectorAll('.search-menu-button');
        this.formContainer = document.querySelector('#searchForm');
        this.originalFormHTML = this.formContainer.innerHTML;
        
        // Configure menu options
        this.menuConfig = {
            'mine-for-merits': {
                showSearchForm: true,
                visibleButtons: ['mainSearchButton', 'highestPricesButton'],
                searchFunction: null,  // Use default form submit
                visibleFields: [
                    'system', 
                    'distance', 
                    'limit',
                    'controlling_power',
                    'power_goal',
                    'opposing_power',
                    'signal_type',
                    'ring_type_filter',
                    'reserve_level',
                    'mining-type-group',
                    'minDemand',
                    'maxDemand',
                    'landingPadSize',
                    'materialsInput',
                    'systemStateInput'
                ]
            },
            'res-hotspots': {
                showSearchForm: true,
                visibleButtons: ['resSearchButton'],
                searchFunction: 'searchResHotspots',
                visibleFields: [
                    'system', 
                    'distance',
                    'limit',
                    'controlling_power',
                    'opposing_power'
                ]
            },
            'platinum-spots': {
                showSearchForm: true,
                visibleButtons: ['platinumSearchButton'],
                searchFunction: 'searchHighYieldPlatinum',
                visibleFields: [
                    'system', 
                    'distance',
                    'limit',
                    'controlling_power',
                    'opposing_power'
                ]
            },
            'system-finder': {
                needsMapRedirect: true
            },
            'how-to': {
                template: 'templates/how_to.html',
                showSearchForm: true,
                isInfoPage: true
            }
        };

        this.setupEventListeners();
        this.createInfoContainer();
        this.updateButtonStates();
        
        // Initialize form state
        const config = this.menuConfig[this.activeButton];
        if (config && config.showSearchForm) {
            this.formContainer.style.display = 'block';
            // First hide everything
            this.formContainer.querySelectorAll('.form-group, button').forEach(element => {
                element.style.display = 'none';
            });
            // Then show what we need for mine-for-merits
            this.updateVisibleElements(config.visibleButtons, config.visibleFields);
        }
    }

    createInfoContainer() {
        // Create container for info pages that will slide over the form
        this.infoContainer = document.createElement('div');
        this.infoContainer.className = 'info-container';
        this.infoContainer.style.display = 'none';
        this.formContainer.parentNode.insertBefore(this.infoContainer, this.formContainer.nextSibling);
    }

    setupEventListeners() {
        // Menu button clicks
        this.buttons.forEach(button => {
            button.addEventListener('click', () => {
                const buttonId = button.getAttribute('data-id');
                this.setActiveButton(buttonId);
            });
        });

        // Handle enter key on system input
        const systemInput = this.formContainer.querySelector('#system');
        if (systemInput) {
            systemInput.addEventListener('keypress', (event) => {
                if (event.key === 'Enter') {
                    const config = this.menuConfig[this.activeButton];
                    
                    if (config && config.searchFunction) {
                        // For res/plat searches
                        event.preventDefault();
                        window[config.searchFunction]();
                    }
                    // For mine-for-merits, let the normal form submit happen
                }
            });
        }

        // Form submit handling
        this.formContainer.addEventListener('submit', (event) => {
            const config = this.menuConfig[this.activeButton];
            if (config && config.searchFunction) {
                // Prevent default only for special searches
                event.preventDefault();
            }
        });
    }

    async setActiveButton(buttonId) {
        if (buttonId === this.activeButton) return;
        
        const config = this.menuConfig[buttonId];
        
        // Special handling for system finder - redirect to map page
        if (buttonId === 'system-finder') {
            window.location.href = '/map.html';
            return;
        }
        
        // Update active button state FIRST
        this.activeButton = buttonId;
        this.updateButtonStates();
        
        // Handle info pages (like how-to)
        if (config.isInfoPage) {
            try {
                const response = await fetch(config.template);
                if (!response.ok) throw new Error(`Failed to load template for ${buttonId}`);
                
                const content = await response.text();
                this.infoContainer.innerHTML = content;
                this.infoContainer.style.display = 'block';
                this.formContainer.style.display = 'none'; // Hide the entire form
                
                // Clean up all result tables
                const resultTables = document.querySelectorAll('.results-table');
                resultTables.forEach(table => {
                    table.style.display = 'none';
                    const tbody = table.querySelector('tbody');
                    if (tbody) tbody.innerHTML = '';
                });
            } catch (error) {
                console.error('Failed to load template:', error);
            }
        } else {
            // Regular menu items
            this.infoContainer.style.display = 'none';
            if (config.showSearchForm) {
                this.formContainer.style.display = 'block';
                this.updateVisibleElements(config.visibleButtons, config.visibleFields);
            }
        }
    }

    updateVisibleElements(visibleButtons, visibleFields) {
        // First hide everything
        this.formContainer.querySelectorAll('.form-group, button').forEach(element => {
            element.style.display = 'none';
        });

        // Then show only what's needed for the current menu
        if (this.activeButton === 'mine-for-merits') {
            // Show all form groups for mine-for-merits
            this.formContainer.querySelectorAll('.form-group').forEach(group => {
                group.style.display = 'flex';
            });
            // Show main buttons
            this.formContainer.querySelectorAll('.mainSearchButton, .highestPricesButton').forEach(btn => {
                btn.style.display = 'block';
            });
            // Show search button
            const searchButton = this.formContainer.querySelector('button[type="submit"]');
            if (searchButton) searchButton.style.display = 'block';
        } else {
            // For res-hotspots and platinum-spots
            // Show only specified form groups
            this.formContainer.querySelectorAll('.form-group').forEach(group => {
                const input = group.querySelector('input, select');
                const inputId = input?.id;
                if (inputId && visibleFields.includes(inputId)) {
                    group.style.display = 'flex';
                }
            });
            // Show specific button
            if (this.activeButton === 'res-hotspots') {
                const resButton = this.formContainer.querySelector('.resSearchButton');
                if (resButton) resButton.style.display = 'block';
            } else if (this.activeButton === 'platinum-spots') {
                const platButton = this.formContainer.querySelector('.platinumSearchButton');
                if (platButton) platButton.style.display = 'block';
            }
        }

        // Handle row visibility
        this.formContainer.querySelectorAll('.form-row').forEach(row => {
            // Skip the button row - always keep it visible
            if (row.querySelector('button')) {
                row.style.removeProperty('display');
                return;
            }
            
            const hasVisibleGroup = Array.from(row.querySelectorAll('.form-group'))
                .some(group => group.style.display !== 'none');
            row.style.display = hasVisibleGroup ? '' : 'none';
        });
    }

    updateButtonStates() {
        // Remove active class from all buttons and their images
        this.buttons.forEach(button => {
            button.classList.remove('active');
            const img = button.querySelector('img');
            if (img) img.classList.remove('active');
        });
        
        // Add active class to current button and its image
        const activeButton = Array.from(this.buttons).find(button => 
            button.getAttribute('data-id') === this.activeButton
        );
        
        if (activeButton) {
            activeButton.classList.add('active');
            const img = activeButton.querySelector('img');
            if (img) img.classList.add('active');
        }
    }

    getActiveButton() {
        return this.activeButton;
    }
}

// Export the class
window.SearchMenu = SearchMenu;

// Initialize menu when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.searchMenu = new SearchMenu();
}); 