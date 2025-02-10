// Form storage handling
class FormStorage {
    constructor() {
        this.storageKey = 'searchFormValues';
        this.defaults = {
            'system': 'Harma',
            'distance': '200',
            'power_goal': 'Reinforce',
            'controlling_power': 'Archon Delaine',
            'opposing_power': 'Any',
            'maxDemand': '90000'
        };
        
        // Check for form existence periodically until found
        this.checkForForm();
    }

    checkForForm() {
        const form = document.getElementById('searchForm');
        if (form) {
            // Form exists, load values immediately
            this.loadSavedFormValues();
        } else {
            // Check again in 50ms
            setTimeout(() => this.checkForForm(), 50);
        }
    }

    saveFormValues(miningSearch) {
        const values = {};
        const form = document.getElementById('searchForm');
        if (!form) return;
        
        // Save form field values
        form.querySelectorAll('input, select').forEach(element => {
            if (element.type === 'checkbox') {
                if (element.checked) {
                    if (!values[element.name]) {
                        values[element.name] = [];
                    }
                    values[element.name].push(element.value);
                }
            } else {
                const identifier = element.id || element.name;
                if (identifier) {
                    values[identifier] = element.value;
                }
            }
        });

        // Save dynamic fields only if they exist and have values
        if (miningSearch?.selectedMaterials) {
            values.selectedMaterials = Array.from(miningSearch.selectedMaterials);
        }
        if (miningSearch?.selectedMiningTypes) {
            values.selectedMiningTypes = Array.from(miningSearch.selectedMiningTypes);
        }
        if (window.selectedSystemStates) {
            values.selectedSystemStates = Array.from(window.selectedSystemStates);
        }

        localStorage.setItem(this.storageKey, JSON.stringify(values));
    }

    loadSavedFormValues() {
        const savedValues = localStorage.getItem(this.storageKey);
        let values = null;

        if (savedValues) {
            try {
                values = JSON.parse(savedValues);
            } catch (e) {
                console.error('Failed to parse saved values:', e);
                values = null;
            }
        }

        // If no valid saved values, use defaults
        if (!values) {
            values = this.defaults;
        }

        if (values) {
            // Restore basic form fields only if we have saved values
            Object.entries(values).forEach(([key, value]) => {
                const element = document.getElementById(key) || document.querySelector(`[name="${key}"]`);
                if (element && !element.classList.contains('dynamic-input')) {
                    if (element.type === 'checkbox') {
                        element.checked = value;
                    } else {
                        element.value = value;
                    }
                }
            });
        }

        // Handle dynamic fields only after MiningSearch is ready
        const waitForMiningSearch = setInterval(() => {
            if (window.miningSearch) {
                clearInterval(waitForMiningSearch);
                
                if (values) {
                    // Restore saved values if they exist
                    if (values.selectedMaterials && window.miningSearch.updateSelectedMaterials) {
                        window.miningSearch.selectedMaterials = new Set(values.selectedMaterials);
                        window.miningSearch.updateSelectedMaterials();
                    }
                    if (values.selectedMiningTypes && window.miningSearch.updateSelectedMiningTypes) {
                        window.miningSearch.selectedMiningTypes = new Set(values.selectedMiningTypes);
                        window.miningSearch.updateSelectedMiningTypes();
                    }
                    if (values.selectedSystemStates && typeof updateSelectedSystemStates === 'function') {
                        window.selectedSystemStates = new Set(values.selectedSystemStates);
                        updateSelectedSystemStates();
                    }
                } else {
                    // Set default values for dynamic fields
                    window.miningSearch.selectedMaterials = new Set(['Default']);
                    window.miningSearch.updateSelectedMaterials();
                    window.miningSearch.selectedMiningTypes = new Set(['All']);
                    window.miningSearch.updateSelectedMiningTypes();
                    window.selectedSystemStates = new Set(['Any']);
                    if (typeof updateSelectedSystemStates === 'function') {
                        updateSelectedSystemStates();
                    }
                }

                // Update power selection if handler exists
                if (window.miningSearch.handlePowerSelection) {
                    window.miningSearch.handlePowerSelection();
                }
            }
        }, 50);

        // Clear the interval after 5 seconds to prevent infinite checking
        setTimeout(() => clearInterval(waitForMiningSearch), 5000);
    }

    clearStorage() {
        localStorage.removeItem(this.storageKey);
        
        // Reset form to use HTML defaults
        const form = document.getElementById('searchForm');
        if (form) {
            form.reset();
            
            // Update slider gradients
            const distanceSlider = document.getElementById('distance');
            const limitSlider = document.getElementById('limit');
            
            if (distanceSlider) {
                const percentage = (distanceSlider.value / distanceSlider.max) * 100;
                distanceSlider.style.setProperty('--slider-percentage', `${percentage}%`);
            }
            if (limitSlider) {
                const percentage = (limitSlider.value / limitSlider.max) * 100;
                limitSlider.style.setProperty('--slider-percentage', `${percentage}%`);
            }
        }

        // Set default values for dynamic fields
        if (window.miningSearch) {
            window.miningSearch.selectedMaterials = new Set(['Default']);
            window.miningSearch.updateSelectedMaterials();
            window.miningSearch.selectedMiningTypes = new Set(['All']);
            window.miningSearch.updateSelectedMiningTypes();
        }
        
        window.selectedSystemStates = new Set(['Any']);
        if (typeof updateSelectedSystemStates === 'function') {
            updateSelectedSystemStates();
        }

        // Update power selection if handler exists
        if (window.miningSearch?.handlePowerSelection) {
            window.miningSearch.handlePowerSelection();
        }
    }
}

const formStorage = new FormStorage();
window.formStorage = formStorage;
export default formStorage; 