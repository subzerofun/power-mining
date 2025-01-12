// Form storage handling
class FormStorage {
    constructor() {
        this.storageKey = 'searchFormValues';
    }

    saveFormValues(miningSearch) {
        const values = {};
        const form = document.getElementById('searchForm');
        
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
                values[element.name] = element.value;
            }
        });

        // Save selected materials
        values.selectedMaterials = Array.from(miningSearch.selectedMaterials);

        // Save selected mining types
        values.selectedMiningTypes = Array.from(miningSearch.selectedMiningTypes);

        localStorage.setItem(this.storageKey, JSON.stringify(values));
    }

    loadSavedFormValues() {
        const savedValues = localStorage.getItem(this.storageKey);
        if (!savedValues) return;

        const values = JSON.parse(savedValues);

        // Restore form field values
        Object.entries(values).forEach(([name, value]) => {
            if (name === 'selectedMaterials') {
                miningSearch.selectedMaterials = new Set(value);
                miningSearch.updateSelectedMaterials();
            } else if (name === 'selectedMiningTypes') {
                miningSearch.selectedMiningTypes = new Set(value);
                miningSearch.updateSelectedMiningTypes();
            } else {
                const elements = document.getElementsByName(name);
                elements.forEach(element => {
                    if (element.type === 'checkbox') {
                        element.checked = value.includes(element.value);
                    } else {
                        element.value = value;
                    }
                });
            }
        });
    }

    clearStorage() {
        localStorage.removeItem(this.storageKey);
    }
}

const formStorage = new FormStorage();
export default formStorage; 