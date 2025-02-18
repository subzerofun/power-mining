// Track system searches in GA via backend
export function trackSystemSearch(system, interaction_type = 'click') {
    if (!system) return;
    
    // Use the map endpoint for tracking
    fetch('/api/track_search', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            ref_system: system.name,
            controlling_power: system.controllingPower || 'Any',
            power_state: system.powerState || 'None',
            system_state: system.systemState || 'None',
            display_format: 'map',
            interaction_type: interaction_type
        })
    }).catch(error => {
        console.debug('Analytics event failed:', error);
    });
}

// Helper function to validate UUID format
function isValidUUID(uuid) {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(uuid);
}

// Helper function to get or create session ID
function getSessionId() {
    let sessionId = sessionStorage.getItem('ga_session_id');
    if (!sessionId) {
        sessionId = new Date().getTime() + '.' + Math.random().toString(36).substring(2);
        sessionStorage.setItem('ga_session_id', sessionId);
    }
    return sessionId;
}
