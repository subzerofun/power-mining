// Create WebSocket connection
const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsHost = window.location.host;  // Use host instead of hostname to include port if any
const wsUrl = `${protocol}//${wsHost}/ws`;

const ws = new WebSocket(wsUrl);
let eddnCircle = document.querySelector('.eddn-circle');
let eddnText = document.querySelector('.eddn-text');

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    
    // Update EDDN status
    eddnCircle.classList.remove('running', 'error', 'updating', 'starting');
    switch(data.state) {
        case 'starting':
            eddnCircle.classList.add('starting');
            eddnText.textContent = 'Starting';
            break;
        case 'running':
            eddnCircle.classList.add('running');
            eddnText.textContent = 'Running';
            break;
        case 'error':
            eddnCircle.classList.add('error');
            eddnText.textContent = 'Connection error';
            break;
        case 'updating':
            eddnCircle.classList.add('updating');
            eddnText.textContent = 'Updating';
            break;
        case 'offline':
        default:
            eddnText.textContent = 'Offline';
            break;
    }
};

ws.onerror = function(error) {
    // Handle WebSocket error
    eddnCircle.classList.remove('running', 'error', 'updating');
    eddnCircle.classList.add('error');
    eddnText.textContent = 'Connection error';
}; 
