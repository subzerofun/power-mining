// EDDN Status WebSocket
const ws = new WebSocket('ws://' + window.location.hostname + ':8765');
let eddnCircle = document.querySelector('.eddn-circle');
let eddnText = document.querySelector('.eddn-text');
let dailyCircle = document.querySelector('.daily-circle');
let dailyText = document.querySelector('.daily-text');

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    const eddn = data.eddn;
    const daily = data.daily;
    
    // Update EDDN status
    eddnCircle.classList.remove('running', 'error', 'updating', 'starting');
    switch(eddn.state) {
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
    
    // Update Daily status
    dailyCircle.classList.remove('running', 'error', 'updating', 'downloading', 'extracting');
    switch(daily.state) {
        case 'downloading':
            dailyCircle.classList.add('downloading');
            dailyText.textContent = `${daily.progress}% Downloading`;
            break;
        case 'extracting':
            dailyCircle.classList.add('extracting');
            dailyText.textContent = `${daily.progress}% Extracting`;
            break;
        case 'processing':
            dailyCircle.classList.add('updating');
            if (daily.total > 0) {
                const percent = Math.round((daily.progress / daily.total) * 100);
                dailyText.textContent = `${percent}% Processing`;
            } else {
                dailyText.textContent = 'Processing';
            }
            break;
        case 'error':
            dailyCircle.classList.add('error');
            dailyText.textContent = daily.message || 'Error';
            break;
        case 'updated':
            dailyCircle.classList.add('running');
            dailyText.textContent = `EDDN data: ${daily.last_update}`;
            break;
        case 'offline':
            if (daily.last_update) {
                dailyCircle.classList.add('running');
                dailyText.textContent = `EDDN data: ${daily.last_update}`;
            } else {
                dailyText.textContent = 'Not Updated';
            }
            break;
        default:
            dailyText.textContent = daily.message || 'Unknown';
            break;
    }
};

ws.onerror = function(error) {
    // Handle WebSocket error
    eddnCircle.classList.remove('running', 'error', 'updating');
    eddnCircle.classList.add('error');
    eddnText.textContent = 'Connection error';
    
    dailyCircle.classList.remove('running', 'error', 'updating', 'downloading', 'extracting');
    dailyCircle.classList.add('error');
    dailyText.textContent = 'Connection error';
}; 
