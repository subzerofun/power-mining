// Web Worker for handling system loading and processing
let loadedChunks = new Set();
const CHUNK_SIZE = 100;

self.onmessage = function(e) {
    const { type, data } = e.data;
    
    if (type === 'loadChunk') {
        const { chunkKey, x, y, z } = data;
        
        if (!loadedChunks.has(chunkKey)) {
            loadedChunks.add(chunkKey);
            
            fetch(`/api/systems?chunk=${chunkKey}`)
                .then(response => response.json())
                .then(systems => {
                    self.postMessage({
                        type: 'chunkLoaded',
                        data: {
                            chunkKey,
                            systems
                        }
                    });
                })
                .catch(error => {
                    console.error('Error loading systems:', error);
                    loadedChunks.delete(chunkKey);
                });
        }
    } else if (type === 'clearChunks') {
        loadedChunks.clear();
    }
}; 