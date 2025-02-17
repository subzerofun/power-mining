// Constants
export const USE_LOCAL_STORAGE = true;  // Toggle local storage functionality
export const STORAGE_KEY = 'powerplay_data';
export const STORAGE_META_KEY = 'powerplay_meta';

// Check if local storage is available and has enough space
export function checkStorageAvailability() {
    try {
        // Test if we can write to localStorage at all
        localStorage.setItem('test', 'test');
        localStorage.removeItem('test');
        
        // Log current storage usage for debugging
        let totalSpace = 0;
        for (let key in localStorage) {
            if (localStorage.hasOwnProperty(key)) {
                totalSpace += (localStorage[key].length * 2) / 1024 / 1024; // MB
            }
        }
        
        console.log('%c Storage Space Check:', 'background: #222; color: #bada55; font-size: 14px;', {
            totalSpaceUsed: totalSpace.toFixed(2) + ' MB',
            powerplayData: localStorage.getItem(STORAGE_KEY) ? 'Present' : 'Not present',
            powerplayMeta: localStorage.getItem(STORAGE_META_KEY) ? 'Present' : 'Not present'
        });

        // If we can write to localStorage, we're good - we'll be replacing existing data
        return true;
        
    } catch (e) {
        console.warn('Local storage not available:', e);
        return false;
    }
}

// Get file size from gzip header
// gzip header format: see RFC 1952
export async function getUncompressedSize(gzipData) {
    // Last 4 bytes of gzip file contain the uncompressed size (modulo 2^32)
    const dv = new DataView(gzipData.slice(-4));
    return dv.getUint32(0, true); // true for little-endian
}

// Load and decompress data
export async function loadAndDecompress(url) {
    try {
        // Check if it's a .gz file
        const isGzipped = url.toLowerCase().endsWith('.gz');
        
        // Try to get from local storage first if enabled
        if (USE_LOCAL_STORAGE && checkStorageAvailability()) {
            const storedMeta = localStorage.getItem(STORAGE_META_KEY);
            const storedData = localStorage.getItem(STORAGE_KEY);
            
            if (storedMeta && storedData) {
                const meta = JSON.parse(storedMeta);
                console.log('Stored metadata:', meta);  // Log stored metadata
                
                // Fetch headers to get file size and last modified
                const response = await fetch(url, { method: 'HEAD' });
                const currentSize = response.headers.get('content-length');
                const lastModified = response.headers.get('last-modified');
                const serverTimestamp = lastModified ? new Date(lastModified).toISOString() : new Date().toISOString();
                
                console.log('Server data:', {  // Log server data
                    size: currentSize,
                    timestamp: serverTimestamp,
                    lastModified: lastModified
                });
                
                // Compare both size and timestamp
                const sizeMatches = meta.size === currentSize;
                const storedTime = new Date(meta.timestamp);
                const serverTime = new Date(serverTimestamp);
                const isNewer = serverTime > storedTime;
                
                console.log('Comparison:', {  // Log comparison details
                    sizeMatches,
                    storedTime: storedTime.toISOString(),
                    serverTime: serverTime.toISOString(),
                    isNewer,
                    timeDiff: serverTime - storedTime + ' ms'
                });
                
                // Use cached data only if size matches and server data isn't newer
                if (sizeMatches && !isNewer) {
                    console.log('Using cached data from local storage');
                    return JSON.parse(storedData);
                } else {
                    console.log('Server data is newer or size mismatch, fetching fresh data');
                    console.log('Reason:', !sizeMatches ? 'Size mismatch' : 'Server data is newer');
                }
            }
        }

        // Fetch the file
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        let data;
        if (isGzipped) {
            // Get the compressed data as ArrayBuffer
            const compressed = await response.arrayBuffer();
            
            // Get uncompressed size for verification
            const expectedSize = await getUncompressedSize(compressed);
            
            // Decompress using pako (needs to be included in your HTML)
            const decompressed = pako.inflate(new Uint8Array(compressed), { to: 'string' });
            
            // Verify the size matches
            if (decompressed.length !== expectedSize) {
                console.warn('Decompressed size mismatch! Expected:', expectedSize, 'Got:', decompressed.length);
            }
            
            data = JSON.parse(decompressed);
        } else {
            data = await response.json();
        }

        // Store in local storage if enabled and available
        if (USE_LOCAL_STORAGE && checkStorageAvailability()) {
            try {
                // Get the last-modified header for timestamp
                const lastModified = response.headers.get('last-modified');
                const meta = {
                    size: response.headers.get('content-length'),
                    timestamp: lastModified || new Date().toISOString()  // Use server timestamp if available
                };
                
                localStorage.setItem(STORAGE_META_KEY, JSON.stringify(meta));
                localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
                console.log('Data cached in local storage');
            } catch (e) {
                console.warn('Failed to cache in local storage:', e);
            }
        }

        return data;
    } catch (error) {
        console.error('Error loading/decompressing data:', error);
        throw error;
    }
}

// Clear cached data
export function clearCache() {
    if (USE_LOCAL_STORAGE) {
        localStorage.removeItem(STORAGE_KEY);
        localStorage.removeItem(STORAGE_META_KEY);
        console.log('Cache cleared');
    }
} 