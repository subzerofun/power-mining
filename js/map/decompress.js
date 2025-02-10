// Constants
export const USE_LOCAL_STORAGE = true;  // Toggle local storage functionality
export const STORAGE_KEY = 'powerplay_data';
export const STORAGE_META_KEY = 'powerplay_meta';

// Check if local storage is available and has enough space
export function checkStorageAvailability() {
    try {
        // Test with a small amount of data first
        localStorage.setItem('test', 'test');
        localStorage.removeItem('test');
        
        // Check available space (rough estimate)
        let totalSpace = 0;
        for (let key in localStorage) {
            if (localStorage.hasOwnProperty(key)) {
                totalSpace += (localStorage[key].length * 2) / 1024 / 1024; // MB
            }
        }
        
        // Most browsers allow 5-10MB per domain. We'll be conservative and check for 20MB free
        const estimatedFreeSpace = 20 - totalSpace;
        return estimatedFreeSpace >= 15; // Ensure we have at least 15MB free
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
                
                // Fetch only the headers to get file size
                const response = await fetch(url, { method: 'HEAD' });
                const currentSize = response.headers.get('content-length');
                
                // If sizes match and data exists, use stored data
                if (meta.size === currentSize) {
                    console.log('Using cached data from local storage');
                    return JSON.parse(storedData);
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
                const meta = {
                    size: response.headers.get('content-length'),
                    timestamp: new Date().toISOString()
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