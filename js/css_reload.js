        // CSS Live Reload Configuration
        const LIVE_CSS = false; // Toggle CSS live reload
        let lastReloadTime = Date.now();

        if (LIVE_CSS) {
            // Function to extract filename from path
            function getFilename(path) {
                return path.split('/').pop();
            }

            // Function to reload a specific CSS file
            function reloadCSS(link) {
                const href = link.href;
                // Create new link element
                const newLink = document.createElement('link');
                newLink.rel = 'stylesheet';
                newLink.href = href.split('?')[0] + '?v=' + Date.now();
                
                // Insert new before removing old
                link.parentNode.insertBefore(newLink, link);
                
                // Remove old link after new one loads
                newLink.onload = () => {
                    link.parentNode.removeChild(link);
                    console.log(`Reloaded CSS: ${getFilename(href)}`);
                };
            }

            // Check for CSS changes
            setInterval(() => {
                // Don't check too frequently
                if (Date.now() - lastReloadTime < 100) return;

                // Get all CSS links
                const cssLinks = document.querySelectorAll('link[rel="stylesheet"]');
                cssLinks.forEach(link => {
                    if (!link.href.includes('/css/')) return;

                    fetch(link.href + '?check=' + Date.now(), { method: 'HEAD' })
                        .then(response => {
                            const lastMod = response.headers.get('last-modified');
                            if (lastMod) {
                                const lastModTime = new Date(lastMod).getTime();
                                if (lastModTime > lastReloadTime) {
                                    reloadCSS(link);
                                    lastReloadTime = Date.now();
                                }
                            }
                        })
                        .catch(err => {
                            // Fallback: reload CSS every time file is saved
                            reloadCSS(link);
                            lastReloadTime = Date.now();
                        });
                });
            }, 1000); // Check every second
        }