package nl.vu.zorilla.gossip;

import ibis.util.ThreadPool;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.NodeInfo;

import org.apache.log4j.Logger;

class Cyclon implements GossipAlgorithm, Runnable {

    public static final int NEW_NODE_TIMEOUT = 10 * 1000;

    private static final int CACHE_SIZE = 10;

    private static final int FALLBACK_CACHE_SIZE = 10;

    private static final int SWAP_SIZE = 3;

    private static final Logger logger = Logger.getLogger(Cyclon.class);

    private final GossipCache cache;

    private final GossipCache fallbackCache;

    private final String name;

    private final boolean retry;

    private final GossipService service;

    private final Stats stats;

    Cyclon(String name, boolean retry, boolean useFallbackCache,
            GossipService service, File statsDir, UUID self) {
        this.name = name;
        this.retry = retry;
        this.service = service;

        cache = new GossipCache(self);

        if (useFallbackCache) {
            fallbackCache = new GossipCache(self);
        } else {
            fallbackCache = null;
        }

        stats = new Stats(statsDir, name);
    }

    public void start() {
        ThreadPool.createNew(this, name);
        logger.info("Started " + name + " Gossip service");
    }

    /**
     * Handles a request, makes up a reply.
     */
    public synchronized GossipMessage handleRequest(GossipMessage request) {
        stats.addToStats(request);
        List<GossipCacheEntry> replyEntries = cache
                .selectRandomEntries(SWAP_SIZE);

        cache.add(request.getEntries());

        // remove entries we send if needed.
        cache.purgeDownTo(replyEntries, CACHE_SIZE);

        return new GossipMessage(service.getNodeInfo(), request.getSender(),
                replyEntries, true, name);
    }

    private void doGossip(GossipCacheEntry peer, int timeout)
            throws IOException {
        NodeInfo self = service.getNodeInfo();

        if (peer.getInfo().sameNodeAs(self)) {
            // reset "self" to 0
            cache.add(new GossipCacheEntry(self));
            return;
        }

        List<GossipCacheEntry> sendEntries = cache
                .selectRandomEntries(SWAP_SIZE - 1);
        sendEntries.add(new GossipCacheEntry(self));

        // send out request, waits for reply. Will throw an exception
        // if nothing is received.
        GossipMessage request = new GossipMessage(self, peer.getInfo(),
                sendEntries, true, name);

        GossipMessage reply;
        reply = service.doTcpRequest(request, timeout);
        stats.addToStats(reply);

        synchronized (this) {

            cache.add(reply.getEntries());

            // remove send out entries
            cache.purgeDownTo(sendEntries, CACHE_SIZE);

            // if space left, add (updated entry of) peer again
            if (cache.cacheSize() < CACHE_SIZE) {
                cache.remove(peer);
                cache.add(new GossipCacheEntry(reply.getSender()));
            }

        }

    }

    public void doGossip(int timeout) {
        cache.incrementEntries();

        synchronized (this) {
            if (cache.size() < CACHE_SIZE) {
                NodeInfo bootstrap = service.getBootstrapNode();
                if (bootstrap != null) {
                    GossipCacheEntry entry = new GossipCacheEntry(bootstrap);
                    if (!cache.contains(entry)) {
                        logger.debug(name + ": adding entry from bootstrap cache: "
                                + bootstrap);
                        cache.add(entry);
                    }
                }
            }
        }

        GossipCacheEntry peer = cache.removeOldest();

        if (retry) {
            // reserve time for retry;
            timeout = timeout / 2;
        }

        if (peer != null) {
            try {
                doGossip(peer, timeout);
                if (fallbackCache != null) {
                    fallbackCache.replace(peer, FALLBACK_CACHE_SIZE);
                }
                return;
            } catch (IOException e) {
                logger.info(name + ": gossiping from  " + service.getNodeInfo() + " to "
                        + peer + " failed, removing from cache");
                logger.debug(name + ": gossiping from  " + service.getNodeInfo()
                        + " to " + peer + " failed, removing from cache", e);
                cache.remove(peer);
            }
        }

        //second try (or first if the original cache was empty)
        if (retry) {
            if (fallbackCache != null) {
                peer = fallbackCache.selectRandomEntry();
                logger.info(name + ": retrying gossip with " + peer + " from the fallback cache");
            } else {
                peer = cache.removeOldest();
                logger.info(name + ": retrying gossip with " + peer + " from the normal cache");
            }

            if (peer != null) {
                try {
                    doGossip(peer, timeout);
                } catch (IOException e2) {
                    logger
                            .info(name + ": retry, gossiping from  "
                                    + service.getNodeInfo() + " to " + peer
                                    + " failed");
                    logger.debug(name + 
                            ": retry, gossiping from  " + service.getNodeInfo()
                                    + " to " + peer + " failed", e2);
                    cache.remove(peer);
                }
            }
        }
    }

    public void newNode(NodeInfo info) {
        GossipCacheEntry entry = new GossipCacheEntry(info);
        cache.add(entry);
        try {
            doGossip(entry, NEW_NODE_TIMEOUT);
        } catch (IOException e) {
            logger.warn("could not gossip with new node: " + info);
        }
    }

    public NodeInfo[] getNodes() {
        return cache.getNodes();
    }

    public NodeInfo[] getRandomNodes(int n) {
        return cache.selectRandom(n);
    }

    public NodeInfo getRandomNode() {
        return cache.selectRandom();
    }

    public String getName() {
        return name;
    }

    public void run() {
        for (int i = 0; true; i++) {
            int timeout = Node.randomTimeout(GossipService.TIMEOUT);
            long deadline = System.currentTimeMillis() + timeout;

            doGossip(timeout - GossipService.TIMEOUT_MARGIN);

            long remaining = deadline - System.currentTimeMillis();
            if (remaining > 0) {
                try {
                    Thread.sleep(remaining);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            } else {
                logger.warn("gossips took " + remaining / -1000.0
                        + " seconds too long");
            }
            if (i % 10 == 0) {
                logger.info(name + " cache:\n" + cache.toNewLinedString());
                if (fallbackCache != null) {
                    logger.info(name + " fallback cache:\n" + fallbackCache.toNewLinedString());
                }
            }
        }
    }

    public synchronized NodeInfo[] getFallbackNodes() {
        if (fallbackCache == null) {
            return new NodeInfo[0];
        } else {
            return fallbackCache.getNodes();
        }
    }

    public Stats getStats() {
        return stats;
    }

}
