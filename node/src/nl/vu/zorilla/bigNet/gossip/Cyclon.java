package nl.vu.zorilla.bigNet.gossip;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.util.List;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.bigNet.NodeInfo;

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

    private final boolean useTcp;

    private final boolean retry;

    private final GossipService service;

    Cyclon(String name, boolean useTcp, boolean retry,
            boolean useFallbackCache, GossipService service) {
        this.name = name;
        this.useTcp = useTcp;
        this.retry = retry;
        this.service = service;

        cache = new GossipCache();

        if (useFallbackCache) {
            fallbackCache = new GossipCache();
        } else {
            fallbackCache = null;
        }

    }

    public void start() {
        ThreadPool.createNew(this, name);
    }

    /**
     * Handles a request, makes up a reply.
     */
    public synchronized GossipMessage handleRequest(GossipMessage request) {
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
        List<GossipCacheEntry> sendEntries;
        GossipMessage request;

        synchronized (this) {
            if (peer.getInfo().sameNodeAs(self)) {
                // reset "self" to 0
                cache.add(new GossipCacheEntry(self));
                return;
            }

            sendEntries = cache.selectRandomEntries(SWAP_SIZE - 1);
            sendEntries.add(new GossipCacheEntry(self));

            // send out request, waits for reply. Will throw an exception
            // if nothing is received.
            request = new GossipMessage(self, peer.getInfo(), sendEntries,
                    true, name);
        }

        GossipMessage reply;
        if (useTcp) {
            reply = service.doTcpRequest(request, timeout);
        } else {
            reply = service.doUdpRequest(request, timeout);
        }

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
                        logger.debug("adding entry from bootstrap cache: "
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
                service.getStats().reportGossipSuccess(name);
                if (fallbackCache != null) {
                    fallbackCache.replace(peer, FALLBACK_CACHE_SIZE);
                }
            } catch (IOException e) {
                logger.info("gossiping from  " + service.getNodeInfo() + " to "
                        + peer + " failed, removing from cache");
                logger.debug("gossiping from  " + service.getNodeInfo()
                        + " to " + peer + " failed, removing from cache", e);
                cache.remove(peer);

                if (retry) {
                    if (fallbackCache != null) {
                        peer = fallbackCache.selectRandomEntry();
                    } else {
                        peer = cache.removeOldest();
                    }

                    if (peer != null) {
                        try {
                            doGossip(peer, timeout);
                            service.getStats().reportGossipSuccess(name);
                        } catch (IOException e2) {
                            logger.info("RETRY: gossiping from  "
                                    + service.getNodeInfo() + " to " + peer
                                    + " failed, removing from cache");
                            logger.debug("RETRY: gossiping from  "
                                    + service.getNodeInfo() + " to " + peer
                                    + " failed, removing from cache", e2);
                            cache.remove(peer);

                            service.getStats().reportGossipFailure(name);
                        }
                    }
                } else {
                    service.getStats().reportGossipFailure(name);
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

    public NodeInfo[] getRandom(int n) {
        return cache.selectRandom(n);
    }

    public String getName() {
        return name;
    }

    public String getStatus() {
        if (fallbackCache == null) {
            return name + " cache @ " + service.getNodeInfo() + ":\n"
                    + cache.toNewLinedString();
        } else {
            return name + " cache @ " + service.getNodeInfo() + ":\n"
                    + cache.toNewLinedString() + "Fallback cache:\n"
                    + fallbackCache.toNewLinedString();
        }
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
                if (logger.isInfoEnabled()) {
                    logger.info(getStatus());
                }
            }
        }
    }
}
