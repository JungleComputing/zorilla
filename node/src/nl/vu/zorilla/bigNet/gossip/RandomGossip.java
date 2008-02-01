package nl.vu.zorilla.bigNet.gossip;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.util.List;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.bigNet.NodeInfo;

import org.apache.log4j.Logger;

class RandomGossip implements GossipAlgorithm, Runnable {

    public static final int CACHE_SIZE = 10;

    public static final int FALLBACK_CACHE_SIZE = 10;

    public static final int SEND_LIST_SIZE = 3;

    private static final Logger logger = Logger.getLogger(RandomGossip.class);

    private final GossipCache cache;

    private final GossipCache fallbackCache;

    private final String name;

    private final boolean useTcp;

    private final boolean retry;

    private final GossipService service;

    public RandomGossip(String name, boolean useTcp, boolean retry,
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

    private boolean doGossip(GossipCacheEntry peer, int timeout)
            throws IOException {
        NodeInfo self = service.getNodeInfo();

        if (peer == null) {
            logger.debug("noone to gossip with");
            return false;
        }

        if (peer.getInfo().sameNodeAs(self)) {
            logger.debug("selected peer is us, not gossiping");
            return false;
        }

        List<GossipCacheEntry> entries = cache
                .selectRandomEntries(SEND_LIST_SIZE - 1);

        entries.add(new GossipCacheEntry(self));

        GossipMessage request = new GossipMessage(self, peer.getInfo(),
                entries, true, name);

        GossipMessage reply;

        if (useTcp) {
            reply = service.doTcpRequest(request, timeout);
        } else {
            reply = service.doUdpRequest(request, timeout);
        }

        synchronized (this) {
            cache.add(reply.getEntries());

            while (cache.cacheSize() > CACHE_SIZE) {
                cache.removeRandom();
            }
        }
        service.getStats().reportGossipSuccess(name);
        return true;
    }

    public void doGossip(int timeout) {

        synchronized (this) {

            if (cache.size() < CACHE_SIZE) {
                NodeInfo bootstrap = service.getBootstrapNode();
                if (bootstrap != null) {
                    GossipCacheEntry entry = new GossipCacheEntry(bootstrap);
                    if (!cache.contains(entry)) {
                        logger.debug("adding entry from bootstrap cache: " + bootstrap); 
                        cache.add(entry);
                    }
                }
            }
        }

        GossipCacheEntry peer = cache.selectRandomEntry();

        if (retry) {
            // reserve some time for retry as well
            timeout = timeout / 2;
        }

        try {

            if (doGossip(peer, timeout) && fallbackCache != null) {
                fallbackCache.replace(peer, FALLBACK_CACHE_SIZE);
            }

        } catch (IOException e) {
            logger.info("gossiping from  " + service.getNodeInfo() + " to "
                    + peer + " failed");
            logger.debug("gossiping from  " + service.getNodeInfo() + " to "
                    + peer + " failed", e);

            if (retry) {
                if (fallbackCache == null) {
                    peer = cache.selectRandomEntry();
                } else {
                    peer = fallbackCache.selectRandomEntry();
                }

                try {
                    doGossip(peer, timeout);
                } catch (IOException e2) {
                    logger
                            .info("RETRY: gossiping from  "
                                    + service.getNodeInfo() + " to " + peer
                                    + " failed");
                    logger.debug(
                            "RETRY: gossiping from  " + service.getNodeInfo()
                                    + " to " + peer + " failed", e);
                    service.getStats().reportGossipFailure(name);
                }

            } else {
                service.getStats().reportGossipFailure(name);
            }
        }

    }

    public synchronized GossipMessage handleRequest(GossipMessage request) {
        List<GossipCacheEntry> replyEntries = cache
                .selectRandomEntries(SEND_LIST_SIZE - 1);
        NodeInfo self = service.getNodeInfo();

        replyEntries.add(new GossipCacheEntry(self));

        cache.add(request.getEntries());

        while (cache.cacheSize() > CACHE_SIZE) {
            cache.removeRandom();
        }

        return new GossipMessage(service.getNodeInfo(), request.getSender(),
                replyEntries, true, name);
    }

    public void newNode(NodeInfo info) {
        cache.add(new GossipCacheEntry(info));
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
            return name + " cache @ " + service.getNodeInfo() + "\n:"
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
