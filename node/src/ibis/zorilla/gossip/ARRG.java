package ibis.zorilla.gossip;

import ibis.util.ThreadPool;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;


import org.apache.log4j.Logger;

class ARRG implements GossipAlgorithm, Runnable {

    private static final Logger logger = Logger.getLogger(ARRG.class);

    private final GossipCache cache;

    private final GossipCache fallbackCache;
    
    private final int cacheSize;
    
    private final int sendSize;
    
    private final int interval;

    private final String name;

    private final boolean retry;

    private final GossipService service;

    private final Stats stats;

    public ARRG(String name, boolean retry, boolean useFallbackCache, int cacheSize, int sendSize, int interval,
            GossipService service, File statsDir, UUID self) {
        this.name = name;
        this.retry = retry;
        this.service = service;
        this.cacheSize = cacheSize;
        this.sendSize = sendSize;
        this.interval = interval;

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
                .selectRandomEntries(sendSize - 1);

        entries.add(new GossipCacheEntry(self));

        GossipMessage request = new GossipMessage(self, peer.getInfo(),
                entries, true, name);

        GossipMessage reply;

        reply = service.doTcpRequest(request, timeout);
        stats.addToStats(reply);

        synchronized (this) {
            cache.add(reply.getEntries());

            while (cache.cacheSize() > cacheSize) {
                cache.removeRandom();
            }
        }
        return true;
    }

    public void doGossip(int timeout) {

        synchronized (this) {

            if (cache.size() < cacheSize) {
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

        GossipCacheEntry peer = cache.selectRandomEntry();

        if (retry) {
            // reserve some time for retry as well
            timeout = timeout / 2;
        }

        try {

            if (doGossip(peer, timeout) && fallbackCache != null) {
                fallbackCache.replace(peer, cacheSize);
            }

        } catch (IOException e) {
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
                    logger.debug(
                            "RETRY: gossiping from  " + service.getNodeInfo()
                                    + " to " + peer + " failed", e);
                }
            }
        }

    }

    public synchronized GossipMessage handleRequest(GossipMessage request) {
        stats.addToStats(request);
        List<GossipCacheEntry> replyEntries = cache
                .selectRandomEntries(sendSize - 1);
        NodeInfo self = service.getNodeInfo();

        replyEntries.add(new GossipCacheEntry(self));

        cache.add(request.getEntries());

        while (cache.cacheSize() > cacheSize) {
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
        while (true) {
            int timeout = Node.randomTimeout(interval);
            long deadline = System.currentTimeMillis() + timeout;

            doGossip(timeout);

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
