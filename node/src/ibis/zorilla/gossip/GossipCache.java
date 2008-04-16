package ibis.zorilla.gossip;

import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

final class GossipCache {

    private ArrayList<GossipCacheEntry> cache;

    private Random random;

    private final UUID self;

    public GossipCache(UUID self) {
        cache = new ArrayList<GossipCacheEntry>();
        this.self = self;

        random = new Random();
    }

    public synchronized NodeInfo[] getNodes() {
        NodeInfo[] result = new NodeInfo[cache.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = cache.get(i).getInfo();
        }
        return result;
    }

    public synchronized NodeInfo selectRandom() {
        if (cache.size() == 0) {
            return null;
        }
        return cache.get(random.nextInt(cache.size())).getInfo();
    }

    protected synchronized GossipCacheEntry selectRandomEntry() {
        if (cache.size() == 0) {
            return null;
        }
        return cache.get(random.nextInt(cache.size()));
    }

    protected synchronized List<GossipCacheEntry> selectRandomEntries(int n) {
        List<GossipCacheEntry> result = new ArrayList<GossipCacheEntry>();
        BitSet selected = new BitSet();

        if (n > cache.size()) {
            n = cache.size();
        }

        for (int i = 0; i < n; i++) {
            int next;
            do {
                next = random.nextInt(cache.size());
            } while (selected.get(next));

            selected.set(next);
            result.add(cache.get(next));
        }

        return result;
    }

    public synchronized NodeInfo[] selectRandom(int n) {
        BitSet selected = new BitSet();

        if (n > cache.size()) {
            n = cache.size();
        }

        NodeInfo[] result = new NodeInfo[n];

        for (int i = 0; i < n; i++) {
            int next;
            do {
                next = random.nextInt(cache.size());
            } while (selected.get(next));

            selected.set(next);
            result[i] = cache.get(next).getInfo();
        }

        return result;
    }

    protected synchronized GossipCacheEntry removeOldest() {
        int oldest = -1;

        for (int i = 0; i < cache.size(); i++) {
            if (oldest == -1
                    || cache.get(i).getAge() > cache.get(oldest).getAge()) {
                oldest = i;
            }
        }

        if (oldest != -1) {
            GossipCacheEntry result = cache.get(oldest);
            cache.remove(oldest);
            return result;
        }
        return null;
    }

    protected synchronized GossipCacheEntry removeRandom() {
        return cache.remove(random.nextInt(cache.size()));
    }

    protected synchronized void add(GossipCacheEntry entry) {
        if (entry.getInfo().getID().equals(self)) {
            // do not add ourselves to cache
            return;
        }
        cache.add(entry);
        cleanup();
    }

    protected synchronized void add(List<GossipCacheEntry> entries) {
        for (GossipCacheEntry entry : entries) {
            if (!entry.getInfo().getID().equals(self)) {
                cache.add(entry);
            }
        }
        cleanup();
    }

    protected synchronized void incrementEntries() {
        for (GossipCacheEntry entry : cache) {
            entry.incAge();
        }
    }

    protected synchronized int cacheSize() {
        return cache.size();
    }

    /**
     * remove duplicates and expired entries
     */
    private synchronized void cleanup() {
        for (int i = 0; i < cache.size(); i++) {
            for (int j = i + 1; j < cache.size(); j++) {
                if (cache.get(i).sameNodeAs(cache.get(j))) {
                    if (cache.get(j).getInfo().newer(cache.get(i).getInfo())) {
                        cache.set(i, cache.get(j));
                    }
                    cache.remove(j);
                    j--;
                }
            }
        }

        long version = Node.getVersion();
        for (int i = 0; i < cache.size(); i++) {
            if (cache.get(i).hasExpired() || version != cache.get(i).getInfo().getVersion()) {
                cache.remove(i);
                i--;
            }
        }

    }

    protected synchronized void remove(GossipCacheEntry entry) {
        for (int i = 0; i < cache.size(); i++) {
            if (cache.get(i).sameNodeAs(entry)) {
                cache.remove(i);
                // go back one entry
                i--;
            }
        }
    }

    protected synchronized boolean contains(GossipCacheEntry entry) {
        for (int i = 0; i < cache.size(); i++) {
            if (cache.get(i).sameNodeAs(entry)) {
                return true;
            }
        }
        return false;
    }

    protected synchronized void purgeDownTo(List<GossipCacheEntry> victims,
            int size) {
        for (GossipCacheEntry victim : victims) {
            if (cache.size() <= size) {
                return;
            }
            remove(victim);
        }
    }

    public synchronized String toNewLinedString() {
        String result = "";
        for (GossipCacheEntry entry : cache) {
            result += entry.toString() + "\n";
        }

        return result;
    }

    public synchronized void replace(GossipCacheEntry peer, int maximum_size) {
        for (int i = 0; i < cache.size(); i++) {
            if (cache.get(i).sameNodeAs(peer)) {
                cache.set(i, peer);
                return;
            }
        }

        // add new entry to the back of the list
        cache.add(peer);

        // remove some entries until we reach the desired size
        // (but NOT the last one we just added)
        while (cache.size() > maximum_size) {
            cache.remove(random.nextInt(cache.size() - 1)).getInfo();
        }
    }

    public synchronized void clear() {
        cache.clear();
    }

    public synchronized int size() {
        return cache.size();
    }

}
