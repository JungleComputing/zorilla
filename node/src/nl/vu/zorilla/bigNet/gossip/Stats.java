package nl.vu.zorilla.bigNet.gossip;

import ibis.util.ThreadPool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import vu.platform934.analysis.GossipCharts;

class Stats implements Runnable {

    // add statistics to charts every X milliseconds
    public static final int TIMEOUT = 5 * 60  * 1000;

    private final Logger logger = Logger.getLogger(Stats.class);

    private final Map<String, GossipCharts<UUID>> chartss;

    private final File homeDir;

    private final String[] algorithms;

    private static class Starter implements Runnable {
        private long delay;

        private Stats stats;

        Starter(Stats stats, long delay) {
            this.stats = stats;
            this.delay = delay;

            ThreadPool.createNew(this, "stats starter");
        }

        public void run() {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // IGNORE
            }
            stats.start();
        }
    }

    Stats(File homeDir, Set<String> algorithms, long delay) {
        this.homeDir = homeDir;
        this.algorithms = algorithms.toArray(new String[0]);

        chartss = new HashMap<String, GossipCharts<UUID>>();

        // negative delay will make stats not start at all
        if (delay == 0) {
            start();
        } else if (delay > 0) {
            new Starter(this, delay);
        }

    }

    synchronized void start() {
        for (String algorithmName : algorithms) {
            chartss.put(algorithmName, new GossipCharts<UUID>(algorithmName));
        }
        logger.info("starting to gather statistics");
        ThreadPool.createNew(this, "gossip stats");
    }

    synchronized void addTimeBasedStats() {
        for (GossipCharts<UUID> charts : chartss.values()) {
            charts.addTimeBasedData();
        }
    }

    synchronized void addToStats(GossipMessage message) {
        String algorithm = message.getAlgorithmName();
        GossipCharts<UUID> charts = chartss.get(algorithm);

        if (charts == null) {
            return;
        }

        logger.debug("adding " + message + " to stats");

        ArrayList<UUID> ids = new ArrayList<UUID>();

        for (GossipCacheEntry entry : message.getEntries()) {
            ids.add(entry.getInfo().getID());
        }

        charts.giveNextGossipItems(ids);

    }

    private void delete(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] list = file.listFiles();
            for (File child : list) {
                delete(child);
            }
        }
        file.delete();
    }

    synchronized void printCharts() {
        long start = System.currentTimeMillis();

        List<GossipCharts> list = new LinkedList<GossipCharts>();
        for (GossipCharts<UUID> charts : chartss.values()) {
            list.add(charts);
        }

        File oldStats = new File(homeDir, "stats.prev");
        File newStats = new File(homeDir, "stats");

        delete(oldStats);
        newStats.renameTo(oldStats);
        newStats.mkdirs();

        GossipCharts.printCombinedCharts(newStats, "total", list);

        logger.info("printing stats took "
                + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
    }

    public synchronized void reportGossipFailure(String algorithm) {
        GossipCharts<UUID> charts = chartss.get(algorithm);

        if (charts == null) {
            return;
        }

        charts.reportFailure();
    }

    public synchronized void reportGossipSuccess(String algorithm) {
        GossipCharts<UUID> charts = chartss.get(algorithm);

        if (charts == null) {
            return;
        }

        charts.reportSuccess();
    }

    public synchronized void run() {
        while (true) {
            try {
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                // IGNORE
            }

            for (GossipCharts<UUID> charts : chartss.values()) {
                charts.addTimeBasedData();
            }
        }
    }

}
