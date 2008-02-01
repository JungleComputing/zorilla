package nl.vu.zorilla.gossip;

import ibis.util.ThreadPool;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import vu.platform934.analysis.Chart;

class Stats implements Runnable {

    public static final boolean PLOT_EVERY_UPDATE = false;

    // add statistics and print data every X milliseconds
    public static final int TIMEOUT = 60 * 1000;

    private final Logger logger = Logger.getLogger(Stats.class);

    // when did we last see a certain node
    private final Map<UUID, Long> lastSeenMap;

    private long intervalCount;

    private long totalIntervalTime;

    // current time in "seen items"
    private long itemCount;

    // total number of exchanges succesfully performed
    private long exchangeCount;

    private final ArrayList<DataPoint> plot;

    private final File statsDir;

    private final String name;

    private final long start;

    // private final GossipCharts<UUID> charts;

    Stats(File statsDir, String name) {

        this.statsDir = statsDir;
        this.name = name;

        lastSeenMap = new HashMap<UUID, Long>();
        intervalCount = 0;
        totalIntervalTime = 0;
        exchangeCount = 0;
        itemCount = 0;
        plot = new ArrayList<DataPoint>();
        plot.add(new DataPoint(0, 0, 0));

        start = System.currentTimeMillis();

        // charts = new GossipCharts<UUID>(name);

        ThreadPool.createNew(this, "gossip stats for " + name);
    }

    private synchronized void addEntry(GossipCacheEntry entry) {
        itemCount++;

        UUID id = entry.getInfo().getID();

        Long lastSeen = lastSeenMap.get(id);
        lastSeenMap.put(id, itemCount);

        if (lastSeen == null) {
            // new item
            return;
        }

        // update stats
        intervalCount++;
        totalIntervalTime += (itemCount - lastSeen);
    }

    synchronized void addToStats(GossipMessage message) {
        exchangeCount++;

        for (GossipCacheEntry entry : message.getEntries()) {
            addEntry(entry);
        }

        if (PLOT_EVERY_UPDATE) {
            addDataPoint();
        }
    }

    private synchronized void addDataPoint() {
        double pns;
        long now = System.currentTimeMillis();

        if (intervalCount == 0) {
            pns = 0;
        } else {
            pns = (double) totalIntervalTime / (double) intervalCount;
        }

        double time = (now - start) / 1000.0;

        plot.add(new DataPoint(time, pns, exchangeCount));
    }

    void save(File dir) {
        // charts.addTimeBasedData();
        // charts.printCharts(statsDir);
        try {

            File file = new File(dir, name + ".plot");
            if (file.exists()) {
                file.renameTo(new File(dir, name + ".old"));
            }
            PrintWriter writer = new PrintWriter(file);

            writer.println("stats for " + name + " at "
                    + new Date(System.currentTimeMillis()));
            writer.println("//  time    -  PNS    - #exchanges");
            writer.println("//(seconds)   (nodes)");

            synchronized (this) {
                for (DataPoint dataPoint : plot) {
                    writer.printf("%.4f %.4f %.0f\n", dataPoint.getTime(),
                            dataPoint.getPNS(), dataPoint.getExchangeCount());
                }
            }

            writer.close();
        } catch (Exception e) {
            logger.error("could not print statistics", e);
        }

    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(TIMEOUT);
            } catch (InterruptedException e) {
                // IGNORE
            }

            addDataPoint();
            save(statsDir);
        }
    }

    public synchronized double getPNS() {
        if (intervalCount == 0) {
            return 0;
        } else {
            return (double) totalIntervalTime / (double) intervalCount;
        }
    }

    public synchronized long getTotalExchanges() {
        return exchangeCount;
    }

    void addPnsData(Chart chart) {
        chart.addDataSeries(name);

        synchronized (this) {
            int step = plot.size() / 100;
            if (step < 1) {
                step = 1;
            }

            for (int i = 0; i < plot.size(); i += step) {
                chart.addPoint(name, plot.get(i).getTime(), plot.get(i)
                        .getPNS());
            }
        }
    }

    void addExchangesData(Chart chart) {
        chart.addDataSeries(name);

        synchronized (this) {
            int step = plot.size() / 100;
            if (step < 1) {
                step = 1;
            }

            for (int i = 0; i < plot.size(); i += step) {
                chart.addPoint(name, plot.get(i).getTime(), plot.get(i)
                        .getExchangeCount());
            }
        }

    }

}
