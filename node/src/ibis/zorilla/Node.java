package ibis.zorilla;

import ibis.zorilla.cluster.ClusterService;
import ibis.zorilla.cluster.VivaldiService;
import ibis.zorilla.gossip.GossipService;
import ibis.zorilla.job.JobService;
import ibis.zorilla.net.DiscoveryService;
import ibis.zorilla.net.FloodService;
import ibis.zorilla.net.Network;
import ibis.zorilla.net.UdpDiscoveryService;
import ibis.zorilla.net.ZoniService;
import ibis.zorilla.www.WebService;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * General peer-to-peer node. Implements a communication structure for "modules"
 * 
 */
public final class Node implements Runnable {

    // logger
    private static Logger logger = Logger.getLogger(Node.class);

    private static final Random random = new Random();

    private final Config config;

    // directory to store logs, stats, etc
    private final File logDir;

    private final Network network;

    // ***** Services *****\\

    private final DiscoveryService discoveryService;

    private final UdpDiscoveryService udpDiscoveryService;

    private final GossipService gossipService;

    private final VivaldiService vivaldiService;

    private final ClusterService clusterService;

    private final FloodService floodService;

    private final JobService jobService;

    private final WebService webService;

    private final ZoniService zoniService;

    private final long startTime;

    private final String name;

    private final UUID id;

    private long deadline;

    public static int randomTimeout(int mean) {
        double variance = mean / 10;
        int result = mean
                + (int) ((random.nextDouble() - 0.5) * 2.0 * variance);
        logger.debug("randomTimeout(" + mean + ") == " + result);
        return result;
    }

    public static int randomInt(int n) {
        return random.nextInt(n);
    }

    public static UUID generateUUID() {
        return UUID.randomUUID();
    }

    public static String getVersion() {
        return Package.getPackage("ibis.zorilla").getImplementationVersion();
    }

    Node(Properties properties) throws Exception {
        // Log.initLog4J("ibis.zorilla", Level.INFO);

        config = new Config(properties);

        // make up a UUID for this node
        String idString = config.getProperty(Config.NODE_ID);
        if (idString == null) {
            id = generateUUID();
        } else {
            id = UUID.fromString(idString);
        }

        startTime = System.currentTimeMillis();

        network = new Network(this);

        // give this node a (user friendly) name
        if (config.getProperty(Config.NODE_NAME) != null) {
            name = config.getProperty(Config.NODE_NAME);
        } else {
            String cluster = config.getProperty(Config.CLUSTER_NAME);
            String hostName = InetAddress.getLocalHost().getHostName();
            InetSocketAddress[] addresses = network.getAddress()
                    .getPrivateAddresses();
            if (addresses.length == 0) {
                addresses = network.getAddress().getPublicAddresses();
            }
            if (addresses.length == 0) {
                throw new IOException("could not get network address");
            }

            int port = addresses[0].getPort();

            if (cluster == null) {
                name = hostName + ":" + port;
                config.put(Config.NODE_NAME, name);
            } else {
                name = hostName + ":" + port + "@" + cluster;
                config.put(Config.NODE_NAME, name);
            }
        }
        
        config.getTmpDir().mkdirs();
        config.getTmpDir().deleteOnExit();
        if (!config.getTmpDir().exists()) {
            throw new Exception("could not create tmp dir: " + config.getTmpDir());
        }
        
        if (config.isWorker()) {
            // TODO: start log forwarding service

            logDir = null;
        } else {
            // save log locally
            logDir = new File(config.getConfigDir(), name);

            logDir.mkdirs();

            File log4jFile = new File(logDir, "log");
            FileAppender appender = new FileAppender(new PatternLayout(
                    "%d{HH:mm:ss} %-5p [%t] %c - %m%n"), log4jFile
                    .getAbsolutePath());
            Logger.getRootLogger().addAppender(appender);
        }

       
        // INIT SERVICES

        // webservice is first, as it also keeps logs
        webService = new WebService(this);

        vivaldiService = new VivaldiService(this);

        discoveryService = new DiscoveryService(this);

        udpDiscoveryService = new UdpDiscoveryService(this);

        gossipService = new GossipService(this);

        clusterService = ClusterService.getClusterService(this);

        floodService = new FloodService(this);

        jobService = new JobService(this);

        zoniService = new ZoniService(this);

        discoveryService.start();
        udpDiscoveryService.start();
        gossipService.start();
        vivaldiService.start();
        clusterService.start();
        floodService.start();
        jobService.start();

        // start accepting connections
        network.start();
        zoniService.start();
        webService.start();

        long maxRunTime = config.getLongProperty(Config.MAX_RUNTIME, 0);
        if (maxRunTime > 0) {
            deadline = System.currentTimeMillis() + (maxRunTime * 1000);
            logger.info("Shutting down zorilla node in " + maxRunTime
                    + " seconds");
        } else {
            deadline = Long.MAX_VALUE;
        }

        logger.info("Saving statistics and logs to " + logDir);
        if (config.isMaster()) {
            logger.info("MASTER node " + name + " started");
        } else if (config.isWorker()) {
            logger.info("WORKER node " + name + " started");
        } else {
            logger.info("Node " + name + " started");
        }            
    }

    public synchronized Config config() {
        return config;
    }

    public File getNodeDir() {
        return logDir;
    }

    public UUID getID() {
        return id;
    }

    public String getName() {
        return name;
    }

    public NodeInfo getInfo() {
        return new NodeInfo(id, name, config.getProperty(Config.CLUSTER_NAME),
                vivaldiService.getCoordinates(), network.getAddress());
    }

    public long getStartTime() {
        return startTime;
    }

    public Network network() {
        return network;
    }

    public DiscoveryService discoveryService() {
        return discoveryService;
    }

    public UdpDiscoveryService udpDiscoveryService() {
        return udpDiscoveryService;
    }

    public GossipService gossipService() {
        return gossipService;
    }

    public VivaldiService vivaldiService() {
        return vivaldiService;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public FloodService floodService() {
        return floodService;
    }

    public JobService jobService() {
        return jobService;
    }

    public WebService webService() {
        return webService;
    }

    public ZoniService zoniService() {
        return zoniService;
    }

    public synchronized void stop(long delay) {
        long newDeadline = System.currentTimeMillis() + delay;

        if (newDeadline < deadline) {
            deadline = newDeadline;
            notifyAll();
        }
    }

    public Map<String, String> getStats() {
        Map<String, String> result = new LinkedHashMap<String, String>();

        result.put("Name", name);
        result.put("Version", getVersion());
        result.put("ID", id.toString());
        result.put("Start.Time", new Date(startTime).toString());

        result.put("Nr.of.jobs", Integer.toString(jobService.getJobs().length));

        result.put("Address", network.getAddress().toString());
        result.put("Coordinate", vivaldiService().getCoordinates().toString());
        result.put("Max.workers", Integer
                .toString(jobService().getMaxWorkers()));

        return result;
    }

    public String toString() {
        return name;
    }

    // delay until a certain time before the dealine
    private synchronized void waitUntilDeadline(long margin) {
        while (true) {
            long now = System.currentTimeMillis();
            long maxDelay = (deadline - now) - margin;

            if (maxDelay <= 0) {
                return;
            }

            logger.debug("waiting for: " + maxDelay);

            try {
                wait(maxDelay);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
    }

    public void run() {
        logger.debug("waiting until one minute before deadline");
        waitUntilDeadline(60 * 1000);

        logger.debug("killing al jobs");

        jobService.killAllJobs();

        logger.debug("waiting until deadline");

        waitUntilDeadline(0);

        logger.debug("node done");

    }

}
