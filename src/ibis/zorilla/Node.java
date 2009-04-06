package ibis.zorilla;

import ibis.smartsockets.virtual.VirtualSocketFactory;
import ibis.util.TypedProperties;
import ibis.zorilla.cluster.ClusterService;
import ibis.zorilla.cluster.VivaldiService;
import ibis.zorilla.gossip.GossipService;
import ibis.zorilla.job.JobService;
import ibis.zorilla.net.DiscoveryService;
import ibis.zorilla.net.FloodService;
import ibis.zorilla.net.Network;
import ibis.zorilla.net.UdpDiscoveryService;
import ibis.zorilla.net.ZoniService;
import ibis.zorilla.slave.SlaveService;
import ibis.zorilla.www.WebService;

import java.io.File;
import java.net.InetAddress;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * General peer-to-peer node. Implements a communication structure for "modules"
 * 
 */
public final class Node implements Runnable, ibis.ipl.server.Service {

    // logger
    private static Logger logger = Logger.getLogger(Node.class);

    private static final Random random = new Random();

    private static final long version;

    static {
        version = Long.parseLong(Package.getPackage("ibis.zorilla")
                .getImplementationVersion());
    }

    public static long getVersion() {
        return version;
    }

    private final ZorillaProperties config;

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
    
    private final SlaveService slaveService;

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

    public Node(TypedProperties properties) throws Exception {
        this(properties, null);
    }

    public Node(TypedProperties properties, VirtualSocketFactory factory)
            throws Exception {

        // Log.initLog4J("ibis.zorilla", Level.INFO);

        config = new ZorillaProperties(properties);

        // make up a UUID for this node
        String idString = config.getProperty(ZorillaProperties.NODE_ID);
        if (idString == null) {
            id = generateUUID();
        } else {
            id = UUID.fromString(idString);
        }

        startTime = System.currentTimeMillis();

        network = new Network(this, config, factory);

        // give this node a (user friendly) name
        if (config.getProperty(ZorillaProperties.NODE_NAME) != null) {
            name = config.getProperty(ZorillaProperties.NODE_NAME);
        } else {
            String cluster = config
                    .getProperty(ZorillaProperties.CLUSTER_NAME);
            String hostName = InetAddress.getLocalHost().getHostName();
            int port = network.getAddress().machine().getPorts(false)[0];

            if (cluster == null) {
                name = hostName + ":" + port;
                config.put(ZorillaProperties.NODE_NAME, name);
            } else {
                name = hostName + ":" + port + "@" + cluster;
                config.put(ZorillaProperties.NODE_NAME, name);
            }
        }

        if (config.isWorker()) {
            // TODO: start log forwarding service
        }

        // start logging node logs
        File log4jFile = new File(config.getLogDir(), name + ".log");
        FileAppender appender = new FileAppender(new PatternLayout(
                "%d{HH:mm:ss} %-5p [%t] %c - %m%n"), log4jFile
                .getAbsolutePath());
        Logger.getRootLogger().addAppender(appender);

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
        
        slaveService = new SlaveService(this);

        discoveryService.start();
        udpDiscoveryService.start();
        gossipService.start();
        vivaldiService.start();
        clusterService.start();
        floodService.start();
        jobService.start();
        slaveService.start();

        // start accepting connections
        network.start();
        zoniService.start();
        webService.start();

        long maxRunTime = config.getLongProperty(
                ZorillaProperties.MAX_RUNTIME, 0);
        if (maxRunTime > 0) {
            deadline = System.currentTimeMillis() + (maxRunTime * 1000);
            logger.info("Shutting down zorilla node in " + maxRunTime
                    + " seconds");
        } else {
            deadline = Long.MAX_VALUE;
        }

        logger.info("Read configuration from " + config.getConfigDir());
        logger.info("Saving statistics and logs to " + config.getLogDir());
        logger.info("Saving temporary files to " + config.getTmpDir());
        if (config.isMaster()) {
            logger.info("MASTER node " + name + " started");
        } else if (config.isWorker()) {
            logger.info("WORKER node " + name + " started");
        } else {
            logger.info("Node " + name + " started");
        }
    }

    public synchronized ZorillaProperties config() {
        return config;
    }

    public UUID getID() {
        return id;
    }

    public String getName() {
        return name;
    }

    public NodeInfo getInfo() {
        return new NodeInfo(id, name, config
                .getProperty(ZorillaProperties.CLUSTER_NAME),
                vivaldiService.getCoordinates(), network.getAddress(), version);
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
    
    public SlaveService getSlaveService() {
        return slaveService;
    }

    @Override
    public synchronized void end(long delay) {
        long newDeadline = System.currentTimeMillis() + delay;

        if (newDeadline < deadline) {
            deadline = newDeadline;
            notifyAll();
        }
    }

    public Map<String, String> getStats() {
        Map<String, String> result = new LinkedHashMap<String, String>();

        result.put("Name", name);
        result.put("Version", Long.toString(version));
        result.put("ID", id.toString());
        result.put("Start.Time", new Date(startTime).toString());

        result.put("Nr.of.jobs", Integer.toString(jobService.getJobs().length));

        result.put("Address", network.getAddress().toString());
        result.put("Coordinate", vivaldiService().getCoordinates().toString());
        result.putAll(jobService.getResources().asStringMap());

        return result;
    }

    public String toString() {
        return "Zorilla node " + name;
    }

    // delay until a certain time before the deadline
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
        
        slaveService.end();
        network.end();

        logger.debug("node done");

    }

    @Override
    public String getServiceName() {
        return "zorilla";
    }

}