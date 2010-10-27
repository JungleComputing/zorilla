package ibis.zorilla;

import ibis.ipl.server.ServerProperties;
import ibis.zorilla.cluster.ClusterService;
import ibis.zorilla.cluster.VivaldiService;
import ibis.zorilla.gossip.GossipService;
import ibis.zorilla.job.JobService;
import ibis.zorilla.net.DiscoveryService;
import ibis.zorilla.net.FloodService;
import ibis.zorilla.net.Network;
import ibis.zorilla.net.UdpDiscoveryService;
import ibis.zorilla.www.WebService;

import java.io.File;
import java.net.InetAddress;
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
public final class Node {

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

    private final Config config;

    private final ibis.ipl.server.Server iplServer;

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

    private final long startTime;

    private final String name;

    private final UUID id;

//    private final VirtualMachine machine; 

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

    public Node(Properties properties) throws Exception {
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

        // start the IPL server first, we use its socket factory...
        Properties serverProperties = new Properties();
        serverProperties.setProperty(ServerProperties.PORT, ""
                + config.getPort());

        // copy-paste start hub property to server
        if (config.isHub()) {
            serverProperties.setProperty(ServerProperties.START_HUB, "true");
        } else {
            serverProperties.setProperty(ServerProperties.START_HUB, "false");
            logger.info("sleeping to give hub a chance to appear");
            Thread.sleep(60000);
        }

        // copy-paste hub addresses property to server
        if (config.getProperty(Config.HUB_ADDRESSES) != null) {
            serverProperties.setProperty(ServerProperties.HUB_ADDRESSES, config
                    .getProperty(Config.HUB_ADDRESSES));
        }
        
        if (config.getProperty(Config.VIZ_INFO) != null) {
            serverProperties.setProperty(ServerProperties.VIZ_INFO, config
                    .getProperty(Config.VIZ_INFO));
        }
        
        if (config.getBooleanProperty(Config.VERBOSE)) {
            serverProperties.put(ServerProperties.PRINT_EVENTS, "true");
            serverProperties.put(ServerProperties.PRINT_STATS, "true");
            
            
        }
        
        //start ipl server
        iplServer = new ibis.ipl.server.Server(serverProperties);
        
        network = new Network(this, config, iplServer.getSocketFactory());

        // give this node a (user friendly) name
        if (config.getProperty(Config.NODE_NAME) != null) {
            name = config.getProperty(Config.NODE_NAME);
        } else {
            String cluster = config.getProperty(Config.CLUSTER_NAME);
            String hostName = InetAddress.getLocalHost().getHostName();
            int port = network.getAddress().machine().getPorts(false)[0];

            if (cluster == null) {
                name = hostName + ":" + port;
                config.put(Config.NODE_NAME, name);
            } else {
                name = hostName + ":" + port + "@" + cluster;
                config.put(Config.NODE_NAME, name);
            }
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

        discoveryService.start();
        udpDiscoveryService.start();
        gossipService.start();
        vivaldiService.start();
        clusterService.start();
        floodService.start();
        jobService.start();

        // start accepting connections
        network.start();
        webService.start();

        logger.info("Read configuration from " + config.getConfigDir());
        logger.info("Saving statistics and logs to " + config.getLogDir());
        logger.info("Saving temporary files to " + config.getTmpDir());
        logger.info("Node " + name + " started");
        
//        machine = new VirtualMachine(new File("/home/ndrost/vm/windowsssh.ovf"), new File("/home/ndrost/tmp/sandbox"));
//        logger.info("vm port = " + machine.getSshPort());
    }

    public synchronized Config config() {
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
                .getProperty(Config.CLUSTER_NAME), vivaldiService
                .getCoordinates(), network.getAddress(), version, config.isHub());
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

    public ibis.ipl.server.Server getIPLServer() {
        return iplServer;
    }

    public synchronized void end() {
        
        logger.debug("killing al jobs");

        jobService.killAllJobs();

        logger.info("stopping zorilla node");
        
        network.end();
        
        logger.info("node done");

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
}
