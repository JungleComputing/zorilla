package ibis.zorilla;

import ibis.zorilla.cluster.ClusterService;
import ibis.zorilla.cluster.VivaldiService;
import ibis.zorilla.gossip.GossipService;
import ibis.zorilla.job.JobService;
import ibis.zorilla.job.ZorillaJob;
import ibis.zorilla.net.Discovery;
import ibis.zorilla.net.FloodService;
import ibis.zorilla.net.Network;
import ibis.zorilla.net.UdpDiscoveryService;
import ibis.zorilla.rpc.SocketRPC;

import java.io.File;
import java.net.InetAddress;
import java.rmi.RemoteException;
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

    private final Network network;

    private final SocketRPC socketRPC;

    // ***** Services *****\\

    private final Discovery discoveryService;

    private final UdpDiscoveryService udpDiscoveryService;

    private final GossipService gossipService;

    private final VivaldiService vivaldiService;

    private final ClusterService clusterService;

    private final FloodService floodService;

    private final JobService jobService;

    private final long startTime;

    private final String name;

    private final UUID id;

    private boolean ended = false;

    // private final VirtualMachine machine;

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

        network = new Network(this, config);

        // give this node a (user friendly) name
        if (config.getProperty(Config.NODE_NAME) != null) {
            name = config.getProperty(Config.NODE_NAME);
        } else {
            name = InetAddress.getLocalHost().getHostName();

        }

        // start logging node logs
        File log4jFile = new File(config.getLogDir(), name + ".log");
        FileAppender appender = new FileAppender(new PatternLayout(
                "%d{HH:mm:ss} %-5p [%t] %c - %m%n"),
                log4jFile.getAbsolutePath());
        Logger.getRootLogger().addAppender(appender);

        // INIT SERVICES
     
        vivaldiService = new VivaldiService(this);

        discoveryService = new Discovery(this, network);

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

        logger.info("Read configuration from " + config.getConfigDir());
        logger.info("Saving statistics and logs to " + config.getLogDir());
        logger.info("Saving temporary files to " + config.getTmpDir());
        logger.info("Node " + name + " started");

        socketRPC = new SocketRPC(SocketRPC.DEFAULT_PORT);
        logger.info("Started RPC on port " + socketRPC.getPort());

        // socketRPC.exportObject(NodeInterface.class, this, "zorilla node");

        // machine = new VirtualMachine(new
        // File("/home/ndrost/vm/windowsssh.ovf"), new
        // File("/home/ndrost/tmp/sandbox"));
        // logger.info("vm port = " + machine.getSshPort());
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

    public long getStartTime() {
        return startTime;
    }

    public Network network() {
        return network;
    }

    public Discovery discoveryService() {
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

    public SocketRPC getRPC() {
        return socketRPC;
    }

    public synchronized void end() {
        if (ended) {
            // already ended
            notifyAll();
            return;
        }

        logger.debug("killing al jobs");

        jobService.killAllJobs();

        logger.info("stopping zorilla node");

        network.end();

        socketRPC.end();

        logger.info("node done");

        ended = true;
        notifyAll();
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

    public synchronized void waitUntilEnded() {
        while (!ended) {
            try {
                wait(1000);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
    }

    public ZorillaJob[] getJobs() throws RemoteException {
        return jobService().getJobs();
    }

    public ZorillaJob getJob(UUID jobID) throws Exception {
        return jobService().getJob(jobID);
    }

    public UUID[] getJobIDs() throws RemoteException {
        return jobService().getJobIDs();
    }

    public ZorillaJob submitJob(JobDescription jobDescription) throws Exception {
        ZorillaJob job = jobService().submitJob(jobDescription, null);
        return job;
    }
}
