package nl.vu.zorilla;

import ibis.util.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.BindException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import nl.vu.zorilla.io.ZorillaPrintStream;
import nl.vu.zorilla.stats.Stats;
import nl.vu.zorilla.ui.WebInterface;
import nl.vu.zorilla.ui.ZoniServer;
import nl.vu.zorilla.util.Resources;
import nl.vu.zorilla.util.TypedProperties;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.gridlab.gat.URI;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * General peer-to-peer node. Implements a communication structure for "modules"
 * 
 */
public final class Node implements Runnable {

    // logger
    private static Logger logger = Logger.getLogger(Node.class.getName());

    private static final Random random = new Random();

    private ZorillaPrintStream logStream;

    private boolean quite;

    private final TypedProperties properties;

    private final Config config;

    // P2P network
    private final Network network;

    /**
     * webserver for interacting with this node via a browser
     */
    private final WebInterface webInterface;

    private final ZoniServer zoniServer;

    private boolean ending = false;

    private final long startTime;

    private final String version;

    private final Map<UUID, Job> jobs;

    private final String name;

    private final UUID id;

    private Resources availableResources;

    private boolean killing = false;

    public static int randomTimeout(int mean) {
        double variance = mean / 10;
        int result = mean
                + (int) ((random.nextDouble() - 0.5) * 2.0 * variance);
        logger.debug("randomTimeout(" + mean + ") == " + result);
        return result;
    }

    public static UUID generateUUID() {
        return UUID.randomUUID();
    }

    private static String extractVersion() throws ZorillaException {
        return Package.getPackage("nl.vu.zorilla").getImplementationVersion();

    }

    public String getVersion() {
        return version;
    }

    Node(Config config, UUID id, boolean quite) throws Exception {
        this.config = config;
        this.properties = config.getProperties();
        this.quite = quite;
        this.id = id;
        
        File log4jFile = new File(config.getHomeDir(), "log");
        FileAppender appender = new FileAppender(new PatternLayout("%d{HH:mm:ss} %-5p [%t] %c - %m%n"), log4jFile
                .getAbsolutePath());
        Logger.getRootLogger().addAppender(appender);

        startTime = System.currentTimeMillis();
        version = extractVersion();

        // create tmp log stream (we don't know final file name yet)
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        logStream = new ZorillaPrintStream(byteArrayStream);

        network = Network.createNetwork(this);

        name = network.getNodeName();

        // initialize node log file
        byteArrayStream.flush();
        File logFile = new File(config.getHomeDir(), name + ".log");
        try {
            FileOutputStream fileStream = new FileOutputStream(logFile, false);
            logStream = new ZorillaPrintStream(fileStream);
        } catch (IOException e) {
            throw new ZorillaException("cannot create log file for node", e);
        }

        // dump data from temp stream to file
        byteArrayStream.toByteArray();

        WebInterface webInterface = null;
        try {
            webInterface = new WebInterface(this);
            message("Started web interface on port " + webInterface.getPort());
        } catch (BindException e) {
            warn("Disabling web interface, could not bind socket", e);
        }
        this.webInterface = webInterface;

        jobs = new HashMap<UUID, Job>();

        availableResources = config.getResources();

        ZoniServer zoniServer = null;
        try {
            zoniServer = new ZoniServer(this, config.getClientPort());
            message("Started zoni interface on port " + zoniServer.getPort());
        } catch (BindException e) {
            warn("Disabling zoni interface, could not bind socket", e);
        }
        this.zoniServer = zoniServer;

        if (properties.containsKey("zorilla.max.runtime")) {
            message("Maximum runtime is "
                    + properties.getLongProperty("zorilla.max.runtime")
                    + " seconds");
            new Killer(this,
                    properties.getLongProperty("zorilla.max.runtime") * 1000);
        }

        message("Node " + name + " initialized");
    }

    // tell the user something
    public synchronized void message(String message) {
        log(message);
        if (!quite) {
            System.out.println(new Date() + " | " + message);
        }
    }

    // put something in the log
    public synchronized void log(String message) {
        try {
            logStream.printlog(message);
        } catch (IOException e) {
            logger.error("could not write message to log", e);
        }

    }

    // put something in the log
    public synchronized void log(String message, Throwable error) {
        try {
            logStream.printlog(message);
            logStream.print(error);
        } catch (IOException e) {
            logger.error("could not write message to log", e);
        }
    }

    // warn the user
    public synchronized void warn(String message) {
        try {
            logStream.printlog(message);
        } catch (IOException e) {
            logger.error("could not write message to log", e);
        }

        System.err.println(new Date() + " | WARNING " + message);

    }

    // warn the user
    public synchronized void warn(String message, Throwable error) {
        try {
            logStream.printlog("WARNING: " + message);
            logStream.print(error);
        } catch (IOException e) {
            logger.error("could not write message to log", e);
        }
        System.err.println(new Date() + " | WARNING " + message + ":"
                + error.getMessage());
    }

    public synchronized Config config() {
        return config;
    }

    public UUID getID() {
        return id;
    }

    public InetAddress guessInetAddress() {
        return network.guessInetAddress();
    }

    public int getZoniPort() {
        return zoniServer.getPort();
    }

    public int getWebPort() {
        return webInterface.getPort();
    }

    public int getBroadcastPort() {
        return config.getBroadcastPort();
    }

    private synchronized Resources getFreeResources() {
        Resources free = availableResources;

        for (Job job : jobs.values()) {
            free = free.subtract(job.usedResources());
        }
        return free;
    }

    /**
     * Useful to get resources for workers and such
     */
    public synchronized int nrOfResourceSetsAvailable(Resources request) {
        Resources free = availableResources;

        logger.debug("getting request for resources: " + request);

        for (Job job : jobs.values()) {
            free = free.subtract(job.usedResources());
        }

        logger.debug("free resources: " + free);

        int result = 0;

        if (request.zero()) {
            // infinite-loop-preventer
            warn(
                    "tried to check number of times \"zero\" resources are available, returning 0",
                    new Exception());
            return 0;
        }

        free = free.subtract(request);
        while (free.greaterOrEqualZero()) {
            result++;

            free = free.subtract(request);
        }

        logger.debug("result for resource request: " + result);

        return result;
    }

    public long startTime() {
        return startTime;
    }

    public void kill(boolean entireNetwork) throws ZorillaException {
        message("node killed (kill entire network = " + entireNetwork + ")");
        if (entireNetwork) {
            new Killer(this, Config.NETWORK_KILL_TIMEOUT);
            network.killNetwork();
        } else {
            end(Config.NODE_SHUTDOWN_TIMEOUT);
        }
    }

    public void handleNetworkKill() {
        synchronized (this) {
            if (killing) {
                return;
            }
            killing = true;
        }

        message("received kill command from network, terminating node after "
                + Config.NETWORK_KILL_TIMEOUT / 1000 + " seconds");
        new Killer(this, Config.NETWORK_KILL_TIMEOUT);
    }

    public Job submitJob(URI executable, String[] arguments,
            Map<String, String> environment, Map<String, String> attributes,
            Map<String, String> preStage, Map<String, String> postStage,
            String stdout, String stdin, String stderr)

    throws ZorillaException, IOException {

        Job job;

        job = Job.create(executable, arguments, environment, attributes,
                preStage, postStage, stdout, stdin, stderr, this);

        synchronized (this) {
            jobs.put(job.getID(), job);
        }

        return job;
    }

    public void advertise(JobAdvert advert) throws IOException,
            ZorillaException {
        network.advertise(advert);
    }

    public void handleJobAdvert(JobAdvert advert) {
        try {
            Job job;
            UUID jobID = (UUID) advert.getJobID();
            synchronized (this) {
                job = jobs.get(jobID);

                if (job == null) {
                    logger.debug("recevied job advert for "
                            + jobID.toString().substring(0, 7));

                    job = Job.createConstituent(advert, this);
                    jobs.put(jobID, job);
                }
            }
        } catch (Exception e) {
            warn("error on handling job advert", e);
        }
    }

    /**
     * Ends this node within the number of milliseconds given.
     */
    void end(long timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        Job[] jobArray;

        message("Shutting down Zorilla node");
        log("ending node in " + timeout + " milliseconds");

        synchronized (this) {
            if (ending) {
                return;
            }
            ending = true;
            notifyAll();

            jobArray = jobs.values().toArray(new Job[0]);

        }

        for (Job job : jobArray) {
            job.end(deadline);
        }

        network.end(deadline);

        if (webInterface != null) {
            webInterface.stop();
        }

        if (zoniServer != null) {
            zoniServer.stop();
        }

        synchronized (this) {
            log("node end done.");
            ending = true;
        }
    }

    /**
     * returns the job with the given ID
     * 
     * @throws ZorillaException
     *             if there is no Job for the given ID
     */
    public synchronized Job getJob(UUID jobID) throws ZorillaException {
        Job result = jobs.get(jobID);

        if (result == null) {
            throw new ZorillaException("requested job: " + jobID
                    + " does not exist");
        }

        return result;
    }

    public synchronized Job[] getJobs() {
        return jobs.values().toArray(new Job[0]);
    }

    /**
     * cleanup and maintenance thread
     */
    public synchronized void run() {
        while (!ending) {
            try {
                wait(Config.NODE_MAINTENANCE_INTERVAL);
            } catch (InterruptedException e) {
                // IGNORE
            }

            // purge dead jobs
            Iterator<Job> iterator = jobs.values().iterator();
            while (iterator.hasNext()) {
                Job job = iterator.next();

                if (job.zombie()) {
                    iterator.remove();
                }
            }
        }
    }

    public String toString() {
        return name;
    }

    private static void printUsage(PrintStream out) {
        out.println("Zorilla usage:");
        out.println();

        out.println("--config_file | -c");
        out.println("\t" + Config.getPropertyDescription("config.file"));

        out.println("--bamboo");
        out.println("\tuse bamboo for the inter-node connections (default)");

        out.println("--no_bamboo");
        out.println("\tuse the internal Zorilla P2P network for inter-node");
        out.println("\tconnections (broken!)");

        out.println("--node_address | -ip");
        out.println("\t" + Config.getPropertyDescription("node.address"));

        out.println("--network_interface | -ni");
        out.println("\t" + Config.getPropertyDescription("network.interface"));

        out.println("--network_name");
        out.println("\t" + Config.getPropertyDescription("network.name"));

        out.println("--cluster");
        out.println("\t" + Config.getPropertyDescription("cluster"));

        out.println("--home_address");
        out.println("\t" + Config.getPropertyDescription("home.address"));

        out.println("--peers");
        out.println("\t" + Config.getPropertyDescription("peers"));

        out.println("--port");
        out.println("\t" + Config.getPropertyDescription("port"));

        out.println("--broadcast_port");
        out.println("\t" + Config.getPropertyDescription("broadcast.port"));

        out.println("--www_port");
        out.println("\t" + Config.getPropertyDescription("www.port"));

        out.println("--zoni_port");
        out.println("\t" + Config.getPropertyDescription("zoni.port"));

        out.println("--random_ports");
        out.println("\t shortcut to set all the port options to 0");

        out.println("--log_dir");
        out.println("\t" + Config.getPropertyDescription("log.dir"));

        out.println("--tmp_dir");
        out.println("\t" + Config.getPropertyDescription("tmp.dir"));

        out.println("--max_job_lifetime");
        out.println("\t" + Config.getPropertyDescription("max.job.lifetime"));

        out.println("--memory");
        out.println("\t" + Config.getPropertyDescription("memory"));

        out.println("--diskspace");
        out.println("\t" + Config.getPropertyDescription("diskspace"));

        out.println("--processors");
        out.println("\t" + Config.getPropertyDescription("processors"));

        out.println("--native_jobs");
        out.println("\tenable the running of native applications by this node");
        out.println("\t(WARNING: security risk!)");

        out.println("--no_native_jobs");
        out.println("\tdeny running native applications on this node");

        out.println("--gossip_algorithm");
        out.println("\t" + Config.getPropertyDescription("gossip.algorithm"));

        out.println("--gossip_protocol");
        out.println("\t" + Config.getPropertyDescription("gossip.protocol"));

        out.println("--config_properties");
        out
                .println("\tprint a list of all valid configuration properties of Zorilla");
        out
                .println("\tthat can be used in the config file, or set as a system property");

        out.println("--quite | -q");
        out.println("only print out warnings, not status messages");

        out.println("-? | -h | -help | --help");
        out.println("\tprint this message");
    } // MAIN FUNCTION

    public static void main(String[] args) {
        boolean quite = false;

        Properties commandLineProperties = new Properties();

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--config_file")
                    || args[i].equalsIgnoreCase("-c")) {
                i++;
                commandLineProperties.put("zorilla.config.file", args[i]);
            } else if (args[i].equalsIgnoreCase("--bamboo")) {
                commandLineProperties.put("zorilla.bamboo", "true");
            } else if (args[i].equalsIgnoreCase("--no_bamboo")) {
                commandLineProperties.put("zorilla.bamboo", "false");
            } else if (args[i].equalsIgnoreCase("-ip")
                    || args[i].equalsIgnoreCase("--node_address")) {
                i++;
                commandLineProperties.put("zorilla.node.address", args[i]);
            } else if (args[i].equalsIgnoreCase("-ni")
                    || args[i].equalsIgnoreCase("--network_interface")) {
                i++;
                commandLineProperties.put("zorilla.network.interface", args[i]);
            } else if (args[i].equalsIgnoreCase("--network_name")) {
                i++;
                commandLineProperties.put("zorilla.network.name", args[i]);
            } else if (args[i].equalsIgnoreCase("--cluster")) {
                i++;
                commandLineProperties.put("zorilla.cluster", args[i]);
            } else if (args[i].equalsIgnoreCase("--home_address")) {
                i++;
                commandLineProperties.put("zorilla.home.address", args[i]);
            } else if (args[i].equalsIgnoreCase("--peers")) {
                i++;
                commandLineProperties.put("zorilla.peers", args[i]);
            } else if (args[i].equalsIgnoreCase("--port")) {
                i++;
                commandLineProperties.put("zorilla.port", args[i]);
            } else if (args[i].equalsIgnoreCase("--broadcast_port")) {
                i++;
                commandLineProperties.put("zorilla.broadcast.port", args[i]);
            } else if (args[i].equalsIgnoreCase("--www_port")) {
                i++;
                commandLineProperties.put("zorilla.www.port", args[i]);
            } else if (args[i].equalsIgnoreCase("--zoni_port")) {
                i++;
                commandLineProperties.put("zorilla.zoni.port", args[i]);
            } else if (args[i].equalsIgnoreCase("--random_ports")) {
                commandLineProperties.put("zorilla.port", "0");
                commandLineProperties.put("zorilla.broadcast.port", "0");
                commandLineProperties.put("zorilla.www.port", "0");
                commandLineProperties.put("zorilla.zoni.port", "0");
            } else if (args[i].equalsIgnoreCase("--log_dir")) {
                i++;
                commandLineProperties.put("zorilla.log.dir", args[i]);

            } else if (args[i].equalsIgnoreCase("--tmp_dir")) {
                i++;
                commandLineProperties.put("zorilla.tmp.dir", args[i]);

            } else if (args[i].equalsIgnoreCase("--max_job_lifetime")) {
                i++;
                commandLineProperties.put("zorilla.max.job.lifetime", args[i]);

            } else if (args[i].equalsIgnoreCase("--memory")) {
                i++;
                commandLineProperties.put("zorilla.memory", args[i]);

            } else if (args[i].equalsIgnoreCase("--diskspace")) {
                i++;
                commandLineProperties.put("zorilla.diskspace", args[i]);

            } else if (args[i].equalsIgnoreCase("--processors")) {
                i++;
                commandLineProperties.put("zorilla.processors", args[i]);
            } else if (args[i].equalsIgnoreCase("--processors")) {
                i++;
                commandLineProperties.put("zorilla.processors", args[i]);
            } else if (args[i].equalsIgnoreCase("--native_jobs")) {
                commandLineProperties.put("zorilla.native.jobs", "true");
            } else if (args[i].equalsIgnoreCase("--no_native_jos")) {
                commandLineProperties.put("zorilla.native.jobs", "false");
            } else if (args[i].equals("-D")) {
                i++;
                String[] parts = args[i].split("=");
                if (parts.length != 2) {
                    System.err
                            .println("property should be defined as VARIABLE=VALUE (not "
                                    + args[i] + ")");
                    System.exit(1);
                }
                commandLineProperties.put(parts[0], parts[1]);
            } else if (args[i].startsWith("-D")) {
                String[] parts = args[i].substring(2).split("=");
                if (parts.length != 2) {
                    System.err
                            .println("property should be defined as VARIABLE=VALUE (not "
                                    + args[i] + ")");
                    System.exit(1);
                }
                commandLineProperties.put(parts[0], parts[1]);
            } else if (args[i].equalsIgnoreCase("--quite")
                    || args[i].equalsIgnoreCase("-q")) {
                quite = true;
            } else if ((args[i].equalsIgnoreCase("--help")
                    || args[i].equalsIgnoreCase("-h") || args[i]
                    .equalsIgnoreCase("-?"))) {
                printUsage(System.out);
                System.exit(0);
            } else {
                System.err.println("unknown command line option: " + args[i]);
                System.err.println();
                printUsage(System.err);
                System.exit(1);
            }
        }

        Node node = null;
        try {
            UUID nodeID = generateUUID();
            Config config = new Config(commandLineProperties, nodeID);
            node = new Node(config, nodeID, quite);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.err.println("...cannot start node, exiting");
            // cannot create node, exit...
            System.exit(1);
        }

        // register shutdown hook
        try {
            Runtime.getRuntime().addShutdownHook(new Shutdown(node));
        } catch (Exception e) {
            // IGNORE
        }

        try {
            Signal.handle(new Signal("USR2"), new Terminator(node));
        } catch (Exception e) {
            node.warn("could not install handler for USR2 signal");
        }

        node.run();

    }

    private static class Shutdown extends Thread {
        private final Node node;

        Shutdown(Node node) {
            this.node = node;
        }

        public void run() {
            node.log("shutdown hook triggered");

            node.end(0);
        }
    }

    private static class Terminator implements SignalHandler {
        private final Node node;

        Terminator(Node node) {
            this.node = node;
        }

        public void handle(Signal signal) {
            node.log("SIGUSR2 catched, shutting down");

            node.end(Config.NODE_SHUTDOWN_TIMEOUT);
            System.exit(0);
        }
    }

    // kills node, after waiting for a while
    private static class Killer implements Runnable {
        private final Node node;

        private final long timeout;

        Killer(Node node, long timeout) {
            this.node = node;
            this.timeout = timeout;
            ThreadPool.createNew(this, "node killer");
        }

        public void run() {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                // IGNORE
            }
            node.end(Config.NODE_SHUTDOWN_TIMEOUT);
        }
    }

    public Map<String, Object> getInfo() {
        Map<String, Object> result = new HashMap<String, Object>();

        result.put("name", name);
        result.put("version", version);
        result.put("id", id);
        result.put("start.time", new Date(startTime));

        result.put("nr.of.jobs", getJobs().length);

        return result;
    }

    public void setAttributes(Map<String, String> newAttributes)
            throws ZorillaException {
        if (newAttributes.size() > 0) {
            throw new ZorillaException("update of attributes not supported yet");
        }
    }

    public Stats getStats() {
        Stats result = new Stats(id);

        result.put("node", getInfo());
        result.put("node", "free.resources", getFreeResources());
        result.put("node", "properties", properties);

        network.getStats(result);

        return result;
    }

    public TypedProperties getProperties() {
        return properties;
    }

}
