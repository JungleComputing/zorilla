package nl.vu.zorilla;

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
import java.util.UUID;

import nl.vu.zorilla.bigNet.Function;
import nl.vu.zorilla.bigNet.Message;
import nl.vu.zorilla.bigNet.Metric;
import nl.vu.zorilla.bigNet.Network;
import nl.vu.zorilla.bigNet.NetworkException;
import nl.vu.zorilla.io.ZorillaPrintStream;
import nl.vu.zorilla.ui.WebInterface;
import nl.vu.zorilla.ui.ZoniServer;


import org.apache.log4j.Logger;
import org.gridlab.gat.URI;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * General peer-to-peer node. Implements a communication structure for "modules"
 * 
 */
public final class Node implements Runnable {

    // logger
    static Logger logger = Logger.getLogger(Node.class.getName());

    private ZorillaPrintStream logStream;

    private boolean quite;

    private final Config config;

    /**
     * interface to the different network transports
     */
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

    public static UUID generateUUID() {
        return UUID.randomUUID();
    }

    private static String extractVersion() throws ZorillaException {
        return Package.getPackage("nl.vu.zorilla")
                .getImplementationVersion();

    }
    
    public String getVersion() {
        return version;
    }

    Node(Config config, boolean quite) throws Exception {
        this.config = config;
        this.quite = quite;

        id = generateUUID();
        startTime = System.currentTimeMillis();
        version = extractVersion();

        // create tmp log stream (we don't know final file name yet)
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        logStream = new ZorillaPrintStream(byteArrayStream);
        
        // print configuration to log file (and possibly console)
        message("Initializing Zorilla with the following configuration:");
        config.print(logStream);
        if (!quite) {
            config.print(new ZorillaPrintStream(System.out));
        }

        network = Network.createNetwork(this);

        name = network.address().getHostName() + ":"
                + network.address().getPort();   

        // initialize node log file
        byteArrayStream.flush();
        File logFile = new File(config.getLogDir(), name + ".log");
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

        // there is exactly one node :)
        availableResources = config.getResources();

        ZoniServer zoniServer = null;
        try {
            zoniServer = new ZoniServer(this, config.getClientPort());
            message("Started zoni interface on port " + zoniServer.getPort());
        } catch (BindException e) {
            warn("Disabling zoni interface, could not bind socket", e);
        }
        this.zoniServer = zoniServer;

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
        System.err.println(new Date() + " | WARNING " + message + ":" + error.getMessage());
    }

    public synchronized Config config() {
        return config;
    }

    public UUID getID() {
        return id;
    }

    public InetAddress getInetAddress() {
        return network.address().getAddress();
    }

    public int getPort() {
        return network.address().getPort();
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

    /**
     * Useful to get resources for workers and such
     */
    public synchronized int nrOfResourceSetsAvailable(Resources request) {
        Resources free = availableResources;

        for (Job job : jobs.values()) {
            free = free.subtract(job.usedResources());
        }

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

        return result;
    }

    public long startTime() {
        return startTime;
    }

    /**
     * Kill every node we can reach. Should reach just about the entire world.
     * >:)
     */
    public void kill(boolean entireNetwork) throws ZorillaException {
        message("node killed (kill entire network = " + entireNetwork + ")");
        if (entireNetwork) {
            Message m = network.getMessage();
            m.setFunction(Function.KILL_NETWORK);

            network.flood(m, Metric.LATENCY_HOPS, 100);
            // network.flood(m, 100, Metric.HOPS, null, Function.KILL_NETWORK);
        } else {
            end(Config.NODE_SHUTDOWN_TIMEOUT);
        }
    }

    public Job submitJob(URI executable, String[] arguments,
            Map<String, String> environment, Map<String, String> attributes,
            Map<String, String> preStage, Map<String, String> postStage,
            String stdout, String stdin, String stderr)

    throws ZorillaException, IOException {

        Job job;

        job = Job.create(executable, arguments, environment, attributes, preStage,
                postStage, stdout, stdin, stderr, this);

        synchronized (this) {
            jobs.put(job.getID(), job);
        }

        return job;
    }

    public void advertise(Job job, Metric metric, long metricValue)
            throws IOException, NetworkException, ZorillaException {
        Message message = network.getMessage();
        message.setFunction(Function.JOB_ADVERT);
        message.writeObject(job.getID());
        job.writeBootstrap(message);
        network.flood(message, metric, metricValue);

    }

    /**
     * delivers a message to this node
     * 
     * @param m
     *            the message that needs to be delivered
     */
    public void receive(Message m) {

        if (logger.isDebugEnabled()) {
            logger.debug("received message: " + m);
        }

        if (m.getFunction() == Function.KILL_NETWORK) {
            logger.debug("received network kill");

            // cancel all jobs

            Job[] jobArray;
            synchronized (this) {
                jobArray = jobs.values().toArray(new Job[0]);
            }

            // kill all jobs
            for (Job job : jobArray) {
                job.end(Config.NETWORK_KILL_TIMEOUT);
            }

            try {
                Thread.sleep(Config.NETWORK_KILL_TIMEOUT);
            } catch (InterruptedException e) {
                // IGNORE
            }
            end(0);
            System.err.println("node killed remotely");
            System.exit(1);
        } else if (m.getFunction() == Function.JOB_ADVERT) {
            try {
                Job job;
                UUID jobID = (UUID) m.readObject();
                synchronized (this) {
                    job = jobs.get(jobID);

                    if (job == null) {
                        logger.debug("recevied job advert for "
                                + jobID.toString().substring(0, 7));

                        job = Job.createConstituent(m, this);
                        jobs.put(jobID, job);
                    }
                }
            } catch (Exception e) {
                warn("error on handling job advert", e);
            }
        } else {
            warn("message received for unknown function: " + m.getFunction());
        }
    }

    /**
     * Ends this node within the number of milliseconds given.
     */
    void end(long timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        Job[] jobArray;

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

        if(zoniServer != null) {
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

        out.println("--config_properties");
        out.println("\tprint a list of all valid configuration properties of Zorilla");
        out.println("\tthat can be used in the config file, or set as a system property");

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
            } else if (args[i].equalsIgnoreCase("--quite")
                    || args[i].equalsIgnoreCase("-q")) {
                quite = true;
            } else if (args[i].equalsIgnoreCase("--config_properties")) {
                Config.printProperties(System.out);
                System.exit(0);
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
            Config config = new Config(commandLineProperties);
            node = new Node(config, quite);
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

    public Map<String, String> getInfo() {
        Map<String, String> result = new HashMap<String, String>();

        result.put("node.name", name);
        result.put("nr.of.jobs", Integer.toString(getJobs().length));

        // TODO: add some more info

        return result;
    }

    public void setAttributes(Map<String, String> newAttributes)
            throws ZorillaException {
        if (newAttributes.size() > 0) {
            throw new ZorillaException("update of attributes not supported yet");
        }
    }

}
