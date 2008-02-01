package nl.vu.zorilla;

import ibis.util.IPUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Properties;

import nl.vu.zorilla.io.ZorillaPrintStream;
import nl.vu.zorilla.util.TypedProperties;
import nl.vu.zorilla.zoni.ZoniProtocol;

/**
 * @author Niels Drost
 * 
 * Class representing configuration parameters of Zorilla. It is possible to
 * override parameters using a config file or system properties. The default
 * location of the config file is {user.home}/.zorilla but it is possible to
 * override this location by setting the "zorilla.config.file" system property
 * 
 * Setting preference order:
 * <UL>
 * <ITEM>System property</ITEM> <ITEM>Config file</ITEM> <ITEM>Default</ITEM>
 * </UL>
 * 
 */
public class Config {

    public static final String[][] validProperties = {

            { "home.dir", "location of configuration and other files of zorilla" },

            { "bamboo",
                    "if set to '', '1' or 'true' zorilla will use bamboo as a P2P network" },
            { "node.address", "(bamboo only) ip address of Zorilla node" },
            {
                    "network.interface",
                    "(bamboo only) name of the network interface (eth0, eth1, ...)\n\tused by the P2P network" },

            {
                    "network.name",
                    "name of P2P network (a Zorilla node will only connect to a network\n\twith a idential name)" },
            {
                    "cluster",
                    "name of the cluster of this Zorilla node (usually the domain\n\tof this machine)" },
            { "home.address",
                    "ip:[port] address of a statistics collecting node or application" },
            {
                    "peers",
                    "comma seperated list of one or more addresses (ip:[port])\n\tof peers in the P2P network used for bootstrapping the network" },

            { "port", "UDP port used by the P2P network in Zorilla" },
            { "broadcast.port",
                    "UDP port used for node discovery broadcast messages" },
            { "www.port",
                    "TCP port of build-in webserver for status of a Zorilla node" },
            { "zoni.port",
                    "TCP port for connections from client (applications) to Zorilla" },

            { "tmp.dir", "directory to put temporary files of zorilla in" },

            { "max.job.lifetime", "maximum lifetime of any job in milliseconds" },
            { "memory",
                    "memory available for zorilla (in bytes, K, M and G suffixes supported)" },
            { "diskspace",
                    "memory available for zorilla (in bytes, K, M and G suffixes supported)" },
            { "processors", "number of processors available for zorilla" },
            {
                    "native.jobs",
                    "if set to '', '1' or 'true' zorilla will support running\n\tnative applications (WARNING: security risk!)" },

    };

    public static final long JOB_STATE_EXPIRATION_TIMEOUT = 30 * 60 * 1000;

    public static final long JOB_STATE_REFRESH_TIMEOUT = 60 * 1000;

    public static final long JOB_WAIT_TIMEOUT = 1 * 1000;

    public static final long JOB_MIN_ADVERT_TIMEOUT = 5 * 1000;

    public static final long JOB_MAX_ADVERT_TIMEOUT = 60 * 1000;

    public static final int MAX_ADVERT_RADIUS = 15;

    public static final int FILE_BLOCK_SIZE = 256 * 1024;

    public static final int FILE_DOWNLOAD_ATTEMPTS = 10;

    public static final int FILE_PRIMARY_DOWNLOAD_TRESHOLD = 2;

    public static final long JOB_CLUSTERING_TIMEOUT = 10 * 1000;

    public static final long WORKER_ADD_TIMEOUT = 10 * 1000;

    // how long to wait before ending a node upon receiving a kill...
    public static final long NETWORK_KILL_TIMEOUT = 10 * 1000;

    // maximum shutting down on receiving a user signal
    public static final int NODE_SHUTDOWN_TIMEOUT = 10 * 1000;

    // clean up and do maintenance once every minute
    public static final int NODE_MAINTENANCE_INTERVAL = 60 * 1000;

    // timeout for Job connections.
    public static final long JOB_PORT_CONNECT_TIMEOUT = 2 * 1000;

    public static final long CONSTITUENT_EXPIRATION_TIMEOUT = 1200 * 1000;

    public static final long CALL_TIMEOUT = 10 * 1000;

    public static final long DEFAULT_WORKER_MEM = 512 * 1024 * 1024; // 512Mb

    public static final long DEFAULT_WORKER_DISKSPACE = 10 * 1024 * 1024; // 10Mb

    // 15 minutes
    public static final long DEFAULT_JOB_LIFETIME = 15 * 60 * 1000;

    // buffers used for files transfers et al.
    public static final int BUFFER_SIZE = 100 * 1024;

    private final File homeDir;
    
    private final File logDir;

    private final File tmpDir;
    
    private final File configFile;

    private final Resources resources;

    private final int port;

    private final int wwwPort;

    private final int zoniPort;

    private final int broadcastPort;

    private final String networkName;

    // User supplied address and interface for node.
    private final InetAddress nodeAddress;

    private final String networkInterface;

    private final InetSocketAddress[] peers;

    private final InetSocketAddress home;

    private final boolean bamboo;

    private final String cluster;

    private final long maxJobLifetime;

    private final boolean nativeJobs;

    Config(Properties userProperties) throws ZorillaException,
            NumberFormatException {
        TypedProperties defaultProperties = getDefaultProperties();
        Properties fileProperties = new Properties(defaultProperties);
        Properties systemProperties = new Properties(fileProperties);

        // copy system properties
        systemProperties.putAll(System.getProperties());

        TypedProperties properties = new TypedProperties(systemProperties);
        // add user properties to top-level properties
        properties.putAll(userProperties);

        homeDir = new File(properties.getProperty("zorilla.home.dir"));
        homeDir.mkdirs();
        
        logDir = new File(homeDir, "logs");
        logDir.mkdirs();
  
        configFile = new File(homeDir, "config");
        
        try {
            FileInputStream configFileStream = new FileInputStream(configFile);
            fileProperties.load(configFileStream);
        } catch (FileNotFoundException e) {
            System.err.println("WARNING: could not open config file: "
                    + configFile);
        } catch (IOException e) {
            throw new ZorillaException("could not read config file: "
                    + configFile, e);
        }

        String[] validPropKeys = new String[validProperties.length];
        for (int i = 0; i < validProperties.length; i++) {
            validPropKeys[i] = validProperties[i][0];
        }

        // check if there are any other properties then the default properties
        // defined
        properties.checkProperties("zorilla.", validPropKeys, null, true);

        // fetch all property values from the properties object

      
        if (!logDir.isDirectory()) {
            throw new ZorillaException("cannot create log dir: " + logDir);
        }

        tmpDir = new File(properties.getProperty("zorilla.tmp.dir"));
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();

        if (!tmpDir.isDirectory()) {
            throw new ZorillaException("cannot create temp dir: " + tmpDir);
        }
        
        resources = new Resources(1, properties
                .getIntProperty("zorilla.processors"), properties
                .getSizeProperty("zorilla.memory"), properties
                .getSizeProperty("zorilla.diskspace"));

        port = properties.getIntProperty("zorilla.port");

        if (port < 0) {
            throw new NumberFormatException("port cannot be negative: " + port);
        }

        wwwPort = properties.getIntProperty("zorilla.www.port");

        if (wwwPort < 0) {
            throw new NumberFormatException("www port cannot be negative"
                    + " number: " + wwwPort);
        }

        zoniPort = properties.getIntProperty("zorilla.zoni.port");

        if (zoniPort < 0) {
            throw new NumberFormatException("zoni port cannot be negative"
                    + " number: " + zoniPort);
        }

        broadcastPort = properties.getIntProperty("zorilla.broadcast.port");

        if (broadcastPort < 0) {
            throw new NumberFormatException("broadcast port cannot be negative"
                    + broadcastPort);
        }

        int defaultPort = defaultProperties.getIntProperty("zorilla.port");

        nodeAddress = parseAddress(properties
                .getProperty("zorilla.node.address"));

        peers = parseSocketAddresses(properties
                .getProperty("zorilla.peers"), defaultPort);
        home = parseSocketAddress(properties
                .getProperty("zorilla.home.address"), defaultPort);

        networkName = properties.getProperty("zorilla.network.name");

        bamboo = properties.booleanProperty("zorilla.bamboo");

        networkInterface = properties.getProperty("zorilla.network.interface");

        if (networkInterface != null) {
            System.setProperty("ibis.util.ip.interface", networkInterface);
        }

        maxJobLifetime = properties.getLongProperty("zorilla.max.job.lifetime");

        nativeJobs = properties.booleanProperty("zorilla.native.jobs");

        cluster = properties.getProperty("zorilla.cluster", getDomain());
    }

    private static int parsePort(String string) throws ZorillaException {
        if (string == null) {
            throw new ZorillaException("port cannot be null");
        }

        int port = 0;
        try {
            port = Integer.parseInt(string);
        } catch (NumberFormatException e) {
            throw new ZorillaException("invalid port: " + string);
        }

        if (port <= 0) {
            throw new ZorillaException("invalid port "
                    + "(must be non-zero positive number): " + string);
        }
        return port;
    }

    private static InetAddress parseAddress(String string)
            throws ZorillaException {
        if (string == null) {
            return null;
        }

        try {
            return InetAddress.getByName(string);
        } catch (UnknownHostException e) {
            throw new ZorillaException("invalid address: " + string);
        }
    }

    private static InetSocketAddress[] parseSocketAddresses(String string,
            int defaultPort) throws ZorillaException {
        if (string == null) {
            return new InetSocketAddress[0];
        }

        String[] strings = string.split(",");

        ArrayList<InetSocketAddress> result = new ArrayList<InetSocketAddress>();

        for (int i = 0; i < strings.length; i++) {
            InetSocketAddress address = parseSocketAddress(strings[i],
                    defaultPort);
            if (address != null) {
                result.add(address);
            }
        }
        return result.toArray(new InetSocketAddress[0]);
    }

    private static InetSocketAddress parseSocketAddress(String string,
            int defaultport) throws ZorillaException {
        if (string == null) {
            return null;
        }

        int port = defaultport;

        String[] strings = string.split(":");

        if (strings.length > 2) {
            throw new ZorillaException("illegal address format: " + string);
        } else if (strings.length == 2) {
            // format was "host:port, extract port number"
            port = parsePort(strings[1]);
        }

        return new InetSocketAddress(parseAddress(strings[0]), port);
    }

    private static String getDomain() {
        String canonicalHostname = IPUtils.getLocalHostAddress()
                .getCanonicalHostName();
        int dotIndex = canonicalHostname.indexOf('.');
        if (dotIndex == -1) {
            return canonicalHostname;
        } else {
            return canonicalHostname.substring(dotIndex + 1);
        }
    }

    /**
     * Create a properties object with all properties and their default value
     */
    private static TypedProperties getDefaultProperties() {
        TypedProperties result = new TypedProperties();

        String userHome = System.getProperty("user.home");

        result.setProperty("zorilla.home.dir", userHome  + File.separator + ".zorilla");

        result.setProperty("zorilla.tmp.dir", System
                .getProperty("java.io.tmpdir")
                + File.separator + Node.generateUUID());

        // resources available for workers
        result.setProperty("zorilla.processors", "1");
        result.setProperty("zorilla.memory", "512M");
        result.setProperty("zorilla.diskspace", "1G");

        // main p2p network (bamboo or otherwise)
        result.setProperty("zorilla.port", "5444");

        // port for zoni program connections (5445?)
        result.setProperty("zorilla.zoni.port", String
                .valueOf(ZoniProtocol.DEFAULT_PORT));

        // port for web interface
        result.setProperty("zorilla.www.port", "5446");

        // port for discovering other zorilla nodes (usually UDP)
        result.setProperty("zorilla.broadcast.port", "5447");

        result.setProperty("zorilla.network.name", "default");

        result.setProperty("zorilla.bamboo", "true");

        // 1 day
        result.setProperty("zorilla.max.job.lifetime", Long
                .toString(24 * 60 * 60 * 1000));

        result.setProperty("zorilla.native.jobs", "false");

        return result;

    }

    public int getBroadcastPort() {
        return broadcastPort;
    }

    public int getClientPort() {
        return zoniPort;
    }

    public InetSocketAddress getHomePeer() {
        return home;
    }

    public File getLogDir() {
        return logDir;
    }

    public File getHomeDir() {
        return homeDir;
    }
    
    public String getNetworkName() {
        return networkName;
    }

    public InetAddress getNodeAddress() {
        return nodeAddress;
    }

    public InetSocketAddress[] getPeers() {
        return peers;
    }

    public int getPort() {
        return port;
    }

    public Resources getResources() {
        return resources;
    }

    public int getWwwPort() {
        return wwwPort;
    }

    public boolean useBamboo() {
        return bamboo;
    }

    public String getCluster() {
        return cluster;
    }

    public String getNetworkInterface() {
        return networkInterface;
    }

    public long getMaxJobLifetime() {
        return maxJobLifetime;
    }

    public boolean nativeJobs() {
        return nativeJobs;
    }

    public void print(ZorillaPrintStream out) throws IOException {

        out.println("\tconfig file = " + configFile);
        
        out.println("\thome dir = " + homeDir);
        out.println("\tlog dir = " + logDir);
        out.println("\ttmp dir = " + tmpDir);

        
        if (bamboo) {
            out.println("\tP2P network is bamboo");
        } else {
            out.println("\tP2P network is native Zorilla network");
        }
        out.println("\tavailalable " + resources.toString());
        out.println("\tP2P network port (UDP) = " + port);
        out.println("\tweb web interface port (TCP) = " + wwwPort);
        out.println("\tzoni port (TCP) = " + zoniPort);
        out.println("\tbroadcast port (UDP) = " + broadcastPort);
        out.println("\tnetwork name = " + networkName);

        out.println("\tnode address = " + nodeAddress);
        out.println("\tnetwork interface = " + networkInterface);

        if (peers.length > 0) {
            out.println("\tpeers:");
            for (int i = 0; i < peers.length; i++) {
                out.println("\t\t" + peers[i]);
            }
        } else {
            out.println("\tpeers: none");
        }
        out.println("\thome (statistics gathering) address = " + home);

        out.println("\tcluster = " + cluster);

        out.println("\tmax job lifeTime = " + maxJobLifetime + " milliseconds");

        out.println("\tnative jobs allowed = " + nativeJobs);

    }

    public static void printProperties(PrintStream out) {
        out.println("Overview of Zorilla configuration properties");
        out.println();
        for (int i = 0; i < validProperties.length; i++) {
            out.println("zorilla." + validProperties[i][0] + "\n\t"
                    + validProperties[i][1]);
        }
    }

    public static String getPropertyDescription(String property) {
        for (int i = 0; i < validProperties.length; i++) {
            if (validProperties[i][0].equals(property)) {
                return validProperties[i][1];
            }
        }
        return "no description found";
    }
}