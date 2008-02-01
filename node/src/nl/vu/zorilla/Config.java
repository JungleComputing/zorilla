package nl.vu.zorilla;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import smartsockets.direct.SocketAddressSet;

import nl.vu.zorilla.io.ZorillaPrintStream;
import nl.vu.zorilla.util.Resources;
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

            { "home.dir",
                    "location of configuration and other files of zorilla" },

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

            { "port", "UDP/TCP port used by the P2P network in Zorilla" },
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

    private final File tmpDir;

    private final File configDir;

    private final Resources resources;

    private final int port;

    private final int wwwPort;

    private final int zoniPort;

    private final int broadcastPort;

    private final String networkName;

    // User supplied address and interface for node.
    private final InetAddress nodeAddress;

    private final String networkInterface;

    private final boolean bamboo;

    private final String cluster;

    private final long maxJobLifetime;

    private final boolean nativeJobs;

    private final TypedProperties properties;

    Config(Properties userProperties, UUID nodeID) throws Exception {
        TypedProperties defaultProperties = getDefaultProperties();
        Properties fileProperties = new Properties(defaultProperties);
        Properties systemProperties = new Properties(fileProperties);

        // copy system properties
        systemProperties.putAll(System.getProperties());

        properties = new TypedProperties(systemProperties);
        // add user properties to top-level properties
        properties.putAll(userProperties);
        
        File userHome = new File(System.getProperty("user.home"));
        configDir = new File(userHome,".zorilla");

        File configFile = new File(configDir, "config");
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

        cluster = properties.getProperty("zorilla.cluster");
        
        if (cluster == null) {
            homeDir = new File(configDir,nodeID.toString());
        } else {
            File clusterDir = new File(configDir, cluster);
            homeDir = new File(clusterDir, nodeID.toString());
        }
        homeDir.mkdirs();

        File systemTmpDir = new File(System.getProperty("java.io.tmpdir"));
        
        tmpDir = new File(systemTmpDir, nodeID.toString());
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();

        if (!tmpDir.isDirectory()) {
            throw new ZorillaException("cannot create temp dir: " + tmpDir);
        }

        // FIXME: make this somehow dynamic
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

        networkName = properties.getProperty("zorilla.network.name");

        bamboo = properties.booleanProperty("zorilla.bamboo");

        networkInterface = properties.getProperty("zorilla.network.interface");

        maxJobLifetime = properties.getLongProperty("zorilla.max.job.lifetime");

        nativeJobs = properties.booleanProperty("zorilla.native.jobs");

        createWorkerSecurityFile(new File(configDir, "worker.security.policy"));
    }

    public TypedProperties getProperties() {
        return properties;
    }

    private static void createWorkerSecurityFile(File file)
            throws ZorillaException {
        if (file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            throw new ZorillaException(
                    "could not create worker security file, already a directory: "
                            + file);
        }

        System.err.println("creating worker security file: " + file);

        try {

            FileWriter writer = new FileWriter(file);

            writer
                    .write("// Zorilla worker security file. All applications will be limited\n"
                            + "// to these permissions when running...\n"
                            + "grant {\n"
                            + "\tpermission java.io.FilePermission \"-\", \"read, write, execute, delete\";\n"
                            + "\tpermission java.net.SocketPermission \"*\", \"resolve,accept,connect,listen\";\n"
                            + "\n"
                            + "\t//for System.getProperties()\n"
                            + "\tpermission java.util.PropertyPermission \"*\", \"read,write\";\n"

                            + "\t//to create Classloaders\n"
                            + "\tpermission java.lang.RuntimePermission \"createClassLoader\";\n"
                            + "\n"
                            + "\t//for overriding serialization code (used in Ibis)\n"
                            + "\tpermission java.io.SerializablePermission \"enableSubclassImplementation\", \"\";\n"
                            + "\tpermission java.lang.reflect.ReflectPermission \"suppressAccessChecks\", \"\";\n"
                            + "\tpermission java.lang.RuntimePermission \"accessClassInPackage.sun.misc\", \"\";\n"
                            + "\tpermission java.lang.RuntimePermission \"accessDeclaredMembers\", \"\";\n"
                            + "};\n");

            writer.flush();
            writer.close();

        } catch (IOException e) {
            throw new ZorillaException("could not create worker security file "
                    + file, e);
        }
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

    /**
     * Create a properties object with all properties and their default value
     */
    private static TypedProperties getDefaultProperties() {
        TypedProperties result = new TypedProperties();


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

        result.setProperty("zorilla.bamboo", "false");

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

    public File getHomeDir() {
        return homeDir;
    }

    public File getConfigDir() {
        return configDir;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public String getNetworkName() {
        return networkName;
    }

    public InetAddress getNodeAddress() {
        return nodeAddress;
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

    public static String getPropertyDescription(String property) {
        for (int i = 0; i < validProperties.length; i++) {
            if (validProperties[i][0].equals(property)) {
                return validProperties[i][1];
            }
        }
        return "no description found";
    }

}