package ibis.zorilla;

import ibis.util.TypedProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

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
public class Config extends TypedProperties {

    private static final long serialVersionUID = 1L;

    public static final String PREFIX = "zorilla.";

    public static final String CONFIG_DIR = PREFIX + "config.dir";

    public static final String PROPERTIES = PREFIX + "properties";

    public static final String NODE_ID = PREFIX + "node.id";

    public static final String NODE_NAME = PREFIX + "node.name";

    public static final String CLUSTER_NAME = PREFIX + "cluster.name";

    public static final String NETWORK_NAME = PREFIX + "network.name";

    public static final String PEERS = PREFIX + "peers";

    public static final String PORT = PREFIX + "port";

    public static final String ZONI_PORT = PREFIX + "zoni.port";

    public static final String WWW_PORT = PREFIX + "www.port";

    public static final String DISCOVERY_PORT = PREFIX + "discovery.port";

    public static final String MAX_RUNTIME = PREFIX + "max.runtime";

    public static final String MAX_WORKERS = PREFIX + "max.workers";

    public static final String FIREWALL = PREFIX + "firewall";

    public static final String DISCONNECT_TIME = PREFIX + "disconnect.time";

    public static final String RECONNECT_TIME = PREFIX + "reconnect.time";

    public static final String WAN_DISCONNECT = PREFIX + "wan.disconnect";

    public static final String NATIVE_JOBS = PREFIX + "native.jobs";

    public static final String MESSAGE_LOSS_PERCENTAGE =
        PREFIX + "message.loss.percentage";

    public static final String BOOTSTRAP_TIMEOUT = PREFIX + "bootstrap.timeout";

    public static final String ADDITIONAL_GOSSIP_ALGORITHMS =
        PREFIX + "additional.gossip.algorithms";

    public static final String GOSSIP_INTERVAL = PREFIX + "gossip.interval";

    public static final String GOSSIP_CACHE_SIZE = PREFIX + "gossip.cache.size";

    public static final String GOSSIP_SEND_SIZE = PREFIX + "gossip.send.size";

    public static final String DEFAULT_FLOOD_METRIC =
        PREFIX + "default.flood.metric";

    public static final String MAX_CLUSTER_SIZE = PREFIX + "max.cluster.size";

    private static final Logger logger = Logger.getLogger(Config.class);

    private static final String[][] propertiesList =
        {

                { CONFIG_DIR, ".zorilla",
                        "Location of configuration and other files of zorilla" },

                { NODE_ID, null, "UUID of this node" },

                { NODE_NAME, null,
                        "Name of this node, user friendly but not necessarily unique" },

                { CLUSTER_NAME, null,
                        "Name of the cluster of this Zorilla node" },

                { NETWORK_NAME, "default.network",
                        "Name of Zorilla network to join" },

                { PEERS, null,
                        "Comma seperated list of peer nodes used for bootstrapping" },

                { PORT, "5444", "TCP port number used for the P2P network" },

                { ZONI_PORT, "5445",
                        "TCP port for connections from clients (users,applications) to Zorilla" },

                { WWW_PORT, "5446", "TCP port used for the web interface" },

                { DISCOVERY_PORT, "5447",
                        "UDP port used for discovery of other nodes on the local network" },

                { MAX_RUNTIME, null,
                        "Maximum runtime (in seconds) of this node" },

                {
                        MAX_WORKERS,
                        null,
                        "Maximum number of workers on this node (defaults to number of processors available)" },

                { FIREWALL, "false",
                        "if set to \"true\" this node will not accept incoming connections" },

                { DISCONNECT_TIME, "-1",
                        "number of seconds before this node is \"disconnected\"" },

                { RECONNECT_TIME, "-1",
                        "number of seconds before this node is \"reconnected\"" },

                {
                        WAN_DISCONNECT,
                        "false",
                        "boolean: if set to \"true\" the node will only disconnect from "
                                + "all nodes in other clusters, but will still be connected to the"
                                + "local network" },

                { NATIVE_JOBS, "false",
                        "if set to \"true\" this node will also run native (non-java) jobs" },

                { MESSAGE_LOSS_PERCENTAGE, "0",
                        "percentage of messages lost by the gossiping service" },

                { BOOTSTRAP_TIMEOUT, "0",
                        "maximum bootstrap time (in seconds) for the gossiping algorithm" },

                { ADDITIONAL_GOSSIP_ALGORITHMS, "false",
                        "boolean: if true, additional gossiping algorithms will be run" },

                { GOSSIP_INTERVAL, "1",
                        "Integer: average number of seconds between two consecutive gossip attempts" },

                { GOSSIP_CACHE_SIZE, "100", "Integer: size of gossip cache" },
                { GOSSIP_SEND_SIZE, "30",
                        "Integer: number of items to gossip each gossip round" },

                { DEFAULT_FLOOD_METRIC, "neighbours",
                        "Default metric used to send out flood messages" },

                { MAX_CLUSTER_SIZE, "1000", "Maximum size of a cluster" },

        };

    // public static final long JOB_STATE_EXPIRATION_TIMEOUT = 30 * 60 * 1000;
    // public static final long JOB_STATE_REFRESH_TIMEOUT = 60 * 1000;
    // public static final long JOB_WAIT_TIMEOUT = 1 * 1000;
    // public static final long JOB_MIN_ADVERT_TIMEOUT = 5 * 1000;
    // public static final long JOB_MAX_ADVERT_TIMEOUT = 60 * 1000;
    // public static final int MAX_ADVERT_RADIUS = 15;
    // public static final int FILE_BLOCK_SIZE = 256 * 1024;
    // public static final int FILE_DOWNLOAD_ATTEMPTS = 10;
    // public static final int FILE_PRIMARY_DOWNLOAD_TRESHOLD = 2;
    // public static final long JOB_CLUSTERING_TIMEOUT = 10 * 1000;
    // public static final long WORKER_ADD_TIMEOUT = 10 * 1000;
    // public static final long NETWORK_KILL_TIMEOUT = 10 * 1000;
    // public static final int NODE_SHUTDOWN_TIMEOUT = 10 * 1000;
    // public static final int NODE_MAINTENANCE_INTERVAL = 60 * 1000;
    // public static final long JOB_PORT_CONNECT_TIMEOUT = 2 * 1000;
    // public static final long CONSTITUENT_EXPIRATION_TIMEOUT = 1200 * 1000;
    // public static final long CALL_TIMEOUT = 10 * 1000;
    // public static final long DEFAULT_WORKER_MEM = 512 * 1024 * 1024; // 512Mb
    // public static final long DEFAULT_WORKER_DISKSPACE = 10 * 1024 * 1024; //
    // 10Mb
    // public static final long DEFAULT_JOB_LIFETIME = 15 * 60 * 1000;
    // public static final int BUFFER_SIZE = 100 * 1024;

    private final File configDir;

    Config(Properties userProperties) throws Exception {
        Properties defaultProperties = getHardcodedProperties();
        Properties fileProperties = new Properties(defaultProperties);

        Properties systemProperties = new Properties(fileProperties);
        // copy systemproperties to new map
        systemProperties.putAll(System.getProperties());

        // make propeties stack defaults of this properties object
        defaults = systemProperties;

        // add user properties to top-level properties
        putAll(userProperties);

        // load from classpath
        try {
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            InputStream inputStream =
                classLoader.getResourceAsStream(PROPERTIES);
            if (inputStream != null) {
                fileProperties.load(inputStream);
                logger.debug("loaded " + fileProperties.size() + " properties from classpath");
            }
        } catch (IOException e) {
            logger.warn("could not load properties from classpath", e);
        }

        File configDir = getFileProperty(Config.CONFIG_DIR);
        if (!configDir.isAbsolute()) {
            // make absolute by resolving against user home directory
            String userHome = System.getProperty("user.home");
            configDir = new File(userHome, configDir.getPath());
        }
        this.configDir = configDir;

        File configFile = new File(configDir, "config");
        try {
            FileInputStream configFileStream = new FileInputStream(configFile);
            fileProperties.load(configFileStream);
        } catch (FileNotFoundException e) {
            logger.warn("Could not open config file: " + configFile);
        } catch (IOException e) {
            throw new Exception("Could not read config file: " + configFile, e);
        }

        checkProperties("zorilla.", getValidKeys(), null, true);

    }

    public File getConfigDir() {
        return configDir;
    }

    /**
     * Returns the built-in properties of Ibis.
     * 
     * @return the resulting properties.
     */
    public static Properties getHardcodedProperties() {
        Properties properties = new Properties();

        for (String[] element : propertiesList) {
            if (element[1] != null) {
                properties.setProperty(element[0], element[1]);
            }
        }

        return properties;
    }

    private static String[] getValidKeys() {
        ArrayList<String> result = new ArrayList<String>();

        for (int i = 0; i < propertiesList.length; i++) {
            result.add(propertiesList[i][0]);
        }

        return result.toArray(new String[0]);
    }

    /**
     * Returns a map mapping hard-coded property names to their descriptions.
     * 
     * @return the name/description map.
     */
    public static Map<String, String> getDescriptions() {
        Map<String, String> result = new LinkedHashMap<String, String>();

        for (String[] element : propertiesList) {
            result.put(element[0], element[2]);
        }

        return result;
    }

    public File getFileProperty(String key) {
        String property = getProperty(key);

        if (property == null) {
            return null;
        }

        return new File(getProperty(key));
    }

}