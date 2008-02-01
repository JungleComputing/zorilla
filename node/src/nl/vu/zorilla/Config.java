package nl.vu.zorilla;

import ibis.util.TypedProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

    private static final Logger logger = Logger.getLogger(Config.class);

    private static final String[] propertiesList = {

            Config.CONFIG_DIR,
            "Location of configuration and other files of zorilla",
            ".zorilla",

            Config.NODE_ID,
            "UUID of this node",
            null,

            Config.NODE_NAME,
            "Name of this node, user friendly but not necessarily unique",
            null,

            Config.CLUSTER_NAME,
            "Name of the cluster of this Zorilla node",
            null,

            Config.NETWORK_NAME,
            "Name of Zorilla network to join",
            "default.network",

            Config.PEERS,
            "Comma seperated list of peer nodes used for bootstrapping",
            null,

            Config.PORT,
            "TCP port number used for the P2P network",
            "5444",

            Config.ZONI_PORT,
            "TCP port for connections from clients (users,applications) to Zorilla",
            "5445",

            Config.WWW_PORT,
            "TCP port used for the web interface",
            "5446",

            Config.DISCOVERY_PORT,
            "UDP port used for discovery of other nodes on the local network",
            "5447",

            Config.MAX_RUNTIME,
            "Maximum runtime (in seconds) of this node",
            null,

            Config.FIREWALL,
            "if set to \"true\" this node will not accept incoming connections",
            "false",
            
            Config.DISCONNECT_TIME,
            "number of seconds before this node is \"disconnected\"",
            "-1",
            
            Config.RECONNECT_TIME,
            "number of seconds before this node is \"reconnected\"",
            "-1",
            
            Config.WAN_DISCONNECT,
            "boolean: if set to \"true\" the node will only disconnect from " +
            "all nodes in other clusters, but will still be connected to the" +
            "local network",
            "false",

            Config.NATIVE_JOBS,
            "if set to \"true\" this node will also run native (non-java) jobs",
            "false",
            
            Config.MESSAGE_LOSS_PERCENTAGE,
            "percentage of messages lost by the gossiping service",
            "0",
            
            Config.BOOTSTRAP_TIMEOUT,
            "maximum bootstrap time (in seconds) for the gossiping algorithm",
            "0",
            

    };

    public static final String PREFIX = "zorilla.";

    public static final String CONFIG_DIR = PREFIX + "config.dir";

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

    public static final String FIREWALL = PREFIX + "firewall";
    
    public static final String DISCONNECT_TIME = PREFIX + "disconnect.time";
    public static final String RECONNECT_TIME = PREFIX + "reconnect.time";

    public static final String WAN_DISCONNECT = PREFIX + "wan.disconnect";

    public static final String NATIVE_JOBS = PREFIX + "native.jobs";
    
    public static final String MESSAGE_LOSS_PERCENTAGE = PREFIX + "message.loss.percentage";

    public static final String BOOTSTRAP_TIMEOUT = PREFIX + "bootstrap.timeout";
    
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
        Properties defaultProperties = getDefaultProperties();
        Properties fileProperties = new Properties(defaultProperties);

        Properties systemProperties = new Properties(fileProperties);
        // copy systemproperties to new map
        systemProperties.putAll(System.getProperties());

        // make propeties stack defaults of this properties object
        defaults = systemProperties;

        // add user properties to top-level properties
        putAll(userProperties);

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
    
    private static String[] getValidKeys() {
        ArrayList<String> result = new ArrayList<String>();
        
        for (int i = 0; i < propertiesList.length; i += 3) {
            result.add(propertiesList[i]);
        }
        
        return result.toArray(new String[0]);
    }

    /**
     * Create a properties object with all properties and their default value
     */
    private static Properties getDefaultProperties() {
        Properties result = new Properties();

        // break down the string list into seperate fields
        for (int i = 0; i < propertiesList.length; i += 3) {
            if (propertiesList[i + 2] != null) {
                result.setProperty(propertiesList[i], propertiesList[i + 2]);
            }
        }

        return result;
    }

    /**
     * Returns a map of all valid porperty keys and their descriptions and
     * default values
     */
    public static Map<String, String> getPropertyDescriptions() {
        Map<String, String> result = new HashMap<String, String>();

        for (int i = 0; i < propertiesList.length; i += 3) {
            result.put(propertiesList[i], propertiesList[i + 1]);
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