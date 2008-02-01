package nl.vu.zorilla;

import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public final class Main {

    public static final long NODE_SHUTDOWN_TIMEOUT = 2 * 1000;

    private static Logger logger = Logger.getLogger(Main.class);

    private static class Shutdown extends Thread {
        private final Node node;

        Shutdown(Node node) {
            this.node = node;
        }

        public void run() {
            logger.info("shutdown hook triggered");

            node.stop(0);
        }
    }

    private static class Terminator implements SignalHandler {
        private final Node node;

        Terminator(Node node) {
            this.node = node;
        }

        public void handle(Signal signal) {
            logger.debug("SIGUSR2 catched, shutting down");

            node.stop(NODE_SHUTDOWN_TIMEOUT);
            System.exit(0);
        }
    }

    private static void printUsage(PrintStream out) {
        Map<String, String> properties = Config.getPropertyDescriptions();

        out.println("Zorilla usage:");
        out.println();

        out.println("--config_dir DIR | -c");
        out.println("\t" + properties.get(Config.CONFIG_DIR));

        out.println("--node-id UUID ");
        out.println("\t" + properties.get(Config.NODE_ID));

        out.println("--node-name NAME");
        out.println("\t" + properties.get(Config.NODE_NAME));

        out.println("--cluster-name NAME");
        out.println("\t" + properties.get(Config.CLUSTER_NAME));

        out.println("--network-name NAME");
        out.println("\t" + properties.get(Config.NETWORK_NAME));

        out.println("--peers PEERS | -p");
        out.println("\t" + properties.get(Config.NETWORK_NAME));

        out.println("--port");
        out.println("\t" + properties.get(Config.PORT));

        out.println("--zoni-port");
        out.println("\t" + properties.get(Config.ZONI_PORT));

        out.println("--www-port");
        out.println("\t" + properties.get(Config.WWW_PORT));

        out.println("--discovery-port");
        out.println("\t" + properties.get(Config.DISCOVERY_PORT));

        out.println("--max-runtime");
        out.println("\t" + properties.get(Config.MAX_RUNTIME));

        out.println("--firewall");
        out.println("\tdeny all incoming connections to this node");

        out.println("--no-firewall");
        out.println("\tallow incoming connections to this node (DEFAULT)");

        out.println("--no_native_jobs");
        out
                .println("\tdeny running native applications on this node (DEFAULT)");

        out.println("--native-jobs");
        out.println("\tenable the running of native applications by this node");
        out.println("\tWARNING: SECURITY RISK!");

        out.println("--random-ports");
        out.println("\t shortcut to set all the port options to 0");
        
        out.println();
        out.println("PROPERTY=VALUE\t\tSet a property, as if it was set in a configuration");
        out.println("\t\t\tfile or as a System property.");
        out.println();


        out.println("--config-properties");
        out
                .println("\tprint a list of all valid configuration properties of Zorilla");
        out
                .println("\tthat can be used in the config file, or set as a system property");

        out.println("-? | -h | -help | --help");
        out.println("\tprint this message");
    }

    // MAIN FUNCTION
    public static void main(String[] args) {
        Properties commandLineProperties = new Properties();

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--config-dir")
                    || args[i].equalsIgnoreCase("-c")) {
                i++;
                commandLineProperties.put(Config.CONFIG_DIR, args[i]);
            } else if (args[i].equalsIgnoreCase("--node-id")) {
                i++;
                commandLineProperties.put(Config.NODE_ID, args[i]);
            } else if (args[i].equalsIgnoreCase("--node-name")) {
                i++;
                commandLineProperties.put(Config.NODE_NAME, args[i]);
            } else if (args[i].equalsIgnoreCase("--cluster-name")) {
                i++;
                commandLineProperties.put(Config.CLUSTER_NAME, args[i]);
            } else if (args[i].equalsIgnoreCase("--network-name")) {
                i++;
                commandLineProperties.put(Config.NETWORK_NAME, args[i]);
            } else if (args[i].equalsIgnoreCase("--peers")
                    || args[i].equalsIgnoreCase("-p")) {
                i++;
                commandLineProperties.put(Config.PEERS, args[i]);
            } else if (args[i].equalsIgnoreCase("--port")) {
                i++;
                commandLineProperties.put(Config.PORT, args[i]);
            } else if (args[i].equalsIgnoreCase("--zoni-port")) {
                i++;
                commandLineProperties.put(Config.ZONI_PORT, args[i]);
            } else if (args[i].equalsIgnoreCase("--www-port")) {
                i++;
                commandLineProperties.put(Config.WWW_PORT, args[i]);
            } else if (args[i].equalsIgnoreCase("--discovery-port")) {
                i++;
                commandLineProperties.put(Config.DISCOVERY_PORT, args[i]);
            } else if (args[i].equalsIgnoreCase("--max-runtime")) {
                i++;
                commandLineProperties.put(Config.MAX_RUNTIME, args[i]);
            } else if (args[i].equalsIgnoreCase("--firewall")) {
                commandLineProperties.put(Config.FIREWALL, "true");
            } else if (args[i].equalsIgnoreCase("--no-firewall")) {
                commandLineProperties.put(Config.FIREWALL, "false");
            } else if (args[i].equalsIgnoreCase("--native-jobs")) {
                commandLineProperties.put(Config.NATIVE_JOBS, "true");
            } else if (args[i].equalsIgnoreCase("--no-native_jos")) {
                commandLineProperties.put(Config.NATIVE_JOBS, "false");
            } else if (args[i].equalsIgnoreCase("--random-ports")) {
                commandLineProperties.put(Config.PORT, "0");
                commandLineProperties.put(Config.ZONI_PORT, "0");
                commandLineProperties.put(Config.WWW_PORT, "0");
                commandLineProperties.put(Config.DISCOVERY_PORT, "0");
            } else if (args[i].equalsIgnoreCase("--config-properties")) {
                Map<String, String> properties = Config
                        .getPropertyDescriptions();
                for (Map.Entry<String, String> property : properties.entrySet()) {
                    System.out.println(property.getKey());
                    System.out.println("\t" + property.getValue());
                }
                System.exit(0);
            } else if ((args[i].equalsIgnoreCase("--help")
                    || args[i].equalsIgnoreCase("-h") || args[i]
                    .equalsIgnoreCase("-?"))) {
                printUsage(System.out);
                System.exit(0);
            } else if (args[i].startsWith("-")) {
                System.err.println("unknown command line option: " + args[i]);
                System.err.println();
                printUsage(System.err);
                System.exit(1);
            } else if (args[i].contains("=")) {
                String[] parts = args[i].split("=", 2);
                commandLineProperties.setProperty(parts[0], parts[1]);
            } else {
                System.err.println("unknown command line option: " + args[i]);
                System.err.println();
                printUsage(System.err);
                System.exit(1);
            }
        }

        Node node = null;
        try {
            node = new Node(commandLineProperties);
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
            logger.warn("could not install handler for USR2 signal");
        }

        node.run();

    }

}
