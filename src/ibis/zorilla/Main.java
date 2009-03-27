package ibis.zorilla;

import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

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

    private static void printUsage(PrintStream out) {
        Map<String, String> properties = Config.getDescriptions();

        out.println("Zorilla usage:");
        out.println();

        out.println("--config-dir DIR | -c");
        out.println("\t" + properties.get(Config.CONFIG_DIR));

        out.println("--cluster-name NAME");
        out.println("\t" + properties.get(Config.CLUSTER_NAME));

        out.println("--network-name NAME");
        out.println("\t" + properties.get(Config.NETWORK_NAME));

        out.println("--peers ADDRESS[,ADDRESS]... | -p");
        out.println("\t" + properties.get(Config.PEERS));

        out.println("--port PORT_NR");
        out.println("\t" + properties.get(Config.PORT));

//        out.println("--zoni-port PORT_NR");
//        out.println("\t" + properties.get(Config.ZONI_PORT));

        out.println("--www-port PORT_NR");
        out.println("\t" + properties.get(Config.WWW_PORT));

//        out.println("--discovery-port PORT_NR");
//        out.println("\t" + properties.get(Config.DISCOVERY_PORT));

        out.println("--no-native-jobs");
        out
                .println("\tdeny running native applications on this node (DEFAULT)");

        out.println("--native-jobs");
        out.println("\tenable the running of native applications by this node");
        out.println("\tWARNING: SECURITY RISK!");

        out.println("--max-workers WORKERS");
        out
                .println("\tmaximum number of workers for this node. Defaults to number");
        out.println("\tof processors available");

        out.println("--random-ports");
        out.println("\t shortcut to set all the port options to 0");

        out.println("--master");
        out
                .println("\t make this node a master node (does not have any workers)");

        out.println("--worker");
        out
                .println("\t make this node a worker node (does not accept job submissions)");

        out.println();
        out.println("PROPERTY=VALUE");
        out.println("\tSet a property, as if it was set in a configuration");
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
//            } else if (args[i].equalsIgnoreCase("--zoni-port")) {
//                i++;
//                commandLineProperties.put(Config.ZONI_PORT, args[i]);
            } else if (args[i].equalsIgnoreCase("--www-port")) {
                i++;
                commandLineProperties.put(Config.WWW_PORT, args[i]);
//            } else if (args[i].equalsIgnoreCase("--discovery-port")) {
//                i++;
//                commandLineProperties.put(Config.DISCOVERY_PORT, args[i]);
            } else if (args[i].equalsIgnoreCase("--native-jobs")) {
                commandLineProperties.put(Config.NATIVE_JOBS, "true");
            } else if (args[i].equalsIgnoreCase("--no-native-jobs")) {
                commandLineProperties.put(Config.NATIVE_JOBS, "false");
            } else if (args[i].equalsIgnoreCase("--max-workers")) {
                i++;
                commandLineProperties.put(Config.AVAILABLE_CORES, args[i]);
            } else if (args[i].equalsIgnoreCase("--random-ports")) {
                commandLineProperties.put(Config.PORT, "0");
//                commandLineProperties.put(Config.ZONI_PORT, "0");
                commandLineProperties.put(Config.WWW_PORT, "0");
//                commandLineProperties.put(Config.DISCOVERY_PORT, "0");
            } else if (args[i].equalsIgnoreCase("--worker")) {
                commandLineProperties.put(Config.WORKER, "true");
            } else if (args[i].equalsIgnoreCase("--master")) {
                commandLineProperties.put(Config.MASTER, "true");
            } else if (args[i].equalsIgnoreCase("--config-properties")) {
                Map<String, String> properties = Config.getDescriptions();
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

        node.run();

    }

}
