/*
 * Zolo, the Zorilla Loader
 *
 * Bachelor Project - Machine Utilization Strategies for Desktop Grids
 * June 2008
 * Wouter Leenards <wls400@cs.vu.nl>
 *
 */

package zolo;

import org.apache.commons.cli.*;
import org.apache.commons.configuration.*;

import java.util.Iterator;
import java.util.List;


public class Zolo {

    static StrategyManager manager;
    static Output out;

    /**
     * Constructor
     */
    public Zolo() {
        manager = new StrategyManager(new XMLConfiguration());
        out = new Output();
    }

    /**
     * Main function
     *
     * @param args   Comand line arguments
     */
    public static void main(String[] args) {
        start(args);
    }

    public static void start(String[] args) {

        // Shutdown hook: Kill Zorilla when Zolo is killed
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    // Stop Zorilla
                    manager.stopZorilla();
                } catch(ZoloException ze) {
                    out.error(ze, true);
                }
            }
        });

        try {
            Options opt = new Options();

            // Add CLI options
            opt.addOption("h", "help", false, "Print help for this application");
            opt.addOption("v", "verbose", false, "Explain what is being done");
            opt.addOption("c", true, "[config file] Load config file from a user defined location");

            BasicParser parser = new BasicParser();

            HelpFormatter f = new HelpFormatter();

            CommandLine cl = parser.parse(opt, args);

            new Zolo();
            // Output (verbose)
            out.setVerbosity(cl.hasOption('v'));

            // Help needed
            if (cl.hasOption('h')) {
                f.printHelp("zolo [OPTION]...", opt);
                System.exit(1);
            }

            // Fancy logo (Thanks to http://www.network-science.de/ascii)
            out.verbose("", true);
            out.verbose("ZZZZZ        lll", true);
            out.verbose("   ZZ  oooo  lll  oooo", true);
            out.verbose("  ZZ  oo  oo lll oo  oo", true);
            out.verbose(" ZZ   oo  oo lll oo  oo", true);
            out.verbose("ZZZZZ  oooo  lll  oooo", true);
            out.verbose("", true);

            // Determine config file to use
            String configFile = new String();
            if (cl.hasOption('c')) {
                configFile = cl.getOptionValue("c");
                out.verbose("Loading user defined config file '" + configFile + "'... ", false);
            } else {
                configFile = "zolo-config.xml";
                out.verbose("Loading default config file... ", false);
            }

            // Determine OS
            String strategiesXMLPath = (manager.getOS().equals("Linux")) ?
                "strategies.linux.strategy" :
                "strategies.windows.strategy";

            try {
                // Load configuration file
                XMLConfiguration config = new XMLConfiguration(configFile);
                config.setThrowExceptionOnMissing(true);
                out.verbose("done.", true);

                // Recreate manager with right config
                manager = new StrategyManager(config);

                out.verbose("Adding strategies...", true);

                // Loop through all strategies defined in configuration file
                List strategies = config.configurationsAt(strategiesXMLPath);
                for (Iterator it = strategies.iterator(); it.hasNext();) {
                    HierarchicalConfiguration strategyConfiguration = (HierarchicalConfiguration) it.next();
                    String strategyName = strategyConfiguration.getString("name");
                    out.verbose("-> Strategy " + strategyName + "... ", false);
                    Boolean strategyActive = new Boolean(strategyConfiguration.getString("active"));

                    if (strategyActive == true) {
                        // Strategy is active, add it to the manager
                        try {
                            manager.addStrategy(strategyConfiguration);
                        } catch (ZoloException ze) {
                            out.error(ze, true);
                        }
                        out.verbose("done.", true);
                    } else {
                        // Strategy is not active, do not add it to the manager
                        out.verbose("not added (inactive)", true);
                    }
                }
            } catch(ConfigurationException ce) {
                out.error(ce, true);
            }

        } catch (ParseException pe) {
            out.error(pe, true);
        }

        // Print number of strategies added
        int numStrategies = manager.getNumLoadedStrategies();
        out.verbose(numStrategies +  " strateg" + ((numStrategies == 1) ? "y" : "ies") + " added.", true);

        // Start manager
        out.verbose("Starting manager...", true);
        manager.run();
    }


    /**
     * Stop Zorilla (used by Java Service Wrapper / Command class)
     */
    public static void stop() {
        try {
            //Stop Zorilla
            manager.stopZorilla();
        } catch(ZoloException ze) {
            out.error(ze, true);
        }
    }
}
