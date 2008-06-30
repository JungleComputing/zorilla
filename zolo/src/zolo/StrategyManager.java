package zolo;

import zolo.strategy.*;
import zolo.starter.*;

import org.apache.commons.configuration.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.awt.*;
import java.net.URL;
import java.io.File;
import java.net.MalformedURLException;


public class StrategyManager {

    private HashMap<Strategy, StrategyState> strategyState;
    private Output out = new Output();
    private XMLConfiguration config;
    private ZorillaState zorillaState;
    private Starter starter;


    /**
     * Constructor
     *
     * @param   config   The configuratiuon object
     */
    public StrategyManager(XMLConfiguration config) {
        this.config = config;
        strategyState = new HashMap<Strategy, StrategyState>();
        zorillaState = ZorillaState.DEAD;

        // Initialize Zorilla Starter
        initializeStarter();
    }


    /**
     * Initialize Zorilla Starter
     */
    private void initializeStarter() {

        // Location and working dir are hardcoded here, due to strange
        // configuration error
        String location = "http://www.cs.vu.nl/~ndrost/zorilla-lib.zip";
        String workingDir = "./tmp";

        try {
            starter = new Starter(new URL(location), new File(workingDir));
        } catch (MalformedURLException e) {
            out.error(e, true);
        }
    }


    /**
     * Add strategy
     *
     * @param   strategyConfiguration   The configuratiuon object
     */
    public void addStrategy(HierarchicalConfiguration strategyConfiguration) throws ZoloException {
        String strategyClassName = strategyConfiguration.getString("class");

        try {
            // Dynamically load strategy
            Class c = Class.forName("zolo.strategy." + strategyClassName);
            Strategy strategy = (Strategy) c.newInstance();

            // Strategy default status => NO_GO
            strategyState.put(strategy, StrategyState.NO_GO);

            // Set references to configuration and the manager
            strategy.setConfig(strategyConfiguration);
            strategy.setManager(this);
        } catch(ClassNotFoundException cnfe) {
            throw new ZoloException("Strategy class " + strategyClassName + " not found");
        } catch(Exception e) {
            throw new ZoloException("Error while instatiating class " + strategyClassName +
                                    ": " + e.getMessage());
        }
    }

    /**
     * Get number of loaded strategies
     *
     * @return   number   Number of loaded strategies
     */
    public int getNumLoadedStrategies() {
        return strategyState.size();
    }

    /**
     * Start all the strategies
     */
    public void run() {
        // Start strategies
        Iterator it = strategyState.keySet().iterator();
        while (it.hasNext()) {
            try {
                ((Strategy) it.next()).run();
            } catch (ZoloException ze) {
                out.error(ze, true);
            }
        }
    }

    /**
     * Report strategy state back to the strategy manager
     *
     * @param   strategy  The strategy object
     * @param   state     The strategy state
     */
    public boolean reportState(Strategy strategy, StrategyState state) {
        out.verbose("Received state " + state + " from strategy " + strategy.getName() + ".", true);
        strategyState.put(strategy, state);

        // State changed, check states to see if Zorilla needs to be started or paused
        check();

        return true;
    }

    /**
     * Check if all conditions are met and start or pause Zorilla
     */
    public void check() {
        out.verbose("Checking strategy states.", true);

        try {
            // No NO_GO's => RUN
            if (!strategyState.containsValue(StrategyState.NO_GO)) {
                startZorilla();
                return;
            }

            // Operator = OR && at least one GO => RUN
            if (config.getString("settings.strategy-check-operator").equals("OR") &&
                    strategyState.containsValue(StrategyState.GO)) {
                startZorilla();
                return;
            }

            // Else => PAUSE
            pauseZorilla();
        } catch(ZoloException ze) {
            out.error(ze, true);
        } catch(Exception e) {
            out.error(e, true);
        }
    }


    /**
     * Determine the OS
     *
     * @return   string   Name of OS in capitals
     */
    public String getOS() {
        String os = System.getProperty("os.name");
        return (os.toUpperCase().equals("LINUX")) ? "Linux" : "Windows";
    }


    /**
     * Emit a speaker beep when Zorilla's state changes.
     *
     * @param   number   Number of beeps to emit
     */
    private void beep(int number) {

        // Check configuration
        if (!config.getString("settings.beep").toUpperCase().equals("TRUE")) {
            return;
        }

        int i;
        for(i = 0; i < number; i++) {
        //Commented out: when user is logged out, AWT is unavailable
/*
            try {
                Toolkit.getDefaultToolkit().beep();
            } catch (Exception e) { }
*/
            if (number > 1) {
                try {
                    Thread.sleep(500);
                } catch(InterruptedException ie) {
                    out.error(ie, true);
                }
            }
        }
    }


    /**
     * Start Zorilla
     *
     * @return   Zorilla started?
     */
    private boolean startZorilla() throws ZoloException {
        out.verbose("Switch to Running state... ", false);
        switch (zorillaState) {
            case PAUSED:
                zorillaState = ZorillaState.RUNNING;
                beep(1);
                // @todo: UNPAUSE ZORILLA
                out.verbose("done.", true);
                break;
            case DEAD:
                zorillaState = ZorillaState.RUNNING;
                beep(1);
                starter.run();
                out.verbose("done.", true);
                break;
            case RUNNING:
                out.verbose("Zorilla already running.", true);
                break;
            default:
                throw new ZoloException("Undefined Zorilla state transition");
        }

        return true;
    }

    /**
     * Pause Zorilla
     *
     * @return   Zorilla paused?
     */
    private boolean pauseZorilla() throws ZoloException {
        out.verbose("Switch to Pause state... ", false);
        switch (zorillaState) {
            case RUNNING:
                zorillaState = ZorillaState.PAUSED;
                beep(2);
                // @todo: PAUSE ZORILLA
                out.verbose("done.", true);
                break;
            case PAUSED:
                out.verbose("Zorilla already paused.", true);
                break;
            case DEAD:
                out.verbose("Zorilla is not running.", true);
                break;
            default:
                throw new ZoloException("Undefined Zorilla state transition");
        }

        return true;
    }

    /**
     * Stop Zorilla
     *
     * @return   Zorilla stopped?
     */
    public boolean stopZorilla() throws ZoloException {
        out.verbose("Now STOPPING Zorilla Starter...", false);
        switch (zorillaState) {
            case RUNNING:
                zorillaState = ZorillaState.DEAD;
                beep(3);
                // @todo: STOP ZORILLA
                out.verbose("Zorilla is stopped.", true);
                break;
            case PAUSED:
                zorillaState = ZorillaState.DEAD;
                beep(3);
                // @todo: STOP ZORILLA
                out.verbose("Zorilla is stopped.", true);
                break;
            case DEAD:
                out.verbose("Zorilla is not running.", true);
                break;
            default:
                throw new ZoloException("Undefined Zorilla state transition");
        }

        return true;
    }
}
