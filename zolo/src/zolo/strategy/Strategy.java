package zolo.strategy;

import zolo.*;

import org.apache.commons.configuration.*;


public class Strategy {

    protected HierarchicalConfiguration config;
    protected StrategyManager manager;
    protected Output out = new Output();
    public Strategy strategyObj;

    /**
     * Add config
     *
     * @param config   The configuratiuon object
     */
    public void setConfig(HierarchicalConfiguration config) {
        this.config = config;
        strategyObj = this;
    }

    /**
     * Set the manager (to report to)
     *
     * @param manager   The manager object
     */
    public void setManager(StrategyManager manager) {
        this.manager = manager;
    }

    /**
     * Get the name of the strategy
     *
     * @return   The name of the strategy
     */
    public String getName() {
        return config.getString("name");
    }

    /**
     * Run the strategy
     */
    public void run() throws ZoloException { }

    /**
     * Check conditions
     */
    public void check() { }

}
