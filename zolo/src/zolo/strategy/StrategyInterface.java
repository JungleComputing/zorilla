package zolo.strategy;

import zolo.*;

import org.apache.commons.configuration.*;

interface StrategyInterface {

    public void setConfig(HierarchicalConfiguration config);

    public void setManager(StrategyManager manager);

    public String getName();

    public void run() throws ZoloException;

    public void check();
}
