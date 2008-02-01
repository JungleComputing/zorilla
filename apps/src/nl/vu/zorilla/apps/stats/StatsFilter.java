package nl.vu.zorilla.apps.stats;


import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import nl.vu.zorilla.stats.Stats;
import nl.vu.zorilla.stats.StatsGatherer;
import nl.vu.zorilla.stats.StatsHandler;

public class StatsFilter implements StatsHandler {
    
    //node-id, data
    private SortedMap<UUID, String> dataMap;
    
    private final String catagory;
    private final String name;

    public StatsFilter(int port, String catagory, String name) throws Exception {
        this.catagory = catagory;
        this.name = name;
        
        dataMap = new TreeMap<UUID, String>();
        
        new StatsGatherer(port, this);
        
        //just do nothing...
        while (true) {
            printList();
            
            Thread.sleep(5000);
        }

    }
    
    private synchronized void printList() {
        System.out.println(new Date());
                
        for (Map.Entry<UUID, String> entry: dataMap.entrySet()) {
            
            System.out.printf("%-40s %-40s\n", entry.getKey(), entry.getValue());
        }
    }

    public synchronized void handle(Stats stats) {
        try {
            UUID nodeID = stats.getNodeID();
            String neighbours = stats.get(catagory, name).toString();
           
            dataMap.put(nodeID, neighbours);
        } catch (Exception e) {
            System.err.println("error on receiving stats");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.err.println("usage: StatsFilter PORT CATEGORY NAME");
            System.exit(1);
        }
            
        int port = Integer.parseInt(args[0]);
        

        try {
            new StatsFilter(port, args[1], args[2]);
        } catch (Exception e) {
            System.err.println("error on gathering statistics");
            e.printStackTrace(System.err);
        }
    }

}
