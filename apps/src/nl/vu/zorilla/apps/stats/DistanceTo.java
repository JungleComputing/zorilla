package nl.vu.zorilla.apps.stats;

import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import nl.vu.zorilla.bigNet.Coordinates;
import nl.vu.zorilla.stats.Stats;
import nl.vu.zorilla.stats.StatsGatherer;
import nl.vu.zorilla.stats.StatsHandler;

public class DistanceTo implements StatsHandler {
    
    private SortedMap<String, Double> icmpPingDistances; 

    // node-id, number of neighbours
    private SortedMap<String, Coordinates> dataMap;

    private String homeName;
    
    
    private static SortedMap<String, Double> fillDistances() {
        TreeMap<String, Double> result = new TreeMap<String, Double>();

        result.put("sedna", 0.0);
        result.put("das2.cs.vu.nl",  0.259);
        result.put("das2.liacs.nl", 1.79);
        result.put("das2.nikhef.nl",  1.037);
        result.put("das2.ewi.tudelft.nl", 2.295);
        result.put("das2.phys.uu.nl", 2.61 );
        
        return result;
    }

    public DistanceTo(int port) throws Exception {
        icmpPingDistances = fillDistances();
        homeName = null;

        dataMap = new TreeMap<String, Coordinates>();

        new StatsGatherer(port, this);

        // print list once in a while
        while (true) {
            printList();

            Thread.sleep(5000);
        }

    }

    private synchronized void printList() {
        Coordinates home = null;

        System.out.println(new Date());

        if (homeName != null) {
            home = dataMap.get(homeName);
            System.out.println(homeName + " coordinates are " + home);
        }

        if (home == null) {
            home = new Coordinates();
        }

        for (Map.Entry<String, Coordinates> dataMapEntry : dataMap.entrySet()) {
            String nodeName = dataMapEntry.getKey();
            double referenceDistance = Double.NaN;
            for (Map.Entry<String, Double> pingMapEntry: icmpPingDistances.entrySet()) {
                if (nodeName.split(":")[0].endsWith(pingMapEntry.getKey())) {
                    referenceDistance = pingMapEntry.getValue();
                }
            }
            System.out.printf("%-40s %-40s %6.2f %6.2f\n", nodeName, dataMapEntry
                    .getValue(), dataMapEntry.getValue().distance(home), referenceDistance);
        }
    }

    public synchronized void handle(Stats stats) {
        try {
            String nodeName = stats.get("node", "name").toString();

            if (homeName == null) {
                // first node in becomes "home" node
                homeName = nodeName;
            }

            Coordinates coordinates = (Coordinates) stats.get("network", "coordinate");

            dataMap.put(nodeName, coordinates);
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
        int port = 0;

        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        } else if (args.length != 0) {
            System.err.println("usage: DistanceTo PORT");
            System.exit(1);
        }

        try {
            new DistanceTo(port);
        } catch (Exception e) {
            System.err.println("error on gathering statistics");
            e.printStackTrace(System.err);
        }
    }

}
