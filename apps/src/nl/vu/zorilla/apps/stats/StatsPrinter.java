package nl.vu.zorilla.apps.stats;

import java.util.Date;
import java.util.Map;

import nl.vu.zorilla.stats.Stats;
import nl.vu.zorilla.stats.StatsGatherer;
import nl.vu.zorilla.stats.StatsHandler;

public class StatsPrinter implements StatsHandler {

    public StatsPrinter(int port) throws Exception {
        new StatsGatherer(port, this);

        //just do nothing...
        while (true) {
            Thread.sleep(100);
        }

    }

    public void handle(Stats stats) {
        try {
            System.out.println("received stats created at: "
                    + new Date(stats.creationDate()));
            String[] catagories = stats.getCatagories();

            for (String catagory : catagories) {
                Map<String, Object> entries = stats.get(catagory);

                System.out.println(catagory + ":");
                for (Map.Entry<String, Object> entry : entries.entrySet()) {
                    System.out.println("\t" + entry.getKey() + "="
                            + entry.getValue());
                }
            }
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
        }

        try {
            new StatsPrinter(port);
        } catch (Exception e) {
            System.err.println("error on gathering statistics");
            e.printStackTrace(System.err);
        }
    }

}
