package ibis.zorilla.apps;

import ibis.smartsockets.virtual.VirtualSocketFactory;
import ibis.zorilla.JobInfo;

import java.util.Map;

import org.apache.log4j.Logger;

public final class Zee {

    private static final Logger logger = Logger.getLogger(Zee.class);

    private static void usage() {
        System.out
                .println("Prints out information on a zorilla node and/or running jobs."
                        + "\nA filter on which jobs to print information on can be specified"
                        + "\n"
                        + "usage:  Zee [OPTION].. [JOB]..."
                        + "\n-na,  --node_address IP:PORT  address of node"
                        + "\n-j                            print out information on jobs (DEFAULT)"
                        + "\n-n                            print out information on node"
                        + "\n-v                            print out verbose information");
    }

    private static void printJobInfo(String job, ZoniConnection connection,
            boolean verbose) throws Exception {

        JobInfo info = connection.getJobInfo(job);

        Map<String, String> attributes = info.getAttributes();
        Map<String, String> status = info.getStatus();

        if (verbose) {
            System.out.println("JOB " + info.getJobID());
            System.out.println("  executable = " + info.getExecutable());
            System.out.println("  attributes: ");
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                System.out.println("     " + entry.getKey() + " = "
                        + entry.getValue());
            }

            System.out.println("  status: ");
            for (Map.Entry<String, String> entry : status.entrySet()) {
                System.out.println("     " + entry.getKey() + " = "
                        + entry.getValue());
            }
            System.out.println();
        } else {
            System.out.println(info.getJobID() + "\t" + status.get("phase")
                    + "\t" + status.get("total.workers"));
        }
    }

    public static void main(String[] command) {
        boolean verbose = false;
        boolean printNodeInfo = false;
        boolean printJobInfo = false;
        int jobIndex = -1;
        String hub = null;

        try {
            String nodeSocketAddress = "localhost:" + ZoniProtocol.DEFAULT_PORT;

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                        || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = command[i];
                } else if (command[i].equals("-v")) {
                    verbose = true;
                } else if (command[i].equals("-h")) {
                    i++;
                    hub = command[i];
                } else if (command[i].equals("-n")) {
                    printNodeInfo = true;
                } else if (command[i].equals("-j")) {
                    printJobInfo = true;
                } else if (command[i].equals("--help")) {
                    usage();
                    return;
                } else {
                    // unrecognized option.
                    if (command[i].startsWith("-")) {
                        System.err
                                .println("unrecognized option: " + command[i]);
                        usage();
                        System.exit(1);
                    }

                    jobIndex = i;
                    break;
                }
            }

            if (!printJobInfo && !printNodeInfo) {
                // print job info by default
                printJobInfo = true;
            }

            VirtualSocketFactory factory = ZoniConnection.getFactory(hub);
            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                    factory, null);


            if (printNodeInfo) {

                System.out.println("Node info:");

                for (Map.Entry<String, String> entry : connection.getNodeInfo()
                        .entrySet()) {

                    System.out.println("  " + entry.getKey() + " = "
                            + entry.getValue());
                }

                if (printJobInfo) {
                    // empty line between node and job status
                    System.out.println();
                }

            }

            if (printJobInfo) {

                logger.debug("getting job list");

                String[] jobIDs = connection.getJobList();

                if (jobIDs.length == 0) {
                    System.out.println("no jobs");
                    connection.close();
                    return;
                }

                if (!verbose) {
                    System.out.println("JOB-ID                              "
                            + "    PHASE           CURRENT_NR_OF_WORKERS");
                }

                if (jobIndex == -1) {
                    for (int i = 0; i < jobIDs.length; i++) {
                        printJobInfo(jobIDs[i], connection, verbose);
                    }
                } else {
                    for (int i = jobIndex; i < command.length; i++) {
                        for (int j = 0; j < jobIDs.length; j++) {
                            if (jobIDs[j].toString().toLowerCase().startsWith(
                                    command[i].toLowerCase())) {
                                printJobInfo(jobIDs[i], connection, verbose);
                            }
                        }
                    }
                }
            }

            connection.close();
            factory.end();
        } catch (Exception e) {
            System.err.println("exception on running zee: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
