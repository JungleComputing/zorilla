package nl.vu.zorilla.apps;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.zorilla.zoni.ZoniConnection;
import nl.vu.zorilla.zoni.JobInfo;
import nl.vu.zorilla.zoni.ZoniException;
import nl.vu.zorilla.zoni.ZoniProtocol;

public final class Zee {

    private static final Logger logger = Logger.getLogger(Zee.class);

    private static int parsePort(String string) {
        int port = 0;
        try {
            port = Integer.parseInt(string);
        } catch (NumberFormatException e) {
            System.err.println("invalid port: " + string);
            System.exit(1);
        }

        if (port <= 0) {
            System.err.println("invalid port "
                + "(must be non-zero positive number): " + string);
        }
        return port;
    }

    private static InetSocketAddress parseSocketAddress(String string) {
        int port = 0;

        String[] strings = string.split(":");

        if (strings.length > 2) {
            System.err.println("illegal address format: " + string);
            System.exit(1);
        } else if (strings.length == 2) {
            // format was "host:port, extract port number"
            port = parsePort(strings[1]);
        }

        InetAddress address = null;
        try {
            address = InetAddress.getByName(strings[0]);
        } catch (UnknownHostException e) {
            System.err.println("invalid address: " + string + " exception: "
                + e);
            System.exit(1);
        }

        return new InetSocketAddress(address, port);
    }

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
        boolean verbose) throws ZoniException, IOException {

        JobInfo info = connection.getJobInfo(job);

        Map attributes = info.getAttributes();
        Map status = info.getStatus();

        if (verbose) {
            System.out.println("JOB " + info.getJobID());
            System.out.println("  executable = " + info.getExecutable());
            System.out.println("  attributes: ");
            Iterator iterator = attributes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                System.out.println("     " + entry.getKey() + " = "
                    + entry.getValue());
            }

            System.out.println("  status: ");
            iterator = status.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                System.out.println("     " + entry.getKey() + " = "
                    + entry.getValue());
            }

        } else {
            System.out.println(info.getJobID() + "\t"  + status
                .get("phase") + "\t" + status.get("current.nr.of.workers"));
        }
    }

    public static void main(String[] command) {
        boolean verbose = false;
        boolean printNodeInfo = false;
        boolean printJobInfo = false;
        int jobIndex = -1;

        try {
            InetSocketAddress nodeSocketAddress = new InetSocketAddress(
                InetAddress.getByName(null), ZoniProtocol.DEFAULT_PORT);

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                    || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = parseSocketAddress(command[i]);
                } else if (command[i].equals("-v")) {
                    verbose = true;
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

            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                null, ZoniProtocol.TYPE_CLIENT);

            if (printNodeInfo) {

                Map nodeInfo = connection.getNodeInfo();

                System.out.println("Node info:");

                Iterator iterator = nodeInfo.entrySet().iterator();

                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();

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
                        + "PHASE          CURRENT_NR_OF_WORKERS");
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
        } catch (Exception e) {
            System.err.println("exception on running job: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
