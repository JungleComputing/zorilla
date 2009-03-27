package ibis.zorilla.apps;

import ibis.zorilla.zoni.ZoniConnection;
import org.apache.log4j.Logger;


public final class Zap {

    private static final Logger logger = Logger.getLogger(Zap.class);

    private static void usage() {
        System.out
            .println("zap: Kills running Zorilla jobs(default) or nodes\n"
                + "usage:  zap [OPTION]... [JOB_TO_CANCEL].."
                + "\n-na,  --node_address IP:PORT  address of node"
                + "\n-k                            Kill the node"
                + "\n-K                            kill the entire node network"
                + "\n" + "\nAll jobs with the given prefix(es) are cancelled"
                + "\nIf no job is specified, all jobs are cancelled");
    }

    public static void main(String[] command) {
        int jobIndex = -1;
        boolean killNode = false;
        boolean killNetwork = false;

        try {
            String nodeSocketAddress = "localhost";

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                    || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = command[i];
                } else if (command[i].equals("-k")) {
                    killNode = true;
                } else if (command[i].equals("-K")) {
                    killNode = true;
                    killNetwork = true;
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

            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                null);

            if (killNode) {
                System.out.println("killing node (kill network = "
                    + killNetwork + ")");

                connection.kill(killNetwork);
                return;
            }

            logger.debug("getting job list");

            String[] jobIDs = connection.getJobList();

            if (jobIDs.length == 0) {
                System.out.println("no jobs");
            } else if (jobIndex == -1) {
                for (int i = 0; i < jobIDs.length; i++) {
                    System.out.println("killing job " + jobIDs[i]);
                    connection.cancelJob(jobIDs[i]);
                }
            } else {
                for (int i = jobIndex; i < command.length; i++) {
                    for (int j = 0; j < jobIDs.length; j++) {
                        if (jobIDs[j].toString().toLowerCase().startsWith(
                            command[i].toLowerCase())) {
                            System.out.println("killing job " + jobIDs[i]);
                            connection.cancelJob(jobIDs[i]);
                        }
                    }
                }
            }

            // close connection
            connection.close();
        } catch (Exception e) {
            System.err.println("exception on handling request: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
