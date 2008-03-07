package ibis.zorilla.apps;

import ibis.zorilla.zoni.ZoniConnection;
import ibis.zorilla.zoni.ZoniProtocol;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;


public final class Zap {

    private static final Logger logger = Logger.getLogger(Zap.class);

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
        int port = ZoniProtocol.DEFAULT_PORT;

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
            InetSocketAddress nodeSocketAddress = new InetSocketAddress(
                InetAddress.getByName(null), ZoniProtocol.DEFAULT_PORT);

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                    || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = parseSocketAddress(command[i]);
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
                null, ZoniProtocol.TYPE_CLIENT);

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
