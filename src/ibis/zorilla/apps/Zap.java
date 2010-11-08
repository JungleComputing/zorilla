package ibis.zorilla.apps;

import ibis.zorilla.Config;
import ibis.zorilla.NodeInterface;
import ibis.zorilla.rpc.LocalSocketRPC;

import org.apache.log4j.Logger;

public final class Zap {

    private static final Logger logger = Logger.getLogger(Zap.class);

    private static void usage() {
        System.out
                .println("zap: Kills running Zorilla jobs(default) or nodes\n"
                        + "usage:  zap [OPTION]... [JOB_TO_CANCEL].."
                        + "\n-p,  --port PORT port of local zorilla node"
                        + "\n-k                            Kill the node"
                        + "\n-K                            kill the entire node network"
                        + "\n"
                        + "\nAll jobs with the given prefix(es) are cancelled"
                        + "\nIf no job is specified, all jobs are cancelled");
    }

    public static void main(String[] arguments) {
        int jobIndex = -1;
        boolean killNode = false;
        boolean killNetwork = false;
        int port = Config.DEFAULT_PORT;
        
        try {

            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i].equals("-p")
                        || arguments[i].equals("--port")) {
                    i++;
                    port = Integer.parseInt(arguments[i]);
                } else if (arguments[i].equals("-k")) {
                    killNode = true;
                } else if (arguments[i].equals("-K")) {
                    killNetwork = true;
                } else if (arguments[i].equals("--help")) {
                    usage();
                    return;
                } else {
                    // unrecognized option.
                    if (arguments[i].startsWith("-")) {
                        System.err
                                .println("unrecognized option: " + arguments[i]);
                        usage();
                        System.exit(1);
                    }

                    jobIndex = i;
                    break;
                }
            }
            
            NodeInterface node = LocalSocketRPC.createProxy(
                    NodeInterface.class, "zorilla node", port);
            
            if (killNode) {
                System.out.println("killing node");

                node.end();

                return;
            } else if (killNetwork) {
                System.out.println("killing network");

                node.killNetwork();
                
                return;
            }

//            logger.debug("getting job list");
//
//            String[] jobIDs = connection.getJobList();
//
//            if (jobIDs.length == 0) {
//                System.out.println("no jobs");
//            } else if (jobIndex == -1) {
//                for (int i = 0; i < jobIDs.length; i++) {
//                    System.out.println("killing job " + jobIDs[i]);
//                    connection.cancelJob(jobIDs[i]);
//                }
//            } else {
//                for (int i = jobIndex; i < command.length; i++) {
//                    for (int j = 0; j < jobIDs.length; j++) {
//                        if (jobIDs[j].toString().toLowerCase().startsWith(
//                                command[i].toLowerCase())) {
//                            System.out.println("killing job " + jobIDs[i]);
//                            connection.cancelJob(jobIDs[i]);
//                        }
//                    }
//                }
//            }

            // close connection
   
            
        } catch (Exception e) {
            System.err.println("exception on handling request: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
