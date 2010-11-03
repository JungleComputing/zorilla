package ibis.zorilla.apps;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.util.rpc.RPC;
import ibis.ipl.util.rpc.Example.ExampleInterface;
import ibis.smartsockets.virtual.VirtualSocketFactory;
import ibis.zorilla.NodeInterface;

import org.apache.log4j.Logger;

public final class Zap {

    private static final Logger logger = Logger.getLogger(Zap.class);

    public static IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);
    
    private static void usage() {
        System.out
                .println("zap: Kills running Zorilla jobs(default) or nodes\n"
                        + "usage:  zap [OPTION]... [JOB_TO_CANCEL].."
                        + "\n-na,  --node_address IP:PORT  address of node"
                        + "\n-k                            Kill the node"
                        + "\n-K                            kill the entire node network"
                        + "\n"
                        + "\nAll jobs with the given prefix(es) are cancelled"
                        + "\nIf no job is specified, all jobs are cancelled");
    }

    public static void main(String[] command) {
        int jobIndex = -1;
        boolean killNode = false;
        boolean killNetwork = false;
        String hub = null;

        
        
        try {
            String nodeAddress = "localhost";

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                        || command[i].equals("--node_address")) {
                    i++;
                    nodeAddress = command[i];
                } else if (command[i].equals("-h")) {
                    i++;
                    hub = command[i];
                } else if (command[i].equals("-k")) {
                    killNode = true;
                } else if (command[i].equals("-K")) {
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

            Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null,
                    RPC.rpcPortTypes);

            IbisIdentifier server = ibis.registry().getElectionResult("zorilla");
            
            NodeInterface node = RPC.createProxy(
                    NodeInterface.class, server, "zorilla node", ibis);
            
            if (killNode) {
                System.out.println("killing node");

                node.end();
                
                ibis.end();
                return;
            } else if (killNetwork) {
                System.out.println("killing network");

                node.killNetwork();
                
                ibis.end();
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
            ibis.end();
            
        } catch (Exception e) {
            System.err.println("exception on handling request: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
