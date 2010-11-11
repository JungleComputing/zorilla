package ibis.zorilla.apps;

import ibis.zorilla.api.JobInterface;
import ibis.zorilla.api.RemoteNode;
import ibis.zorilla.api.ZorillaJobDescription;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public final class Zee {

    private static void usage() {
        System.out
                .println("Prints out information on a zorilla node and/or running jobs."
                        + "\nA filter on which jobs to print information on can be specified"
                        + "\n"
                        + "usage:  Zee [OPTION].. [JOB]..."
                        + "\n-p,  --port PORT              port of local zorilla node"
                        + "\n-j                            print out information on jobs (DEFAULT)"
                        + "\n-n                            print out information on node"
                        + "\n-v                            print out verbose information");
    }

    private static void printJobInfo(JobInterface job, boolean verbose) throws Exception {

        ZorillaJobDescription description = job.getDescription();

        if (verbose) {
            System.out.println("JOB " + job.getID());
            System.out.println("  phase = " + job.getPhase());
            System.out.println(description.toMultilineString());
            
            System.out.println();
        } else {
            System.out.println(job.getID() + "\t" + job.getPhase());
        }
    }

    public static void main(String[] arguments) {
        boolean verbose = false;
        boolean printNodeInfo = false;
        boolean printJobInfo = false;

        ArrayList<UUID> ids = new ArrayList<UUID>();
        int port = RemoteNode.DEFAULT_PORT;

        try {
            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i].equals("-p") || arguments[i].equals("--port")) {
                    i++;
                    port = Integer.parseInt(arguments[i]);
                } else if (arguments[i].equals("-v")) {
                    verbose = true;
             
                } else if (arguments[i].equals("-n")) {
                    printNodeInfo = true;
                } else if (arguments[i].equals("-j")) {
                    printJobInfo = true;
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
                    
                    try {
                        ids.add(UUID.fromString(arguments[i]));
                    } catch (IllegalArgumentException e) {
                        System.err.println("expecting UUID, got: "
                                + arguments[i]);
                        usage();
                        System.exit(1);
                    }
                    break;
                }
            }

            if (!printJobInfo && !printNodeInfo) {
                // print job info by default
                printJobInfo = true;
            }

            RemoteNode node = new RemoteNode(port);

            if (printNodeInfo) {

                System.out.println("Node info:");

                for (Map.Entry<String, String> entry : node.getStats().entrySet()) {

                    System.out.println("  " + entry.getKey() + " = "
                            + entry.getValue());
                }

                if (printJobInfo) {
                    // empty line between node and job status
                    System.out.println();
                }

            }

            if (printJobInfo) {
                if (ids.size() == 0) {
                    //print info for all jobs
                    
                    JobInterface[] jobs = node.getJobs();
                    
                    if (jobs.length == 0) {
                        System.out.println("no jobs");
                        return;
                    }
                    
                    for(JobInterface job: jobs) {
                        printJobInfo(job, verbose);
                    }
                } else {
                    //print info for specified jobs
                    
                    for(UUID id: ids){
                        JobInterface job = node.getJob(id);
                        
                        printJobInfo(job, verbose);
                    }
                }
            }
                
        } catch (Exception e) {
            System.err.println("exception on running zee: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
