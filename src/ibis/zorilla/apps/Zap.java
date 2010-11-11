package ibis.zorilla.apps;

import java.util.ArrayList;
import java.util.UUID;

import ibis.zorilla.Config;
import ibis.zorilla.api.JobInterface;
import ibis.zorilla.api.RemoteNode;

public final class Zap {

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
        ArrayList<UUID> ids = new ArrayList<UUID>();
        boolean killNode = false;
        boolean killNetwork = false;
        int port = RemoteNode.DEFAULT_PORT;

        try {

            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i].equals("-p") || arguments[i].equals("--port")) {
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
                        System.err.println("unrecognized option: "
                                + arguments[i]);
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

            RemoteNode node = new RemoteNode(port);

            if (killNode) {
                System.out.println("killing node");

                node.end();

                return;
            } else if (killNetwork) {
                System.out.println("killing network");

                node.killNetwork();

                return;
            }

            if (ids.size() == 0) {
                System.out.println("killing all jobs");
                for(JobInterface job: node.getJobs()) {
                    System.out.println("killing job: " + job);
                    job.cancel();
                }
            
            } else {
                for(UUID id: ids){
                    JobInterface job = node.getJob(id);
                    System.out.println("killing job: " + job);
                    job.cancel();
                }
            }

            // close connection

        } catch (Exception e) {
            System.err.println("exception on handling request: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
