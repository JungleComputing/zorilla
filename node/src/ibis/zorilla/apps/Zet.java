package ibis.zorilla.apps;

import ibis.zorilla.zoni.ZoniConnection;
import java.util.HashMap;
import java.util.Map;

public final class Zet {

    private static void usage() {
        System.out
                .println("Set attributes of jobs or a node. "
                        + "Defaults to setting node attributes"
                        + "\n"
                        + "usage:  Zet [OPTION]... [ATTRIBUTE=NEW_VALUE]..."
                        + "\n--node_address IP:PORT | -na   address of node"
                        + "\n--job JOB | -j                 specify which job to set attributes of"
                        + "\n--pause | -p                   set max workers to 0 (pause job execution)"
                        + "\n--resume | -r                  set max workers back to previous value"
                        + "\n                               (resume job execution)");

    }

    public static void main(String[] command) {
        String job = null;
        int attribute = -1;
        boolean pause = false;
        boolean resume = false;

        try {

            String nodeSocketAddress = "localhost";

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                        || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = command[i];
                } else if (command[i].equals("-j")
                        || command[i].equals("--job")) {
                    i++;
                    job = command[i];
                } else if (command[i].equals("-p")
                        || command[i].equals("--pause")) {
                    pause = true;
                } else if (command[i].equals("-r")
                        || command[i].equals("--resume")) {
                    resume = true;
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

                    attribute = i;
                    break;
                }
            }

            Map<String, String> attributes = new HashMap<String, String>();

            
            if (attribute != -1) {
                for (int i = attribute; i < command.length; i++) {
                    String[] parts = command[i].split("=");
                    if (parts.length != 2) {
                        System.err
                                .println("attribute should be defined as ATTRIBUTE=VALUE (not "
                                        + command[i] + ")");
                        System.exit(1);
                    }
                    attributes.put(parts[0], parts[1]);
                }
            }

            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                    null);

            if (attribute != -1) {
            if (job == null) {
                connection.setNodeAttributes(attributes);
            } else {
                connection.setJobAttributes(job, attributes);
            }
            }
            
            if (pause) {
                System.err.println("PAUSE! :(");
            } else if (resume) {
                System.err.println("RESUME! :)");
            }

            connection.close();
        } catch (Exception e) {
            System.err.println("exception on changing settings: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
