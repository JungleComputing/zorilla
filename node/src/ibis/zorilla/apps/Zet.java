package ibis.zorilla.apps;

import ibis.zorilla.zoni.ZoniConnection;
import java.util.HashMap;
import java.util.Map;

public final class Zet {

    private static void usage() {
        System.out.println("Set attributes of jobs or a node. "
                + "Defaults to setting node attributes" + "\n"
                + "usage:  Zet [OPTION]... [ATTRIBUTE=NEW_VALUE]..."
                + "\n-na,  --node_address IP:PORT  address of node"
                + "\n-j JOB                        specify "
                + "which job to set attributes of");
    }

    public static void main(String[] command) {

        String job = null;
        int attribute = -1;

        try {

            String nodeSocketAddress = "localhost";

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                        || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = command[i];
                } else if (command[i].equals("-j")) {
                    i++;
                    job = command[i];
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

            if (attribute == -1) {
                System.err.println("no new attributes specified");
                System.exit(1);
            }

            Map<String, String> attributes = new HashMap<String, String>();

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

            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                    null);

            if (job == null) {
                connection.setNodeAttributes(attributes);
            } else {
                connection.setJobAttributes(job, attributes);
            }

            connection.close();
        } catch (Exception e) {
            System.err.println("exception on changing settings: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
