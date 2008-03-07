package ibis.zorilla.apps;

import ibis.zorilla.zoni.ZoniConnection;
import ibis.zorilla.zoni.ZoniProtocol;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;


public final class Zet {

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
            InetSocketAddress nodeSocketAddress = new InetSocketAddress(
                InetAddress.getByName(null), ZoniProtocol.DEFAULT_PORT);

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-na")
                    || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = parseSocketAddress(command[i]);
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

            Map<String,String> attributes = new HashMap<String,String>();

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
                null, ZoniProtocol.TYPE_CLIENT);

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
