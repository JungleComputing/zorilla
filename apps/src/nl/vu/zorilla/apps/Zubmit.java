package nl.vu.zorilla.apps;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.zorilla.zoni.ZoniConnection;
import nl.vu.zorilla.zoni.JobInfo;
import nl.vu.zorilla.zoni.ZoniProtocol;

public final class Zubmit {

    private static final Logger logger = Logger.getLogger(Zubmit.class);

    private static String makeAbsolute(String string) {
        File file = new File(string);

        return file.getAbsolutePath();
    }

    private static void addFile(String string, Map inputs) throws Exception {
        logger.debug("string = " + string);

        String prefix = null;

        String[] equalSplit = string.split("=");
        if (equalSplit.length == 2) {
            // virtual path, URI pair
            prefix = equalSplit[0];
            string = equalSplit[1];
        }

        File file = new File(string).getAbsoluteFile();

        inputs.put(prefix, file.getAbsolutePath());
    }

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
                .println("usage: zubmit [OPTION].. EXECUTABLE_URI [JOB_ARGUMENTS]"
                        + "\n\nOptions:"
                        + "\n-na,  --node_address IP:PORT        address of node to submit job to"
                        + "\n-a,  --attribute KEY=VALUE          add key value pair to attributes of job"
                        + "\n-e,  --environment KEY=VALUE        add key value pair to environment of job"
                        + "\n-#w,  --workers N                   specify initial nr of workers (sets nr.of.workers attribute)"
                        + "\n-w,  --wait_until_running           wait until the job starts before exiting"
                        + "\n-W,  --wait_until_done              wait until the job is done before exiting"
                        + "\n                                      also prints stdout and stderr to the console"
                        + "\n                                      and cancels the job when zubmit is interrupted by Ctrl-C"
                        + "\n-v,  --verbose                      print some more info"

                        + "\n\nFiles options:"
                        + "\n-i,  --input [VIRTUAL_PATH=]PATH    add input file"
                        + "\n-o,  --output [VIRTUAL_PATH=]PATH   add output file"
                        + "\n-so, --stdout PATH                  set standard out file"
                        + "\n-si, --stdin PATH                   set standard in file"
                        + "\n-se, --stderr PATH                  set standard error file"

                );
    }

    public static void main(String[] command) {
        boolean waitUntilRunning = false;
        boolean waitUntilDone = false;
        boolean verbose = false;

        try {
            InetSocketAddress nodeSocketAddress = new InetSocketAddress(
                    InetAddress.getByName(null), ZoniProtocol.DEFAULT_PORT);

            Map environment = new HashMap();
            Map attributes = new HashMap();

            Map preStage = new HashMap();
            Map postStage = new HashMap();

            String stdin = null;
            String stdout = null;
            String stderr = null;

            // first option not recognized by this program. Assumed to be
            // executable (URI) of submitted job. URI has java:// scheme
            // in case of java program
            int executableIndex = -1;

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-w")
                        || command[i].equals("--wait_until_running")) {
                    waitUntilRunning = true;
                } else if (command[i].equals("-W")
                        || command[i].equals("--wait_until_done")) {
                    waitUntilDone = true;
                } else if (command[i].equals("-v")
                        || command[i].equals("--verbose")) {
                    verbose = true;

                } else if (command[i].equals("-na")
                        || command[i].equals("--node_address")) {
                    i++;
                    nodeSocketAddress = parseSocketAddress(command[i]);
                } else if (command[i].equals("-i")
                        || command[i].equals("--input")) {
                    i++;
                    addFile(command[i], preStage);
                } else if (command[i].equals("-o")
                        || command[i].equals("--output")) {
                    i++;
                    addFile(command[i], postStage);
                } else if (command[i].equals("-#w")
                        || command[i].equals("--workers")) {
                    i++;
                    attributes.put("nr.of.workers", command[i]);
                } else if (command[i].equals("-si")
                        || command[i].equals("--stdin")) {
                    i++;
                    stdin = makeAbsolute(command[i]);
                } else if (command[i].equals("-so")
                        || command[i].equals("--stdout")) {
                    i++;
                    stdout = makeAbsolute(command[i]);
                } else if (command[i].equals("-se")
                        || command[i].equals("--stderr")) {
                    i++;
                    stderr = makeAbsolute(command[i]);
                } else if (command[i].equals("-a")
                        || command[i].equals("--attribute")) {
                    i++;
                    String[] parts = command[i].split("=");
                    if (parts.length != 2) {
                        System.err
                                .println("attribute should be defined as VARIABLE=VALUE (not "
                                        + command[i] + ")");
                        System.exit(1);
                    }
                    attributes.put(parts[0], parts[1]);
                } else if (command[i].equals("-e")
                        || command[i].equals("--environment")) {
                    i++;
                    String[] parts = command[i].split("=");
                    if (parts.length != 2) {
                        System.err
                                .println("environment variable should be defined as VARIABLE=VALUE (not "
                                        + command[i] + ")");
                        System.exit(1);
                    }
                    environment.put(parts[0], parts[1]);
                } else if (command[i].equals("--help")) {
                    usage();
                    return;
                } else {
                    if (command[i].startsWith("-")) {
                        System.err
                                .println("unrecognized option: " + command[i]);
                        usage();
                        System.exit(1);
                    }
                    executableIndex = i;
                    break;
                }
            }

            if (executableIndex == -1) {
                System.err.println("executable URI not specified");
                System.exit(1);
            }

            String executable = command[executableIndex];

            String[] arguments = new String[(command.length - (executableIndex + 1))];
            int j = 0;
            for (int i = executableIndex + 1; i < command.length; i++) {
                arguments[j] = command[i];
                j++;
            }

            if (stdout == null) {
                if (waitUntilDone) {
                    stdout = File.createTempFile("zorilla", "stdout")
                            .getAbsolutePath();
                } else {
                    stdout = makeAbsolute("job.out");
                }
            }

            if (stderr == null) {
                if (waitUntilDone) {
                    stderr = File.createTempFile("zorilla", "stdout")
                            .getAbsolutePath();
                } else {
                    stderr = makeAbsolute("job.err");
                }
            }

            if (verbose) {
                System.out.println("*** submitting job ***");
                System.out.println("Zorilla node address = "
                        + nodeSocketAddress);
                if (preStage.isEmpty()) {
                    System.out.println("no pre stage files");
                } else {
                    String preStageString = "Pre stage files:";
                    Iterator iterator = preStage.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        if (entry.getKey() == null) {
                            preStageString += "\n " + entry.getValue();
                        } else {
                            preStageString += "\n " + entry.getKey() + " = "
                                    + entry.getValue();
                        }
                    }
                    System.out.println(preStageString);
                }

                if (postStage.isEmpty()) {
                    System.out.println("no post stage files");
                } else {
                    String postStageString = "Post stage files:";
                    Iterator iterator = postStage.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        if (entry.getKey() == null) {
                            postStageString += "\n " + entry.getValue();
                        } else {
                            postStageString += "\n " + entry.getKey() + " = "
                                    + entry.getValue();
                        }
                    }
                    System.out.println(postStageString);
                }

                System.out.println("stdin = " + stdin);
                System.out.println("stdout = " + stdout);
                System.out.println("stderr = " + stderr);
                System.out.println("**********************");
            }

            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                    null, ZoniProtocol.TYPE_CLIENT);

            String jobID = connection.submitJob(executable, arguments,
                    environment, attributes, preStage, postStage, stdin,
                    stdout, stderr);

            System.err.println("submitted job, id = " + jobID);

            if (waitUntilRunning) {
                if (verbose) {
                    System.err.println("waiting until job is running");
                }
                while (true) {
                    JobInfo info = connection.getJobInfo(jobID);

                    if (info.getPhase() >= ZoniProtocol.PHASE_RUNNING) {
                        System.out.println("job now running");
                        break;
                    }

                    Thread.sleep(500);
                }
            }

            if (waitUntilDone) {
                if (verbose) {
                    System.out.println("waiting until job is done");
                }

                FileReader stdoutReader = new FileReader(stdout, System.out);
                FileReader stderrReader = new FileReader(stderr, System.err);

                // register shutdown hook to cancel job..
                try {
                    Runtime.getRuntime().addShutdownHook(
                            new Shutdown(connection, jobID));
                } catch (Exception e) {
                    // IGNORE
                }

                while (true) {
                    JobInfo info = connection.getJobInfo(jobID);

                    if (info.getPhase() >= ZoniProtocol.PHASE_COMPLETED) {
                        if (verbose) {
                            System.out.println("job now done");
                        }
                        break;
                    }

                    // Relaaaax
                    Thread.sleep(1000);
                }

                stdoutReader.end();
                stderrReader.end();

            }

            connection.close();

        } catch (Exception e) {
            System.err.println("exception on running job: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static class Shutdown extends Thread {
        private final ZoniConnection connection;

        private final String jobID;

        Shutdown(ZoniConnection connection, String jobID) {
            this.connection = connection;
            this.jobID = jobID;
        }

        public void run() {
            try {
                if (!connection.isClosed()) {
                    logger.debug("shutdown hook triggered, cancelling job");
                    connection.cancelJob(jobID);
                    connection.close();
                    System.err.println("job " + jobID + " cancelled");
                }
            } catch (Exception e) {
                System.err.println("could not cancel job: " + e);
            }
        }
    }
}
