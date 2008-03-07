package ibis.zorilla.apps;

import ibis.zorilla.zoni.JobDescription;
import ibis.zorilla.zoni.ZoniInputFile;
import ibis.zorilla.zoni.JobInfo;
import ibis.zorilla.zoni.ZoniOutputFile;
import ibis.zorilla.zoni.ZoniConnection;
import ibis.zorilla.zoni.ZoniProtocol;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public final class Zubmit {

    private static final Logger logger = Logger.getLogger(Zubmit.class);

    private static void addInputFile(String sandboxPath, File file,
            JobDescipr) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                // recursive
                addInputFile(sandboxPath + "/" + child.getName(), file, files);
            }
        } else {
            try {
                files.add(new ZoniInputFile(sandboxPath, file));
            } catch (Exception e) {
                System.err.println("cannot add input file to job: " + file
                        + " error = " + e.getMessage());
                System.exit(1);
            }
        }
    }

    private static void parseInputFile(String string,
            JobDescription job) throws Exception {
        logger.debug("string = " + string);

        String[] equalSplit = string.split("=", 2);

        File file = new File(equalSplit[0]);

        String sandboxPath;
        if (equalSplit.length == 2) {
            sandboxPath = equalSplit[1];
        } else {
            sandboxPath = file.getName();
        }

        addInputFile(sandboxPath, file, job);

    }

    private static ZoniOutputFile parseOutputFile(String string)
            throws Exception {
        logger.debug("string = " + string);

        String[] equalSplit = string.split("=", 2);

        String sandboxPath = equalSplit[0];

        File file;
        if (equalSplit.length == 2) {
            file = new File(equalSplit[1]);
        } else {
            file = new File(sandboxPath);
        }

        return new ZoniOutputFile(sandboxPath, file);
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
                .println("usage: zubmit [OPTION].. EXECUTABLE_URI [JOB_ARGUMENTS]"
                        + "\n\nOptions:"
                        + "\n-na,  --node_address IP:PORT        address of node to submit job to"
                        + "\n-a,  --attribute KEY=VALUE          add key value pair to attributes of job"
                        + "\n-e,  --environment KEY=VALUE        add key value pair to environment of job"
                        + "\n-c,  --cores N                      specify initial nr of cores used "
                        + "\n                                    (sets nr.of.workers attribute)"
                        + "\n-w,  --wait_until_running           wait until the job starts before exiting"
                        + "\n-I,  --interactive                  wait until the job is done before exiting"
                        + "\n                                    also prints stdout and stderr to the"
                        + "\n                                    console, streams stdin to the job,"
                        + "\n                                    cancels the job when zubmit"
                        + "\n                                    is interrupted by Ctrl-C, and retrieves"
                        + "\n                                    output when the job finishes"
                        + "\n-v,  --verbose                      print some more info"

                        + "\n\nFiles options:"
                        + "\n-i,  --input [VIRTUAL_PATH=]PATH    add input file"
                        + "\n-o,  --output [VIRTUAL_PATH=]PATH   add output file"
                        + "\n-so, --stdout PATH                  set standard out file"
                        + "\n-si, --stdin PATH                   set standard in file"
                        + "\n-se, --stderr PATH                  set standard error file"
                        + "\n-s,  --split-output                 use a seperate output/error file per worker"

                );
    }

    public static void main(String[] command) {
        boolean waitUntilRunning = false;
        boolean interactive = false;
        boolean verbose = false;
        boolean splitOutput = false;

        try {
            InetSocketAddress nodeSocketAddress = new InetSocketAddress(
                    InetAddress.getByName(null), ZoniProtocol.DEFAULT_PORT);

            JobDescription jobDescription = new JobDescription();
            ArrayList<String> inputFiles = new ArrayList<String>();
            ArrayList<String> outputFiles = new ArrayList<String>();
            File stdin = null;
            File stdout = new File("job.out");
            File stderr = new File("job.err");

            // first option not recognized by this program. Assumed to be
            // executable (URI) of submitted job. URI has java:// scheme
            // in case of java program
            int executableIndex = -1;

            if (command.length == 0) {
                usage();
                return;
            }

            for (int i = 0; i < command.length; i++) {
                if (command[i].equals("-w")
                        || command[i].equals("--wait_until_running")) {
                    waitUntilRunning = true;
                } else if (command[i].equals("-I")
                        || command[i].equals("--interactive")) {
                    interactive = true;
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
                    inputFiles.add(command[i]);
                } else if (command[i].equals("-o")
                        || command[i].equals("--output")) {
                    i++;
                    outputFiles.add(command[i]);
                } else if (command[i].equals("-c")
                        || command[i].equals("--cores")) {
                    i++;
                    jobDescription.setAttribute("nr.of.workers", command[i]);
                } else if (command[i].equals("-s")
                        || command[i].equals("--split-output")) {
                    i++;
                    jobDescription.setAttribute("split.stdout", "true");
                    jobDescription.setAttribute("split.stderr", "true");
                } else if (command[i].equals("-si")
                        || command[i].equals("--stdin")) {
                    i++;
                    stdin = new File(command[i]);
                } else if (command[i].equals("-so")
                        || command[i].equals("--stdout")) {
                    i++;
                    stdout = new File(command[i]);
                } else if (command[i].equals("-se")
                        || command[i].equals("--stderr")) {
                    i++;
                    stderr = new File(command[i]);
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
                    jobDescription.setAttribute(parts[0], parts[1]);
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
                    jobDescription.setEnvironment(parts[0], parts[1]);
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
            
            if (interactive) {
                
                
                
            } else {
                for(String file: inputFiles) {
                    parseInputFile(file, jobDescription);
                }
                
                
                jobDescription.setStdinFile(stdin);
                jobDescription.setStdoutFile(stdout);
                jobDescription.setStderrFile(stderr);
                
                
            }
            

            if (verbose) {
                System.out.println("*** submitting job ***");
                System.out.println(jobDescription);
            }

            ZoniConnection connection = new ZoniConnection(nodeSocketAddress,
                    null, ZoniProtocol.TYPE_CLIENT);

            String jobID;
            jobID = connection.submitJob(jobDescription, null);

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
            } else if (interactive) {
                if (verbose) {
                    System.out.println("waiting until job is done");
                }

                FileReader stdoutReader = new FileReader(stdout.getFile(),
                        System.out);
                FileReader stderrReader = new FileReader(stderr.getFile(),
                        System.err);

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
