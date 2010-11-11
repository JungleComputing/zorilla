package ibis.zorilla.apps;

import ibis.smartsockets.virtual.VirtualSocketFactory;
import ibis.zorilla.Config;
import ibis.zorilla.api.JavaJobDescription;
import ibis.zorilla.api.NativeJobDescription;
import ibis.zorilla.api.RemoteNode;
import ibis.zorilla.api.VirtualJobDescription;
import ibis.zorilla.api.ZorillaJobDescription;
import ibis.zorilla.util.StreamWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.log4j.Logger;

public class Zubmit {

    private static final Logger logger = Logger.getLogger(Zubmit.class);

//    private static void addInputFile(String sandboxPath, File file,
//            ZorillaJobDescription job) {
//        if (file.isDirectory()) {
//            for (File child : file.listFiles()) {
//                // recursive
//                addInputFile(sandboxPath + "/" + child.getName(), child, job);
//            }
//        } else {
//            try {
//                job.addInputFile(new ZoniInputFile(sandboxPath, file));
//            } catch (Exception e) {
//                System.err.println("cannot add input file to job: " + file
//                        + " error = " + e.getMessage());
//                System.exit(1);
//            }
//        }
//    }
//
//    private static void parseInputFile(String string, ZorillaJobDescription job)
//            throws Exception {
//        logger.debug("string = " + string);
//
//        String[] equalSplit = string.split("=", 2);
//
//        File file = new File(equalSplit[0]);
//
//        String sandboxPath;
//        if (equalSplit.length == 2) {
//            sandboxPath = equalSplit[1];
//        } else {
//            sandboxPath = file.getName();
//        }
//
//        addInputFile(sandboxPath, file, job);
//
//    }
//
//    private static void parseOutputFile(String string, ZorillaJobDescription job)
//            throws Exception {
//        logger.debug("string = " + string);
//
//        String[] equalSplit = string.split("=", 2);
//
//        String sandboxPath = equalSplit[0];
//
//        File file;
//        if (equalSplit.length == 2) {
//            file = new File(equalSplit[1]).getAbsoluteFile();
//        } else {
//            file = new File(sandboxPath).getAbsoluteFile();
//        }
//
//        job.addOutputFile(sandboxPath, file);
//    }
//
//    private static void copyOutputFile(String sandboxPath, File file,
//            ZoniConnection connection, String jobID) throws Exception {
//
//        logger.debug("copying: " + sandboxPath + " to file " + file);
//
//        ZoniFileInfo info = connection.getFileInfo(sandboxPath, jobID);
//
//        logger.debug("info = " + info);
//
//        if (info.isDirectory()) {
//            for (ZoniFileInfo child : info.getChildren()) {
//                copyOutputFile(sandboxPath + "/" + child.getName(), new File(
//                        file, child.getName()), connection, jobID);
//            }
//        } else {
//            file.getParentFile().mkdirs();
//            FileOutputStream fileStream = new FileOutputStream(file);
//
//            connection.getOutputFile(fileStream, sandboxPath, jobID);
//
//            fileStream.close();
//        }
//    }

    private static void usage() {
        System.out
                .println("usage: zubmit TYPE [OPTION].. EXECUTABLE_URI [JOB_ARGUMENTS]"
                        + "\n\nOptions:"
                        + "\n-na,  --node_address IP:PORT        address of node to submit job to"
                        + "\n-a,  --attribute KEY=VALUE          add key value pair to attributes of job"
                        + "\n-e,  --environment KEY=VALUE        add key value pair to environment of job"
                        + "\n-c,  --count N                      specify initial nr of cores used "
                        + "\n-w,  --wait_until_running           wait until the job starts before exiting"
                        + "\n-I,  --interactive                  wait until the job is done before exiting"
                        + "\n                                    also prints stdout and stderr to the"
                        + "\n                                    console, streams stdin to the job,"
                        + "\n                                    cancels the job when zubmit"
                        + "\n                                    is interrupted by Ctrl-C, and retrieves"
                        + "\n                                    output when the job finishes"
                        + "\n-v,  --verbose                      print some more info"
                        + "\n-V,  --virtual                      use a VM when starting the job"

                        + "\n\nFiles options:"
                        + "\n-i,  --input [VIRTUAL_PATH=]PATH    add input file"
                        + "\n-o,  --output PATH[=VIRTUAL_PATH]   add output file"
                        + "\n-so, --stdout PATH                  set standard out file"
                        + "\n-si, --stdin PATH                   set standard in file"
                        + "\n-se, --stderr PATH                  set standard error file"
                        + "\n-s,  --split-output                 use a seperate output/error file per worker"

                );
    }

    public static void main(String[] arguments) {
        int port = RemoteNode.DEFAULT_PORT;
        boolean waitUntilRunning = false;
        boolean interactive = false;
        boolean verbose = false;
        ZorillaJobDescription jobDescription;
        
        JavaJobDescription javaJobDescription = null;
        NativeJobDescription nativeJobDescription = null;
        VirtualJobDescription virtualJobDescription = null;
        
        try {
            if (arguments.length == 0) {
                usage();
                return;
            }
            
            String type = arguments[0];
            
            if (type.equals("java")) {
                javaJobDescription = new JavaJobDescription();
                
                jobDescription = javaJobDescription;
            } else if (type.equals("native")) {
                nativeJobDescription = new NativeJobDescription();
                
                jobDescription = nativeJobDescription;
            } else if (type.equals("virtual")) {
                 virtualJobDescription = new VirtualJobDescription();
                
                jobDescription = virtualJobDescription;
            }

            //working directory already set to cwd.
            
            jobDescription.setStdoutFile(new File("job.out"));
            jobDescription.setStderrFile(new File("job.err"));

            // first option not recognized by this program. Assumed to be
            // executable (URI) of submitted job. URI has java:// scheme
            // in case of java program
            int argumentIndex = -1;

            if (arguments.length == 0) {
                usage();
                return;
            }

            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i].equals("-p") || arguments[i].equals("--port")) {
                    i++;
                    port = Integer.parseInt(arguments[i]);
                } else if (arguments[i].equals("-w")
                        || arguments[i].equals("--wait_until_running")) {
                    waitUntilRunning = true;
                } else if (arguments[i].equals("-I")
                        || arguments[i].equals("--interactive")) {
                    interactive = true;
                } else if (arguments[i].equals("-v")
                        || arguments[i].equals("--verbose")) {
                    verbose = true;
                } else if (arguments[i].equals("-V")
                        || arguments[i].equals("--virtual")) {
                    virtual = true;
                } else if (arguments[i].equals("-i")
                        || arguments[i].equals("--input")) {
                    i++;
                    addInputFile(arguments[i], jobDescription);
                } else if (arguments[i].equals("-o")
                        || arguments[i].equals("--output")) {
                    i++;
                    addOutputFile(arguments[i], jobDescription);
                } else if (arguments[i].equals("-c")
                        || arguments[i].equals("--count")) {
                    i++;
                    jobDescription.setAttribute("count", arguments[i]);
                } else if (arguments[i].equals("-s")
                        || arguments[i].equals("--split-output")) {
                    i++;
                    jobDescription.setAttribute("split.stdout", "true");
                    jobDescription.setAttribute("split.stderr", "true");
                } else if (arguments[i].equals("-si")
                        || arguments[i].equals("--stdin")) {
                    i++;
                    jobDescription.setStdinFile(new File(arguments[i]));
                } else if (arguments[i].equals("-so")
                        || arguments[i].equals("--stdout")) {
                    i++;
                    jobDescription.setStdoutFile(new File(arguments[i]));
                } else if (arguments[i].equals("-se")
                        || arguments[i].equals("--stderr")) {
                    i++;
                    jobDescription.setStderrFile(new File(arguments[i]));
                } else if (arguments[i].equals("-a")
                        || arguments[i].equals("--attribute")) {
                    i++;
                    String[] parts = arguments[i].split("=");
                    if (parts.length != 2) {
                        System.err
                                .println("attribute should be defined as VARIABLE=VALUE (not "
                                        + arguments[i] + ")");
                        System.exit(1);
                    }
                    jobDescription.setAttribute(parts[0], parts[1]);
                } else if (arguments[i].equals("-e")
                        || arguments[i].equals("--environment")) {
                    i++;
                    String[] parts = arguments[i].split("=");
                    if (parts.length != 2) {
                        System.err
                                .println("environment variable should be defined as VARIABLE=VALUE (not "
                                        + arguments[i] + ")");
                        System.exit(1);
                    }
                    jobDescription.setEnvironment(parts[0], parts[1]);
                } else if (arguments[i].startsWith("-D")) {
                    String[] parts = arguments[i].substring(2).split("=");
                    if (parts.length != 2) {
                        System.err
                                .println("java system property should be defined as VARIABLE=VALUE (not "
                                        + arguments[i] + ")");
                        System.exit(1);
                    }
                    jobDescription.setSystemProperty(parts[0], parts[1]);
                } else if (arguments[i].equals("-m") || arguments[i].equals("--main")) {
                    i++;
                    jobDescription.setMain(arguments[i]);
                } else if (arguments[i].equals("--exe")) {
                    i++;
                    jobDescription.setExecutable(new File(arguments[i]));
                } else if (arguments[i].equals("--help")) {
                    usage();
                    return;
                } else {
                    if (arguments[i].startsWith("-")) {
                        System.err
                                .println("unrecognized option: " + arguments[i]);
                        usage();
                        System.exit(1);
                    }
                    argumentIndex = i;
                    break;
                }
            }
            
            String[] jobArguments = new String[(arguments.length - (argumentIndex + 1))];
            int j = 0;
            for (int i = argumentIndex + 1; i < arguments.length; i++) {
                jobArguments[j] = arguments[i];
                j++;
            }

            jobDescription.setArguments(arguments);

            jobDescription.setInteractive(interactive);
            jobDescription.setVirtual(virtual);

            if (verbose) {
                System.out.println("*** submitting job ***");
                System.out.println(jobDescription);
            }

            String jobID;
            jobID = connection.submitJob(jobDescription, null);

            if (verbose || !interactive) {
                System.err.println("submitted job, id = " + jobID);
            }

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

            if (interactive) {
                if (verbose) {
                    System.out.println("** interactive job **");
                }

                ZoniConnection stdinConnection = new ZoniConnection(
                        nodeSocketAddress, factory, null);
                OutputStream stdinStream = stdinConnection.getInput(jobID);
                new StreamWriter(System.in, stdinStream);

                ZoniConnection stdoutConnection = new ZoniConnection(
                        nodeSocketAddress, factory, null);
                InputStream stdoutStream = stdoutConnection.getOutput(jobID,
                        false);
                new StreamWriter(stdoutStream, System.out);

                ZoniConnection stderrConnection = new ZoniConnection(
                        nodeSocketAddress, factory, null);
                InputStream stderrStream = stderrConnection.getOutput(jobID,
                        true);
                new StreamWriter(stderrStream, System.err);

                // register shutdown hook to cancel job..
                try {
                    Runtime.getRuntime().addShutdownHook(
                            new Shutdown(nodeSocketAddress, factory, jobID));
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

                logger.debug("copying "
                        + jobDescription.getOutputFiles().size() + " files");

                // copy output files
                for (Map.Entry<String, File> entry : jobDescription
                        .getOutputFiles().entrySet()) {
                    copyOutputFile(entry.getKey(), entry.getValue(),
                            connection, jobID);
                }
            }

            connection.close();
            factory.end();
        } catch (Exception e) {
            System.err.println("exception on running job: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static String[] parseVirtualArguments(String[] arguments,
            VirtualJobDescription virtualJobDescription) {
        // TODO Auto-generated method stub
        return null;
    }

    private static String[] parseNativeArguments(String[] arguments,
            NativeJobDescription nativeJobDescription) {
        // TODO Auto-generated method stub
        return null;
    }

    private static String[] parseJavaArguments(String[] arguments,
            JavaJobDescription javaJobDescription) {
        // TODO Auto-generated method stub
        return null;
    }

    private static void addOutputFile(String string,
            ZorillaJobDescription jobDescription) {
        // TODO Auto-generated method stub
        
    }

    private static void addInputFile(String string,
            ZorillaJobDescription jobDescription) {
        // TODO Auto-generated method stub
        
    }

    private static class Shutdown extends Thread {
        private final String nodeSocketAddress;
        private final VirtualSocketFactory factory;

        private final String jobID;

        Shutdown(String nodeSocketAddress, VirtualSocketFactory factory,
                String jobID) {
            this.nodeSocketAddress = nodeSocketAddress;
            this.factory = factory;
            this.jobID = jobID;
        }

        public void run() {
            try {
                ZoniConnection connection = new ZoniConnection(
                        nodeSocketAddress, factory, null);
                logger.debug("shutdown hook triggered, cancelling job");

                connection.cancelJob(jobID);
                connection.close();
                System.err.println("job " + jobID + " cancelled");
            } catch (Exception e) {
                System.err.println("could not cancel job: " + e);
            }
        }
    }
}
