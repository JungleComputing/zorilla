package nl.vu.zorilla.job;

import ibis.util.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import nl.vu.zorilla.Config;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.io.InputFile;
import nl.vu.zorilla.io.ZorillaPrintStream;
import nl.vu.zorilla.util.FileReader;
import nl.vu.zorilla.util.StreamWriter;

/**
 * @author Niels Drost
 */
public final class Worker implements Runnable {

    public enum Status {
        INIT, PRE_STAGE, RUNNING, POST_STAGE, DONE, KILLED, USER_ERROR, ERROR;

        public static Status fromOrdinal(int ordinal) throws Exception {
            for (Status status : Status.class.getEnumConstants()) {
                if (status.ordinal() == ordinal) {
                    return status;
                }
            }
            throw new Exception("unknown status: " + ordinal);
        }
    }

    private static final long POLL_INTERVAL = 1000; // 1 second

    private static final Logger logger = Logger.getLogger(Worker.class
            .getName());

    private final UUID id;

    // port on which the node is listening for connections
    private final int nodePort;

    // deadline for this process. set to "infinite" for starters...
    private long deadline = Long.MAX_VALUE;

    private final Node node;

    private final Job job;

    private Status status;

    /**
     * creates a new child for a given Job.
     * 
     */
    public Worker(Job job, UUID id, Node node, long deadline) {
        this.job = job;
        this.id = id;
        this.node = node;

        //TODO: implement application-zorilla interaction
        nodePort = 0;

        status = Status.INIT;
    }

    public void start() {
        ThreadPool.createNew(this, "worker " + id);
    }

    public UUID id() {
        return id;
    }

    private ProcessBuilder nativeCommand(File workingDir)
            throws Exception {

        ProcessBuilder result = new ProcessBuilder();

        String executable = job.getExecutable().getPath();

        if (!executable.startsWith("/")) {
            // executable in "virtual" filesystem
            executable = workingDir.getAbsolutePath() + File.separator
                    + executable;
        }

        // location of executable should be workingDir + relative exe path
        result.command().add(executable);

        // add arguments
        String[] arguments = job.getArguments();
        for (int i = 0; i < arguments.length; i++) {
            result.command().add(arguments[i]);
        }

        result.environment().putAll(job.getEnvironment());

        result.directory(workingDir);

        return result;
    }

    private ProcessBuilder javaCommand(File workingDir) throws Exception {
        String appClassPath;
        ProcessBuilder result = new ProcessBuilder();

        String javaHome = System.getProperty("java.home");
        // String pathSeparator = System.getProperty("path.separator");
        
        File securityFile = new File(node.config().getConfigDir(), "worker.security.policy");
        
        // java executable
        result.command().add(
                javaHome + File.separator + "bin" + File.separator + "java");

        // security stuff
        // result.add("-Djava.security.debug=access");
        result.command().add("-Djava.security.manager");
        result.command().add(
                "-Djava.security.policy==file:" + securityFile.getAbsolutePath());

        result.command().add("-Xmx" + job.getStringAttribute("worker.memory"));

        // node port
        result.command().add("-Dzorilla.node.port=" + nodePort);

        if (!job.getBooleanAttribute("malleable")) {
            result.command().add(
                    "-Dzorilla.nr.of.workers="
                            + job.getIntegerAttribute("nr.of.workers"));
        }

        result.command().add("-Dzorilla.cluster=" + job.cluster());

        // Ibis support
        if (job.getBooleanAttribute("ibis")) {
            
            result.command().add("-Dibis.name=tcp");

            String ibisUtilIPAddress = System
                    .getProperty("ibis.util.ip.address");
            if (ibisUtilIPAddress != null) {
                result.command().add(
                        "-Dibis.util.ip.address=" + ibisUtilIPAddress);
            }

            String ibisUtilIPAltAddress = System
                    .getProperty("ibis.util.ip.alt-address");
            if (ibisUtilIPAltAddress != null) {
                result.command().add(
                        "-Dibis.util.ip.alt-address=" + ibisUtilIPAltAddress);
            }

            String ibisUtilIPInterface = System
                    .getProperty("ibis.util.ip.interface");
            if (ibisUtilIPInterface != null) {
                result.command().add(
                        "-Dibis.util.ip.interface=" + ibisUtilIPInterface);
            }

            String ibisUtilIPAltInterface = System
                    .getProperty("ibis.util.ip.alt-interface");
            if (ibisUtilIPAltInterface != null) {
                result.command().add(
                        "-Dibis.util.ip.alt-interface="
                                + ibisUtilIPAltInterface);
            }

            // ibis malleability support

            if (!job.getBooleanAttribute("malleable")) {
                result.command().add(
                        "-Dibis.pool.total_hosts="
                                + job.getIntegerAttribute("nr.of.workers"));
            }

            result.command().add("-Dibis.pool.cluster=" + job.cluster());
        }

        if (job.getAttributes().containsKey("classpath")) {
            appClassPath = job.getAttributes().get("classpath");
        } else {
            appClassPath = "";

            InputFile[] inputs = job.getPreStageFiles();

            for (int i = 0; i < inputs.length; i++) {
                // path of jar is workingDir + path of jar file in virtual fs
                if (inputs[i].path().endsWith(".jar")) {

                    appClassPath = appClassPath + workingDir.getAbsolutePath()
                            + inputs[i].path() + File.pathSeparator;
                }
            }
        }

        // class path
        result.command().add("-classpath");
        result.command().add(appClassPath);

        // user specified environment options
        for (Map.Entry<String, String> entry : job.getEnvironment().entrySet()) {
            result.command()
                    .add("-D" + entry.getKey() + "=" + entry.getValue());
        }

        // add main class
        result.command().add(job.getExecutable().getSchemeSpecificPart());

        // arguments
        String[] arguments = job.getArguments();
        for (int i = 0; i < arguments.length; i++) {
            result.command().add(arguments[i]);
        }

        result.directory(workingDir);

        return result;

    }

    /**
     * Signal this worker it is time to leave...
     * 
     * @param deadline
     */
    public synchronized void signal(long deadline) {
        logger.debug("worker " + this + " got signal");

        this.deadline = deadline;

        // TODO: also inform the worker of the deadline...

        notifyAll();
    }

    public synchronized Status status() {
        return status;
    }

    public synchronized boolean finished() {
        return status.ordinal() >= Status.DONE.ordinal();
    }

    public synchronized boolean failed() {
        return status.ordinal() >= Status.USER_ERROR.ordinal();
    }

    private synchronized void setStatus(Status status) {
        this.status = status;
        notifyAll();
    }

    public synchronized void waitUntilDone() {
        while (status.equals(Status.INIT) || status.equals(Status.RUNNING)) {
            try {
                wait();
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
    }

    private File createScratchDir(UUID id) throws IOException, Exception {
        File scratchDir = new File(System.getProperty("java.io.tmpdir")
                + File.separator + id.toString());

        scratchDir.mkdirs();

        if (!scratchDir.isDirectory()) {
            throw new IOException("could not create scratch dir");
        }

        InputFile[] preStageFiles = job.getPreStageFiles();
        for (int i = 0; i < preStageFiles.length; i++) {
            preStageFiles[i].copyTo(scratchDir);
        }

        return scratchDir;
    }

    private void postStage(File scratchDir) throws IOException,
            Exception {
        String[] postStageFiles = job.getPostStageFiles();

        for (int i = 0; i < postStageFiles.length; i++) {
            if (!postStageFiles[i].startsWith("/")) {
                throw new Exception(
                        "non-absolute file path found in post staging: "
                                + postStageFiles[i]);
            }
            File file = new File(scratchDir, postStageFiles[i].substring(1));

            if (file.exists() && file.isFile()) {
                // copy file to job object
                FileInputStream in = new FileInputStream(file);
                job.writeOutputFile(postStageFiles[i], in);
                in.close();
            }
        }

    }

    public void run() {
        ProcessBuilder processBuilder;
        java.io.File workingDir;
        ZorillaPrintStream log;
        StreamWriter outWriter;
        StreamWriter errWriter;
        boolean killed = false; // true if we killed the process ourselves

        logger.info("starting worker " + this + " for " + job);

        try {
            log = job.createLogFile(id().toString() + ".log");
        } catch (Exception e) {
            logger.error("could not create log file", e);
            setStatus(Status.ERROR);
            return;
        }

        try {
            // this should not be the case, but just to be safe we check again
            if (job.isNative() && !node.config().booleanProperty(Config.NATIVE_JOBS)) {
                throw new Exception(
                        "cannot run native worker, not allowed");
            }

            log.printlog("starting worker " + id + " on node " + node);

            // creates a dir the user can put all files in. puts copies of
            // all input and jar files in it.
            log.printlog("creating scratch dir");
            setStatus(Status.PRE_STAGE);
            workingDir = createScratchDir(id);

            String jobType = job.getExecutable().getScheme();
            if (jobType != null && jobType.equalsIgnoreCase("java")) {
                processBuilder = javaCommand(workingDir);
            } else {
                processBuilder = nativeCommand(workingDir);
            }

            String cmd = "";
            for (String argument : processBuilder.command()) {
                cmd = cmd + argument + " ";
            }
            logger.debug("running command: " + cmd);
            log.printlog("running command: " + cmd);

            Process process;
            try {
                process = processBuilder.start();
                setStatus(Status.RUNNING);
                logger.debug("made process");
            } catch (IOException e) {
                logger.error("error on forking off worker", e);
                try {
                    if (jobType == null || jobType.equalsIgnoreCase("java")) {
                        // should not happen, must be node error
                        setStatus(Status.ERROR);
                    } else {
                        setStatus(Status.USER_ERROR);
                    }
                    log.printlog(e.toString());
                    job.flush();
                } catch (Exception e2) {
                    // IGNORE
                }
                return;
            }

            outWriter = new StreamWriter(process.getInputStream(), job
                    .getStdout());
            errWriter = new StreamWriter(process.getErrorStream(), job
                    .getStderr());

            FileReader fileReader = new FileReader(job.getStdin(), process
                    .getOutputStream());

            logger.debug("created stream writers, waiting for"
                    + " process to finish");

            // wait for the process to finish or the deadline to pass
            // check every 1 second
            while (status().equals(Status.RUNNING)) {
                try {
                    int result = process.exitValue();
                    log.printlog("process ended with return value " + result);

                    // Wait for output stream read threads to finish
                    outWriter.waitFor();
                    errWriter.waitFor();
                    logger.debug("worker " + this + " done, exit code "
                            + result);

                    fileReader.close();

                    log.printlog("flushing files");
                    setStatus(Status.POST_STAGE);
                    postStage(workingDir);
                    log.close();

                    if (killed) {
                        setStatus(Status.KILLED);
                    } else if (result == 0) {
                        setStatus(Status.DONE);
                    } else {
                        setStatus(Status.USER_ERROR);
                    }
                    logger.info("worker " + this + "(" + status()
                            + ") exited with code " + result);
                } catch (IllegalThreadStateException e) {
                    // process not yet done...
                    synchronized (this) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime >= deadline) {
                            // kill process
                            process.destroy();
                            killed = true;
                        } else {
                            try {
                                long timeout = Math.min(deadline - currentTime,
                                        POLL_INTERVAL);
                                wait(timeout);
                            } catch (InterruptedException e2) {
                                // IGNORE
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("error on running worker", e);
            try {
                setStatus(Status.ERROR);
                log.printlog(e.toString());
                job.flush();
            } catch (Exception e2) {
                // IGNORE
            }
        }
    }

    public String toString() {
        return id.toString().substring(0, 8);
    }

}
