package ibis.zorilla.job;

import ibis.util.RunProcess;
import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.io.ZorillaPrintStream;
import ibis.zorilla.util.StreamWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

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

    private int exitStatus;

    /**
     * creates a new child for a given ZorillaJobDescription.
     * 
     */
    public Worker(Job job, UUID id, Node node, long deadline) {
        this.job = job;
        this.id = id;
        this.node = node;

        // TODO: implement application-zorilla interaction
        nodePort = 0;

        status = Status.INIT;
        exitStatus = -1;
    }

    public void start() {
        ThreadPool.createNew(this, "worker " + id);
    }

    public UUID id() {
        return id;
    }

    private ProcessBuilder nativeCommand(File workingDir) throws Exception {
        ProcessBuilder result = new ProcessBuilder();

        String location = job.getDescription().getExecutable();

        File executableFile = new File(location);

        logger.debug("executable location = " + location);

        // check if the executable is a file in the sandbox, if so: make
        // sure the file is found by exec() by making its path absolute,
        // and try to set the executable bit
        if (!executableFile.isAbsolute()) {
            File sandboxExeFile = new File(workingDir, location);

            logger
                    .debug("executable location (in sandbox) = "
                            + sandboxExeFile);

            if (sandboxExeFile.exists()) {
                logger.debug("exe file " + sandboxExeFile
                        + " exists in sandbox!");

                // set exe bit (first try)
                new RunProcess("/bin/chmod", "u+x",
                        sandboxExeFile.getAbsolutePath()).run();

                // set exe bit (second try)
                new RunProcess("/usr/bin/chmod", "u+x",
                        sandboxExeFile.getAbsolutePath()).run();

                // override exe location
                location = sandboxExeFile.getAbsolutePath();
            }
        }
        result.command().add(location);

        // add arguments
        String[] arguments = job.getDescription().getArguments();
        for (int i = 0; i < arguments.length; i++) {
            result.command().add(arguments[i]);
        }

        result.environment().putAll(job.getDescription().getEnvironment());

        result.directory(workingDir);

        return result;
    }

    private ProcessBuilder javaCommand(File workingDir) throws Exception {
        ProcessBuilder result = new ProcessBuilder();

        String javaHome = System.getProperty("java.home");
        // String pathSeparator = System.getProperty("path.separator");

        File securityFile = new File(node.config().getConfigDir(),
                "worker.security.policy");

        // java executable
        result.command().add(
                javaHome + File.separator + "bin" + File.separator + "java");

        // security stuff
        // result.add("-Djava.security.debug=access");
        result.command().add("-Djava.security.manager");
        result.command().add(
                "-Djava.security.policy==file:"
                        + securityFile.getAbsolutePath());

        result.command().add(
                "-Xmx" + job.getStringAttribute(JobAttributes.MEMORY_MAX) + "m");

        // node port
        result.command().add("-Dzorilla.node.port=" + nodePort);

        result.command().add("-Dzorilla.cluster=" + job.cluster());

        // Ibis support
        if (job.getBooleanAttribute("ibis")) {

            result.command().add(
                    "-Dibis.pool.size=" + job.getAttributes().getMaxWorkers());

            // result.command().add("-Dibis.pool.cluster=" + job.cluster());
        }

        String appClassPath = job.getDescription().getJavaClassPath();
        if (appClassPath == null) {
            // add root of job to classpath
            appClassPath = "." + File.pathSeparator;
            // appClassPath = "";

            InputFile[] inputs = job.getPreStageFiles();

            for (int i = 0; i < inputs.length; i++) {
                // path of jar is workingDir + path of jar file in virtual fs
                if (inputs[i].sandboxPath().endsWith(".jar")) {

                    appClassPath = appClassPath + workingDir.getAbsolutePath()
                            + File.separator + inputs[i].sandboxPath()
                            + File.pathSeparator;
                }
            }
        } else {
            // replace all ; with : or the other way around :)
            appClassPath = appClassPath.replace(":", File.pathSeparator);
            appClassPath = appClassPath.replace(";", File.pathSeparator);
        }

        // class path
        result.command().add("-classpath");
        result.command().add(appClassPath);

        // user specified environment options
        for (Map.Entry<String, String> entry : job.getDescription()
                .getJavaSystemProperties().entrySet()) {
            if (entry.getKey().startsWith("java")) {
                throw new Exception(
                        "cannot add system properties starting with \"java\"");
            }

            result.command()
                    .add("-D" + entry.getKey() + "=" + entry.getValue());
        }

        // add main class
        result.command().add(job.getDescription().getJavaMain());

        // arguments
        String[] arguments = job.getDescription().getJavaArguments();
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
        logger.debug("worker status now: " + status);
        this.status = status;
        notifyAll();
    }

    private synchronized void setExitStatus(int exitStatus) {
        this.exitStatus = exitStatus;
    }

    public synchronized int exitStatus() {
        return exitStatus;
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
        File scratchDir = new File(node.config().getTmpDir(), id.toString())
                .getAbsoluteFile();

        scratchDir.mkdirs();
        scratchDir.deleteOnExit();

        if (!scratchDir.isDirectory()) {
            throw new IOException("could not create scratch dir");
        }

        InputFile[] preStageFiles = job.getPreStageFiles();
        for (int i = 0; i < preStageFiles.length; i++) {
            logger.debug("copying " + preStageFiles[i] + " to scratch dir");
            preStageFiles[i].copyTo(scratchDir);
        }

        return scratchDir;
    }

    private void postStage(File scratchDir) throws IOException, Exception {
        logger.debug("post staging from scratch dir to job");
        String[] postStageFiles = job.getPostStageFiles();

        for (int i = 0; i < postStageFiles.length; i++) {
            File file = new File(scratchDir, postStageFiles[i]);

            logger.debug("post staging " + postStageFiles[i] + " (" + file
                    + ")");

            if (file.exists() && file.isFile()) {
                // copy file to job object
                FileInputStream in = new FileInputStream(file);
                job.writeOutputFile(postStageFiles[i], in);
                in.close();
            } else {
                logger.debug("post staging: " + file + " does not exist");
            }
        }

    }

    public void run() {
        ProcessBuilder processBuilder;
        Process process = null;
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
            if (!job.isJava()
                    && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
                throw new Exception("cannot run native worker, not allowed");
            }

            log.printlog("starting worker " + id + " on node " + node);

            // creates a dir the user can put all files in. puts copies of
            // all input and jar files in it.
            log.printlog("creating scratch dir");
            setStatus(Status.PRE_STAGE);
            workingDir = createScratchDir(id);

            if (job.getDescription().isJava()) {
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

            log.printlog("working directory for worker = "
                    + processBuilder.directory());
            logger.debug("working directory for worker = "
                    + processBuilder.directory());

            try {
                process = processBuilder.start();
                setStatus(Status.RUNNING);
                logger.debug("made process");
            } catch (IOException e) {
                logger.error("error on forking off worker", e);
                try {
                    if (job.isJava()) {
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

            // TODO reimplement stdin
            // FileReader fileReader = new FileReader(job.getStdin(), process
            // .getOutputStream());

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

                    // fileReader.close();

                    setExitStatus(result);

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
        } finally {
            //make sure the process is destroyed, and the worker officially ends
            if (process != null) {
                logger.warn("had to force-destroy worker");
                process.destroy();
            }
            if (!finished()) {
                logger.warn("had to force status of worker to error");
                setStatus(Status.ERROR);
            }
        }
    }

    public String toString() {
        return id.toString().substring(0, 8);
    }

}
