package ibis.zorilla.job;

import ibis.ipl.IbisProperties;
import ibis.util.RunProcess;
import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.io.ZorillaPrintStream;
import ibis.zorilla.util.StreamWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.JavaSoftwareDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;

/**
 * @author Niels Drost
 */
public final class Worker implements Runnable {

    public enum Status {
        INIT, PRE_STAGE, RUNNING, POST_STAGE, DONE, KILLED, FAILED, USER_ERROR, ERROR;

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

    private static GATContext createGATContext(Config config) throws Exception {
        GATContext context = new GATContext();
        // SecurityContext securityContext = new
        // CertificateSecurityContext(null,
        // null, System.getProperty("user.name"), null);
        //        
        // context.addSecurityContext(securityContext);

        context.addPreference("sshtrilead.stoppable", "true");

        context.addPreference("file.create", "true");

        String adaptor = config.getProperty(Config.RESOURCE_ADAPTOR);

        context.addPreference("resourcebroker.adaptor.name", adaptor);

        // context.addPreference("file.adaptor.name", "local,sshtrilead");

        return context;

    }

    private final UUID id;

    // port on which the node is listening for connections
    private final int nodePort;

    // predetermined time when this worker will "fail"
    private final long failureDate;

    // deadline for this process. set to "infinite" for starters...
    private long deadline = Long.MAX_VALUE;

    private final Node node;

    private final ZorillaJob zorillaJob;

    private Status status;

    private int exitStatus;

    /**
     * creates a new child for a given ZorillaJobDescription.
     * 
     * @throws Exception
     *             if creating the worker failed
     * 
     */
    public Worker(ZorillaJob job, UUID id, Node node, long deadline) {
        this.zorillaJob = job;
        this.id = id;
        this.node = node;

        // TODO: implement application-zorilla interaction
        nodePort = 0;

        status = Status.INIT;
        exitStatus = -1;

        int mtbf = job.getAttributes().getMTBF();

        if (mtbf == 0) {
            failureDate = Long.MAX_VALUE;
        } else {
            failureDate = System.currentTimeMillis()
                    + (Node.randomTimeout(mtbf) * 1000);
        }

    }

    public void start() {
        ThreadPool.createNew(this, "worker " + id);
    }

    public UUID id() {
        return id;
    }

    private ProcessBuilder nativeCommand(File workingDir) throws Exception {
        ProcessBuilder result = new ProcessBuilder();

        String location = zorillaJob.getDescription().getExecutable();

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
                new RunProcess("/bin/chmod", "u+x", sandboxExeFile
                        .getAbsolutePath()).run();

                // set exe bit (second try)
                new RunProcess("/usr/bin/chmod", "u+x", sandboxExeFile
                        .getAbsolutePath()).run();

                // override exe location
                location = sandboxExeFile.getAbsolutePath();
            }
        }
        result.command().add(location);

        // add arguments
        String[] arguments = zorillaJob.getDescription().getArguments();
        for (int i = 0; i < arguments.length; i++) {
            result.command().add(arguments[i]);
        }

        result.environment().putAll(
                zorillaJob.getDescription().getEnvironment());

        result.directory(workingDir);

        return result;
    }

    /*
     * private ProcessBuilder javaCommand(File workingDir) throws Exception {
     * ProcessBuilder result = new ProcessBuilder();
     * 
     * String javaHome = System.getProperty("java.home"); // String
     * pathSeparator = System.getProperty("path.separator");
     * 
     * File securityFile = new File(node.config().getConfigDir(),
     * "worker.security.policy");
     * 
     * // java executable result.command().add( javaHome + File.separator +
     * "bin" + File.separator + "java");
     * 
     * // security stuff // result.add("-Djava.security.debug=access");
     * result.command().add("-Djava.security.manager"); result.command().add(
     * "-Djava.security.policy==file:" + securityFile.getAbsolutePath());
     * 
     * result .command() .add( "-Xmx" + job
     * .getStringAttribute(JobAttributes.MEMORY_MAX) + "m");
     * 
     * // node port result.command().add("-Dzorilla.node.port=" + nodePort);
     * 
     * result.command().add("-Dzorilla.cluster=" + job.cluster());
     * 
     * // Ibis support if (job.getBooleanAttribute("ibis")) {
     * 
     * result.command() .add( "-Dibis.pool.size=" +
     * job.getAttributes().getProcessCount());
     * 
     * // result.command().add("-Dibis.pool.cluster=" + job.cluster()); }
     * 
     * String appClassPath = job.getDescription().getJavaClassPath(); if
     * (appClassPath == null) { // add root of job to classpath appClassPath =
     * "." + File.pathSeparator; // appClassPath = "";
     * 
     * InputFile[] inputs = job.getPreStageFiles();
     * 
     * for (int i = 0; i < inputs.length; i++) { // path of jar is workingDir +
     * path of jar file in virtual fs if
     * (inputs[i].sandboxPath().endsWith(".jar")) {
     * 
     * appClassPath = appClassPath + workingDir.getAbsolutePath() +
     * File.separator + inputs[i].sandboxPath() + File.pathSeparator; } } } else
     * { // replace all ; with : or the other way around :) appClassPath =
     * appClassPath.replace(":", File.pathSeparator); appClassPath =
     * appClassPath.replace(";", File.pathSeparator); }
     * 
     * // class path result.command().add("-classpath");
     * result.command().add(appClassPath);
     * 
     * // user specified environment options for (Map.Entry<String, String>
     * entry : job.getDescription() .getJavaSystemProperties().entrySet()) { if
     * (entry.getKey().startsWith("java")) { throw new Exception(
     * "cannot add system properties starting with \"java\""); }
     * 
     * result.command() .add("-D" + entry.getKey() + "=" + entry.getValue()); }
     * 
     * // add main class
     * result.command().add(job.getDescription().getJavaMain());
     * 
     * // arguments String[] arguments =
     * job.getDescription().getJavaArguments(); for (int i = 0; i <
     * arguments.length; i++) { result.command().add(arguments[i]); }
     * 
     * result.directory(workingDir);
     * 
     * return result;
     * 
     * }
     */

    private JavaSoftwareDescription createJavaSoftwareDescription(
            File workingDir, GATContext context, boolean streaming)
            throws Exception {
        JavaSoftwareDescription sd = new JavaSoftwareDescription();

        // FIXME: assumes java is on same location as localhost
        sd.setExecutable(System.getProperty("java.home")
                + java.io.File.separator + "bin" + java.io.File.separator
                + "java");

        // security stuff
        // result.add("-Djava.security.debug=access");
        sd.addJavaSystemProperty("java.security.manager", "default");

        File securityFile = new File(node.config().getConfigDir(),
                "worker.security.policy");

        // double "=" in result overrides all other security files
        sd.addJavaSystemProperty("java.security.policy", "=file:"
                + securityFile.getAbsolutePath());

        sd
                .setJavaOptions("-Xmx"
                        + zorillaJob
                                .getStringAttribute(JobAttributes.MEMORY_MAX)
                        + "m");

        // node port
        sd.addJavaSystemProperty("zorilla.node.port", "" + nodePort);

        sd.addJavaSystemProperty("zorilla.cluster", zorillaJob.cluster());

        // class path

        String appClassPath = zorillaJob.getDescription().getJavaClassPath();

        if (appClassPath == null) {
            // add root of job to classpath
            // appClassPath = "." + File.pathSeparator;
            appClassPath = "";

            InputFile[] inputs = zorillaJob.getPreStageFiles();

            for (int i = 0; i < inputs.length; i++) {
                // path of jar is workingDir + path of jar file in virtual fs
                if (inputs[i].sandboxPath().endsWith(".jar")) {

                    appClassPath = appClassPath + inputs[i].sandboxPath()
                            + File.pathSeparator;
                }
            }
        } else {
            // replace all ; with : or the other way around :)
            appClassPath = appClassPath.replace(":", File.pathSeparator);
            appClassPath = appClassPath.replace(";", File.pathSeparator);
        }

        sd.setJavaClassPath(appClassPath);

        // user specified environment options
        for (Map.Entry<String, String> entry : zorillaJob.getDescription()
                .getJavaSystemProperties().entrySet()) {
            if (entry.getKey().startsWith("java")) {
                throw new Exception(
                        "cannot add system properties starting with \"java\"");
            }

            sd.addJavaSystemProperty(entry.getKey(), entry.getValue());
        }

        // set hub address, prefix with our hub address
        String hubAddresses = zorillaJob.getDescription()
                .getJavaSystemProperties().get(IbisProperties.HUB_ADDRESSES);
        if (hubAddresses == null) {
            hubAddresses = node.network().getAddress().hub().toString();
        } else {
            hubAddresses = node.network().getAddress().hub().toString() + ","
                    + hubAddresses;
        }
        sd.addJavaSystemProperty(IbisProperties.HUB_ADDRESSES, hubAddresses);

        sd.addJavaSystemProperty(IbisProperties.POOL_SIZE, ""
                + zorillaJob.getAttributes().getProcessCount());

        sd.addJavaSystemProperty(IbisProperties.LOCATION_POSTFIX, System
                .getProperty(IbisProperties.LOCATION));

        // main class and options
        sd.setJavaMain(zorillaJob.getDescription().getJavaMain());

        sd.setJavaArguments(zorillaJob.getDescription().getJavaArguments());

        // FIXME:DEBUG
        sd.addAttribute("sandbox.delete", "false");

        for (File file : workingDir.getAbsoluteFile().listFiles()) {
            sd.addPreStagedFile(GAT.createFile(context, "file:"
                    + file.getAbsolutePath()));
        }

        if (streaming) {
            sd.enableStreamingStderr(true);
            sd.enableStreamingStdout(true);
            sd.enableStreamingStdin(true);
        } else {
            sd.setStderr(GAT.createFile(id.toString() + ".err"));
            sd.setStdout(GAT.createFile(id.toString() + ".out"));
        }

        return sd;
    }

    /**
     * Create a job description
     * 
     * @param sd
     *            java software description
     * @param prefix
     *            prefix java job with these arguments. Starts with executable
     * @return software description
     * @throws Exception
     */
    private org.gridlab.gat.resources.JobDescription createJobDescription(
            JavaSoftwareDescription sd, GATContext context, boolean streaming)
            throws Exception {
        org.gridlab.gat.resources.JobDescription result;

        String wrapper = node.config().getProperty(Config.RESOURCE_WRAPPER);

        if (wrapper == null) {
            result = new org.gridlab.gat.resources.JobDescription(sd);
            result.setProcessCount(1);
            result.setResourceCount(1);
        } else {
            // copy all settings from the java description to a "normal"
            // software description
            SoftwareDescription wrapperSd = new SoftwareDescription();
            if (sd.getAttributes() != null) {
                wrapperSd.setAttributes(sd.getAttributes());
            }
            if (sd.getEnvironment() != null) {
                wrapperSd.setEnvironment(sd.getEnvironment());
            }
            if (sd.getPreStaged() != null) {
                for (org.gridlab.gat.io.File src : sd.getPreStaged().keySet()) {
                    wrapperSd.addPreStagedFile(src, sd.getPreStaged().get(src));
                }
            }
            if (sd.getPostStaged() != null) {
                for (org.gridlab.gat.io.File src : sd.getPostStaged().keySet()) {
                    wrapperSd.addPostStagedFile(src, sd.getPostStaged()
                            .get(src));
                }
            }

            // set first prefix element as executable
            wrapperSd.setExecutable("/bin/sh");

            // add wrapper to pre-stage files
            wrapperSd.addPreStagedFile(GAT.createFile(context, wrapper), GAT
                    .createFile(context, "."));

            // prepend arguments with script, java exec, resource and process
            // count
            List<String> argumentList = new ArrayList<String>();

            argumentList.add(wrapper);
            argumentList.add("1");
            argumentList.add("1");
            argumentList.add(sd.getExecutable());
            if (sd.getArguments() != null) {
                for (String arg : sd.getArguments()) {
                    argumentList.add(arg);
                }
            }
            wrapperSd.setArguments(argumentList.toArray(new String[argumentList
                    .size()]));

            if (streaming) {
                wrapperSd.enableStreamingStderr(true);
                wrapperSd.enableStreamingStdin(true);
                wrapperSd.enableStreamingStdout(true);
            } else {
                wrapperSd.setStderr(GAT.createFile(id.toString() + ".err"));
                wrapperSd.setStdout(GAT.createFile(id.toString() + ".out"));
            }

            result = new org.gridlab.gat.resources.JobDescription(wrapperSd);
            result.setProcessCount(1);
            result.setResourceCount(1);
        }

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
        logger.info("worker status now: " + status);
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

        InputFile[] preStageFiles = zorillaJob.getPreStageFiles();
        for (int i = 0; i < preStageFiles.length; i++) {
            logger.debug("copying " + preStageFiles[i] + " to scratch dir");
            preStageFiles[i].copyTo(scratchDir);
        }

        return scratchDir;
    }

    private void postStage(File scratchDir) throws IOException, Exception {
        logger.debug("post staging from scratch dir to job");
        String[] postStageFiles = zorillaJob.getPostStageFiles();

        for (int i = 0; i < postStageFiles.length; i++) {
            File file = new File(scratchDir, postStageFiles[i]);

            logger.debug("post staging " + postStageFiles[i] + " (" + file
                    + ")");

            if (file.exists() && file.isFile()) {
                // copy file to job object
                FileInputStream in = new FileInputStream(file);
                zorillaJob.writeOutputFile(postStageFiles[i], in);
                in.close();
            } else {
                logger.debug("post staging: " + file + " does not exist");
            }
        }
    }

    public void run() {
        java.io.File workingDir;
        ZorillaPrintStream log;
        StreamWriter outWriter;
        StreamWriter errWriter;
        boolean killed = false; // true if we killed the process ourselves
        boolean failed = false; // true if this worker "failed" on purpose
        Job gatJob = null;

        logger.info("starting worker " + this + " for " + zorillaJob);

        try {
            log = zorillaJob.createLogFile(id().toString() + ".log");
        } catch (Exception e) {
            logger.error("could not create log file", e);
            setStatus(Status.ERROR);
            return;
        }

        try {

            // this should not be the case, but just to be safe we check again
            if (!zorillaJob.isJava()
                    && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
                throw new Exception("cannot run native worker, not allowed");
            }

            log.printlog("starting worker " + id + " on node " + node);

            // creates a dir the user can put all files in. puts copies of
            // all input and jar files in it.
            log.printlog("creating scratch dir");
            setStatus(Status.PRE_STAGE);
            workingDir = createScratchDir(id);

            GATContext context = createGATContext(node.config());

            boolean streaming = !node.config().getProperty(
                    Config.RESOURCE_ADAPTOR).equals("sge");

            JobDescription jobDescription;
            if (zorillaJob.getDescription().isJava()) {
                jobDescription = createJobDescription(
                        createJavaSoftwareDescription(workingDir, context,
                                streaming), context, streaming);
            } else {
                logger.error("native jobs not supported for now");
                setStatus(Status.ERROR);
                return;
            }

            logger.debug("running job: " + jobDescription);
            log.printlog("running job: " + jobDescription);

            URI resourceURI = new URI(node.config().getProperty(
                    Config.RESOURCE_URI));

            if (resourceURI.getScheme().equalsIgnoreCase("multissh")) {
                String host = node.jobService().getHost(this);

                if (host == null) {
                    logger.error("Could not allocate host in multissh scheme");
                    setStatus(Status.ERROR);
                    return;
                }

                resourceURI = new URI("ssh://" + host);
            }

            ResourceBroker jobBroker = GAT.createResourceBroker(context,
                    resourceURI);

            // GAT job
            gatJob = jobBroker.submitJob(jobDescription);

            setStatus(Status.RUNNING);
            logger.debug("made process");

            if (streaming) {
                outWriter = new StreamWriter(gatJob.getStdout(), zorillaJob
                        .getStdout());
                errWriter = new StreamWriter(gatJob.getStderr(), zorillaJob
                        .getStderr());
            } else {
                outWriter = null;
                errWriter = null;

            }

            // TODO reimplement stdin
            // FileReader fileReader = new FileReader(job.getStdin(), process
            // .getOutputStream());

            logger.debug("created stream writers, waiting for"
                    + " process/job to finish");

            // wait for the process to finish or the deadline to pass
            // check every 1 second
            while (status().equals(Status.RUNNING)) {

                JobState gatState = gatJob.getState();

                logger.trace("worker Gat job status now " + gatState);

                if (gatState.equals(JobState.STOPPED)
                        || gatState.equals(JobState.SUBMISSION_ERROR)) {
                    int exitStatus = gatJob.getExitStatus();
                    if (outWriter == null) {
                        outWriter = new StreamWriter(GAT
                                .createFileInputStream(id.toString() + ".out"),
                                zorillaJob.getStdout());
                    }
                    if (errWriter == null) {
                        errWriter = new StreamWriter(GAT
                                .createFileInputStream(id.toString() + ".err"),
                                zorillaJob.getStderr());
                    }
                    outWriter.waitFor();
                    errWriter.waitFor();
                    logger.debug("worker " + this + " done, exit code "
                            + exitStatus);
                    setExitStatus(exitStatus);
                    log.printlog("flushing files");
                    setStatus(Status.POST_STAGE);
                    postStage(workingDir);
                    log.close();

                    if (killed) {
                        setStatus(Status.KILLED);
                    } else if (failed || gatState == JobState.SUBMISSION_ERROR) {
                        setStatus(Status.FAILED);
                    } else if (exitStatus == 0) {
                        setStatus(Status.DONE);
                    } else {
                        setStatus(Status.USER_ERROR);
                    }
                    logger.info("worker " + this + "(" + status()
                            + ") exited with code " + exitStatus);
                } else {
                    // process not yet done...
                    boolean kill = false;
                    synchronized (this) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime >= deadline) {
                            logger.info("killing worker process");
                            kill = true;
                        } else if (currentTime >= failureDate) {
                            logger.info("making worker process fail");
                            kill = true;
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
                    if (kill) {
                        gatJob.stop();
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("error on running worker", e);
            try {
                setStatus(Status.ERROR);
                log.printlog(e.toString());
                zorillaJob.flush();
            } catch (Exception e2) {
                // IGNORE
            }
        } finally {
            // make sure the process is destroyed, and the worker officially
            // ends
            if (gatJob != null) {
                // logger.warn("had to force-destroy worker");
                try {
                    gatJob.stop();
                } catch (GATInvocationException e) {
                    // IGNORE
                }
            }
            if (!finished()) {
                logger.warn("had to force status of worker to error");
                setStatus(Status.ERROR);
            }
        }
    }

    /*
     * public void run() { ProcessBuilder processBuilder; Process process =
     * null; java.io.File workingDir; ZorillaPrintStream log; StreamWriter
     * outWriter; StreamWriter errWriter; boolean killed = false; // true if we
     * killed the process ourselves int killedCount = 0; boolean failed = false;
     * // true if this worker "failed" on purpose
     * 
     * logger.info("starting worker " + this + " for " + job);
     * 
     * try { log = job.createLogFile(id().toString() + ".log"); } catch
     * (Exception e) { logger.error("could not create log file", e);
     * setStatus(Status.ERROR); return; }
     * 
     * try { // this should not be the case, but just to be safe we check again
     * if (!job.isJava() && !node.config().getBooleanProperty(
     * ZorillaProperties.NATIVE_JOBS)) { throw new
     * Exception("cannot run native worker, not allowed"); }
     * 
     * log.printlog("starting worker " + id + " on node " + node);
     * 
     * // creates a dir the user can put all files in. puts copies of // all
     * input and jar files in it. log.printlog("creating scratch dir");
     * setStatus(Status.PRE_STAGE); workingDir = createScratchDir(id);
     * 
     * if (job.getDescription().isJava()) { processBuilder =
     * javaCommand(workingDir); } else { processBuilder =
     * nativeCommand(workingDir); }
     * 
     * String cmd = ""; for (String argument : processBuilder.command()) { cmd =
     * cmd + argument + " "; } logger.debug("running command: " + cmd);
     * log.printlog("running command: " + cmd);
     * 
     * log.printlog("working directory for worker = " +
     * processBuilder.directory());
     * logger.debug("working directory for worker = " +
     * processBuilder.directory());
     * 
     * try { process = processBuilder.start(); setStatus(Status.RUNNING);
     * logger.debug("made process"); } catch (IOException e) {
     * logger.error("error on forking off worker", e); try { if (job.isJava()) {
     * // should not happen, must be node error setStatus(Status.ERROR); } else
     * { setStatus(Status.USER_ERROR); } log.printlog(e.toString());
     * job.flush(); } catch (Exception e2) { // IGNORE } return; }
     * 
     * outWriter = new StreamWriter(process.getInputStream(), job .getStdout());
     * errWriter = new StreamWriter(process.getErrorStream(), job .getStderr());
     * 
     * // TODO reimplement stdin // FileReader fileReader = new
     * FileReader(job.getStdin(), process // .getOutputStream());
     * 
     * logger.debug("created stream writers, waiting for" +
     * " process to finish");
     * 
     * // wait for the process to finish or the deadline to pass // check every
     * 1 second while (status().equals(Status.RUNNING)) { try { int result =
     * process.exitValue(); log.printlog("process ended with return value " +
     * result);
     * 
     * // Wait for output stream read threads to finish outWriter.waitFor();
     * errWriter.waitFor(); logger.debug("worker " + this + " done, exit code "
     * + result);
     * 
     * // fileReader.close();
     * 
     * setExitStatus(result);
     * 
     * log.printlog("flushing files"); setStatus(Status.POST_STAGE);
     * postStage(workingDir); log.close();
     * 
     * if (killed) { setStatus(Status.KILLED); } else if (failed) {
     * setStatus(Status.FAILED); } else if (result == 0) {
     * setStatus(Status.DONE); } else { setStatus(Status.USER_ERROR); }
     * logger.info("worker " + this + "(" + status() + ") exited with code " +
     * result); } catch (IllegalThreadStateException e) { // process not yet
     * done... synchronized (this) { long currentTime =
     * System.currentTimeMillis(); if (currentTime >= deadline) { // kill
     * process if (killedCount < 10) { process.destroy(); killed = true;
     * killedCount++; } else if (killedCount == 10) {
     * logger.error("Process for worker " + this +
     * " doesn't seem to want to die. " + "Please kill process manually");
     * killedCount++; } } else if (currentTime >= failureDate) { // kill process
     * if (killedCount < 10) { process.destroy(); failed = true; killedCount++;
     * } else if (killedCount == 10) { logger.error("Process for worker " + this
     * + " doesn't seem to want to die. " + "Please kill process manually"); } }
     * else { try { long timeout = Math.min(deadline - currentTime,
     * POLL_INTERVAL); wait(timeout); } catch (InterruptedException e2) { //
     * IGNORE } } } } } } catch (Exception e) {
     * logger.warn("error on running worker", e); try { setStatus(Status.ERROR);
     * log.printlog(e.toString()); job.flush(); } catch (Exception e2) { //
     * IGNORE } } finally { // make sure the process is destroyed, and the
     * worker officially // ends if (process != null) { //
     * logger.warn("had to force-destroy worker"); process.destroy(); } if
     * (!finished()) { logger.warn("had to force status of worker to error");
     * setStatus(Status.ERROR); } } }
     */

    public String toString() {
        return id.toString().substring(0, 8);
    }

}
