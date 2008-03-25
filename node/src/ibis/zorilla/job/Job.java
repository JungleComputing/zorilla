package ibis.zorilla.job;

import ibis.ipl.IbisIdentifier;
import ibis.util.TypedProperties;
import ibis.zorilla.Node;
import ibis.zorilla.io.ZorillaPrintStream;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Receiver;
import ibis.zorilla.util.PropertyUndefinedException;
import ibis.zorilla.zoni.ZoniFileInfo;
import ibis.zorilla.zoni.ZorillaJobDescription;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * A job in the zorilla system. May be created either from a description of the
 * job and all the needed files, or from a serialized from of another
 * ZorillaJobDescription object
 */
public abstract class Job {

    public static final long DEFAULT_MAX_MEM = 128 * 1024 * 1024; // 128Mb

    public static final long DEFAULT_WORKER_DISKSPACE = 10 * 1024 * 1024; // 10Mb

    public static final long DEFAULT_MAX_WALLTIME = 15 * 60 * 1000;

    public static final String[] validAttributes = { "directory", "count",
            "host.count", "time.max", "walltime.max", "cputime.max",
            "job.type", "project", "dryrun", "memory.min", "memory.max",
            "savestate", "restart",
            "on.user.exit", "on.user.error",
            "share.node", "ibis", "malleable",
            "classpath", "split.stdout", "split.stderr", "copy.output" };


    // Status of jobs
    private static final Logger logger = Logger.getLogger(Job.class);

    public static final int UNKNOWN = 0;

    public static final int INITIAL = 1;

    public static final int PRE_STAGE = 2;

    public static final int SCHEDULING = 3;

    public static final int RUNNING = 4;

    public static final int CLOSED = 5;

    public static final int POST_STAGE = 6;

    public static final int COMPLETED = 7;

    public static final int CANCELLED = 8;

    public static final int ERROR = 9;

    /**
     * create a constituent of the jobstate
     * 
     * @throws Exception
     * @throws IOException
     */
    public static Job createConstituent(Advert advert, Node node)
            throws Exception, IOException {
        String type = advert.getJobImplementationType();

        if (type.equalsIgnoreCase("primaryCopy")) {
            return new ibis.zorilla.job.Copy(advert, node);
        } else {
            throw new Exception("unknown state type");
        }
    }

    protected static void checkAttributes(TypedProperties attributes)
            throws Exception {

        TypedProperties wrong = attributes.checkProperties(null,
                validAttributes, null, false);

        if (wrong.size() > 0) {
            logger.warn("invalid attributes: " + wrong.toString());
        }

        if (attributes.getIntProperty("count", 1) < 1) {
            throw new Exception("count must be a positive integer");
        }

        if (attributes.getSizeProperty("max.memory", 1) < 0) {
            throw new Exception("max.memory attribute invalid: "
                    + attributes.getProperty("max.memory"));
        }

        String onUserExit = attributes.getProperty("on.user.exit");

        if (onUserExit != null
                && !(onUserExit.equalsIgnoreCase("ignore")
                        || onUserExit.equalsIgnoreCase("cancel.job")
                        || onUserExit.equalsIgnoreCase("close.world") || onUserExit
                        .equalsIgnoreCase("job.error"))) {
            throw new Exception("invalid value for on.user.exit: " + onUserExit);
        }

        String onUserError = attributes.getProperty("on.user.error");

        if (onUserError != null
                && !(onUserError.equalsIgnoreCase("ignore")
                        || onUserError.equalsIgnoreCase("cancel.job")
                        || onUserError.equalsIgnoreCase("close.world") || onUserError
                        .equalsIgnoreCase("job.error"))) {
            throw new Exception("invalid value for on.user.error: "
                    + onUserError);
        }
    }

    /**
     * Add any missing attributes
     */
    protected static void appendAttributes(TypedProperties attributes,
            boolean javaJob) {

        // what to do with a job if a worker exits (with exit-code NON ZERO)
        // ignore, close.world, cancel.job, job.error
        if (!attributes.containsKey("on.user.error")) {
            attributes.put("on.user.error", "job.error");
        }

        // what to do with a job if a worker exits (with exit-code ZERO)
        // ignore, close.world, cancel.job, job.error
        if (!attributes.containsKey("on.user.exit")) {
            attributes.put("on.user.exit", "close.world");
        }

        if (!attributes.containsKey("max.memory")) {
            attributes.put("max.memory", Long.toString(DEFAULT_MAX_MEM));
        }

        if (!attributes.containsKey("walltime.max")) {
            attributes.put("lifetime", Long.toString(DEFAULT_MAX_WALLTIME));
        }

        if (!attributes.containsKey("claim.node")) {
            attributes.put("claim.node", "0");
        }

        if (!attributes.containsKey("worker.processors")) {
            attributes.put("worker.processors", "1");
        }

        if (!attributes.containsKey("worker.diskspace")) {
            attributes.put("worker.diskspace", "0");
        }

        if (!attributes.containsKey("nr.of.workers")) {
            logger.debug("putting in default nr of workers (1)");
            attributes.put("nr.of.workers", "1");
        }

        if (!attributes.containsKey("ibis")) {
            attributes.put("ibis", String.valueOf(javaJob));
        }

        if (!attributes.containsKey("ibis.nameserver")) {
            attributes.put("ibis.nameserver", attributes.get("ibis"));
        }

        if (!attributes.containsKey("malleable")) {
            attributes.put("malleable", Boolean.toString(javaJob));
        }
    }

    public String phaseString() {
        int phase = getPhase();

        if (phase == 0) {
            return "UNKNOWN";
        } else if (phase == 1) {
            return "INITIAL";
        } else if (phase == 2) {
            return "PRE_STAGE";
        } else if (phase == 3) {
            return "SCHEDULING";
        } else if (phase == 4) {
            return "RUNNING";
        } else if (phase == 5) {
            return "CLOSED";
        } else if (phase == 6) {
            return "POST_STAGE";
        } else if (phase == 7) {
            return "COMPLETED";
        } else if (phase == 8) {
            return "CANCELLED";
        } else if (phase == 9) {
            return "ERROR";
        } else {
            return "ILLEGAL_PHASE";
        }
    }

    public abstract UUID getID();

    public abstract boolean isJava();

    public abstract void updateAttributes(Map<String, String> attributes)
            throws Exception;

    public abstract void cancel() throws Exception;

    public abstract void end(long deadline);

    /**
     * this job is dead and can be removed from any administration now. Calling
     * functions of this job might have unexpected results.
     */
    public abstract boolean zombie();

    /**
     * Returns some (implementation specific) information on the status of this
     * job. Not to be used for further processing.
     */
    public abstract Map<String, String> getStats();

    /**
     * Returns the input files in the virtual file system.
     */
    protected abstract InputFile[] getPreStageFiles() throws Exception;

    /**
     * Returns the output files in virtual file system.
     */
    protected abstract String[] getPostStageFiles() throws Exception;

    public abstract ZoniFileInfo getFileInfo(String sandboxPath)
            throws Exception;

    /**
     * Returns a stream suitable to write standard out to. Do not close stream
     * when done writing. May return null.
     */
    protected abstract OutputStream getStdout() throws Exception;

    /**
     * Write standard out to the given output stream. Blocks until this job has
     * finished
     */
    public abstract void readStdout(OutputStream out) throws Exception;

    /**
     * Write standard err to the given output stream. Blocks until this job has
     * finished.
     */
    public abstract void readStderr(OutputStream out) throws Exception;

    /**
     * Write the given file to the given output stream. Blocks until this job
     * has finished
     */
    public abstract void readOutputFile(String sandboxPath, DataOutputStream out)
            throws Exception;

    /**
     * Read data from the given stream and hand it to the stdin of all workers.
     * Blocks until the job is done.
     */
    public abstract void writeStdin(InputStream in) throws Exception;

    /**
     * Returns the standard in file.
     */
    protected abstract InputFile getStdin() throws Exception;

    /**
     * Returns a stream suitable to write standard error to Do not close stream
     * when done writing. May return null.
     */
    protected abstract OutputStream getStderr() throws Exception;

    /**
     * Creates an output stream to write to the given virtual file
     */
    protected abstract OutputStream getOutputFile(String sandboxPath)
            throws Exception, IOException;

    /**
     * Creates an output stream and writes to the given virtual file. This
     * function is here mostly for efficiency reasons.
     */
    protected abstract void writeOutputFile(String virtualFilePath,
            InputStream data) throws Exception, IOException;

    protected abstract ZorillaPrintStream createLogFile(String fileName)
            throws Exception, IOException;

    public abstract ZorillaJobDescription getDescription();

    /**
     * Returns the cluster this node is in in this job.
     */
    protected abstract String cluster() throws Exception;

    /**
     * Hint that this might be a good time to flush any changes to the state.
     */
    protected abstract void flush() throws IOException, Exception;

    /**
     * Returns the current value of the attributes of this job
     */
    public abstract Map<String, String> getAttributes();

    public abstract int getPhase();

    protected abstract boolean getBooleanAttribute(String name)
            throws PropertyUndefinedException;

    protected abstract String getStringAttribute(String name)
            throws PropertyUndefinedException;

    protected abstract int getIntegerAttribute(String name)
            throws PropertyUndefinedException;

    protected abstract long getSizeAttribute(String name)
            throws PropertyUndefinedException;

    public abstract int getExitStatus();

    final static String fileName(File file) throws IOException {
        String[] pathElements = file.getPath().split("/");

        if (pathElements.length == 0) {
            throw new IOException("could not find filename in given file"
                    + " path: " + file.getPath());
        }

        return pathElements[pathElements.length - 1];
    }

    public final String toString() {
        return getID().toString().substring(0, 7);
    }

    abstract void log(String message);

    abstract void log(String message, Exception exception);

    abstract EndPoint newEndPoint(String name, Receiver receiver)
            throws IOException, Exception;

    abstract IbisIdentifier getRandomConstituent();
}
