package ibis.zorilla.job;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePortIdentifier;
import ibis.util.ThreadPool;
import ibis.util.TypedProperties;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.io.ObjectInput;
import ibis.zorilla.io.ZorillaPrintStream;
import ibis.zorilla.job.net.Call;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Factory;
import ibis.zorilla.job.net.Invocation;
import ibis.zorilla.job.net.Receiver;
import ibis.zorilla.zoni.ZoniFileInfo;
import ibis.zorilla.zoni.ZorillaJobDescription;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

public final class Copy extends Job implements Receiver, Runnable {

    public static final long JOB_STATE_REFRESH_TIMEOUT = 60 * 1000;

    public static final long JOB_WAIT_TIMEOUT = 1 * 1000;

    // functions

    final static int CREATE_WORKERS = 1;

    final static int START_WORKERS = 2;

    // in case creating workers failed...
    final static int DESTROY_WORKERS = 3;

    private Logger logger = Logger.getLogger(Copy.class);

    // *** BOOTSTRAP DATA *** \\

    private final UUID jobID;

    private final ReceivePortIdentifier primary;

    // *** STATIC INFO ON JOB *** \\

    private ZorillaJobDescription jobDescription;
    
    // *** FILES *** \\

    private InputFile[] preStageFiles;

    private String[] postStageFiles;

    private CopyOutputStream stdout;

    private CopyOutputStream stderr;

    private InputFile stdin;

    // *** DISTRIBUTED STATE *** \\

    private Map<String, String> status;

    private JobAttributes attributes;

    private int phase;

    private Map<UUID, Constituent> constituents;

    private long deadline;

    // *** LOCAL DATA *** \\

    private final UUID id;

    private final Map<UUID, Worker> localWorkers;

    private ZorillaPrintStream logFile = null;

    private final Ibis ibis;

    private final EndPoint endPoint;

    private boolean initialized = false;

    private boolean ended = false;

    private final Node node;

    private final String cluster;

    // resources NEEDED per worker
    private Resources workerResources;

    private long lastStateUpdate = 0;

    // what did we tell the primary last time...
    private int sendMaxNrOfWorkers = 0;

    static final int STATE_UPDATE = 6;

    public Copy(Advert advert, Node node) throws IOException, Exception {
        if (!(advert instanceof PrimaryCopyAdvert)) {
            throw new Exception("wrong advert type");
        }
        PrimaryCopyAdvert ad = (PrimaryCopyAdvert) advert;

        this.node = node;

        id = Node.generateUUID();

        cluster = node.config().getProperty(Config.CLUSTER_NAME);

        jobID = (UUID) ad.getJobID();
        primary = (ReceivePortIdentifier) ad.getPrimaryReceivePort();

        localWorkers = new HashMap<UUID, Worker>();

        ibis = Factory.createIbis(jobID.toString());

        endPoint = new EndPoint(id.toString(), this, ibis);

        // PUT DEFAULT VALUES FOR VARIABLES IN STATE

        jobDescription = null;
        preStageFiles = new InputFile[0];
        postStageFiles = new String[0];
        stdout = null;
        stderr = null;
        stdin = null;
        workerResources = new Resources();

        status = new HashMap<String, String>();
        status.put("initialized", "false");
        attributes = new JobAttributes();
        phase = UNKNOWN;
        constituents = new HashMap<UUID, Constituent>();
        deadline = Integer.MAX_VALUE;

        log("copy created for " + id.toString());

        ThreadPool.createNew(this, "copy of " + id.toString());

    }

    synchronized void log(String message) {
        if (logFile != null) {
            try {
                logFile.printlog(message);
            } catch (IOException e) {
                logger.warn("could not write to log", e);
            }
        }
        logger.debug(this.toString() + ": " + message);
    }

    synchronized void log(String message, Exception e) {
        if (logFile != null) {
            try {
                logFile.printlog(message + ":");
                logFile.print(e);
            } catch (IOException e2) {
                logger.warn("could not write message to log file", e);
            }
        }
        logger.warn(this.toString() + ": " + message, e);
    }

    @SuppressWarnings("unchecked")
    private synchronized void readDynamicState(ObjectInput in)
            throws IOException, Exception {
        try {
            status = (Map<String, String>) in.readObject();
            attributes = (JobAttributes) in.readObject();
            phase = in.readInt();
            constituents = (Map<UUID, Constituent>) in.readObject();

            long now = System.currentTimeMillis();

            end(now + in.readLong());

            lastStateUpdate = now;

        } catch (ClassNotFoundException e) {
            throw new Exception("error while reading state", e);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized void readDynamicState(ReadMessage in)
            throws IOException, Exception {
        try {
            status = (Map<String, String>) in.readObject();
            attributes = (JobAttributes) in.readObject();
            phase = in.readInt();
            constituents = (Map<UUID, Constituent>) in.readObject();

            long now = System.currentTimeMillis();

            end(now + in.readLong());

            lastStateUpdate = now;
        } catch (ClassNotFoundException e) {
            throw new Exception("error while reading state", e);
        }
    }

    @Override
    public UUID getID() {
        return jobID;
    }

    @Override
    public void updateAttributes(Map<String, String> attributes)
            throws Exception {
        throw new Exception("updating of attributes"
                + " only possible at node where job was submitted");
    }

    @Override
    public void cancel() throws Exception {
        end(0);
    }

    @Override
    public synchronized void end(long deadline) {
        if (deadline < this.deadline) {
            this.deadline = deadline;
            for (Worker worker : localWorkers.values()) {
                worker.signal(deadline);
            }

            notifyAll();
        }
    }

    @Override
    public synchronized boolean zombie() {
        return ended;
    }

    @Override
    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>(status);

        if (initialized) {
            result.put("ID", getID().toString());
            result.put("primary", "no");
            result.put("total.workers", "unknown");
            result.put("local.workers", String.valueOf(localWorkers.size()));
            result.put("phase", phaseString());
            if (isJava()) {
                result.put("java", "yes");
            } else {
                result.put("java", "no");
            }
            result.put("executable", jobDescription.getExecutable());
        }

        return result;
    }

    @Override
    protected synchronized InputFile[] getPreStageFiles() {
        return preStageFiles.clone();
    }

    @Override
    protected synchronized String[] getPostStageFiles() {
        return postStageFiles.clone();
    }

    @Override
    protected synchronized String cluster() {
        return cluster;
    }

    @Override
    protected ZorillaPrintStream createLogFile(String fileName)
            throws Exception, IOException {
        synchronized (this) {
            if (!initialized) {
                throw new Exception("cannot create log file for "
                        + "unitialized copy");
            }
        }
        Call call = endPoint.call(primary);

        call.writeInt(Primary.CREATE_LOG_FILE);
        call.writeObject(id);
        call.writeString(fileName);

        call.call();

        CopyOutputStream result = new CopyOutputStream(call, this);

        readDynamicState(call);
        call.finish();

        return new ZorillaPrintStream(result);
    }

    protected CopyOutputStream getOutputFile(String virtualFilePath)
            throws Exception, IOException {
        synchronized (this) {
            if (!initialized) {
                throw new Exception("cannot create otuput file for "
                        + "unitialized copy");
            }
        }
        Call call = endPoint.call(primary);

        call.writeInt(Primary.GET_OUTPUT_FILE);
        call.writeObject(id);
        call.writeString(virtualFilePath);

        call.call();

        CopyOutputStream result = new CopyOutputStream(call, this);

        readDynamicState(call);
        call.finish();

        return result;
    }

    @Override
    protected void writeOutputFile(String virtualFilePath, InputStream data)
            throws Exception, IOException {
        CopyOutputStream out = getOutputFile(virtualFilePath);

        out.readFrom(data);
        out.close();
    }

    @Override
    protected void flush() throws Exception {
        // NOTHING
    }

    @Override
    public synchronized JobAttributes getAttributes() {
        return attributes;
    }

    @Override
    public synchronized int getPhase() {
        return phase;
    }

    @Override
    protected synchronized boolean getBooleanAttribute(String name) {
        return attributes.getBooleanProperty(name);
    }

    @Override
    protected synchronized String getStringAttribute(String name) {
        return attributes.getProperty(name);
    }

    @Override
    protected synchronized int getIntegerAttribute(String name) {
        return attributes.getIntProperty(name);
    }

    @Override
    protected synchronized long getSizeAttribute(String name) {
        return attributes.getSizeProperty(name);
    }

    @Override
    protected synchronized CopyOutputStream getStdout() throws Exception {
        if (!initialized) {
            throw new Exception("copy not initialized");
        }
        return stdout;
    }

    @Override
    protected synchronized InputFile getStdin() throws Exception {
        if (!initialized) {
            throw new Exception("copy not initialized");
        }
        return stdin;
    }

    @Override
    protected synchronized CopyOutputStream getStderr() throws Exception {
        if (!initialized) {
            throw new Exception("copy not initialized");
        }
        return stderr;
    }

    public void receive(ReadMessage message) {
        try {
            int opcode = message.readInt();

            if (opcode != Copy.STATE_UPDATE) {
                log("copy received message with unknown opcode: " + opcode);
            }

            log("state update received in copy");

            readDynamicState(message);
            message.finish();

        } catch (Exception e) {
            log("exception on handling message", e);
        }

    }

    private void handleCreateWorkers(Invocation invocation) throws IOException {
        log("got create workers request from primary");

        int nrOfWorkers = invocation.readInt();
        invocation.finishRead();

        for (int i = 0; i < nrOfWorkers; i++) {
            if (!isJava()
                    && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
                log("cannot create native worker");
                break;
            }
            if (node.jobService().nrOfResourceSetsAvailable(workerResources) < 1) {
                log("cannot claim resources");
                break;
            }

            UUID workerID = Node.generateUUID();
            Worker worker = new Worker(this, workerID, node, deadline);

            synchronized (this) {
                // claim resources, do not start worker yet.
                localWorkers.put(workerID, worker);
                node.jobService().setResourcesUsed(getID(),
                        workerResources.mult(localWorkers.size()));
            }

        }

        synchronized (this) {
            invocation.writeObject(localWorkers.keySet().toArray(new UUID[0]));
        }
        invocation.finish();
    }

    public void invoke(Invocation invocation) throws Exception {

        try {

            int opcode = invocation.readInt();

            if (opcode == CREATE_WORKERS) {
                handleCreateWorkers(invocation);
            } else if (opcode == START_WORKERS) {
                log("got start workers request from primary");
                synchronized (this) {
                    for (Worker worker : localWorkers.values()) {
                        worker.start();
                    }
                }
            } else if (opcode == DESTROY_WORKERS) {
                log("got DESTROY workers request from primary");
                synchronized (this) {
                    for (Worker worker : localWorkers.values()) {
                        worker.signal(0); // tell the worker to go away
                    }

                    localWorkers.clear();
                }

            } else {
                Exception e = new Exception(
                        "unknown incoming invocation to copy: " + opcode);
                log(e.getMessage(), e);
                throw e;
            }
        } catch (IOException e) {
            throw new Exception("Exeption on handling request", e);
        }

    }

    @SuppressWarnings("unchecked")
    private boolean initialize() throws IOException, Exception {
        log("initializing copy: " + this);

        Call call = endPoint.call(primary);

        call.writeInt(Primary.REGISTER);
        call.writeObject(id);
        call.writeObject(endPoint.getID());

        log("doing initialization call");

        call.call();

        log("call done, reading content");

        synchronized (this) {
            try {
                boolean allowed = call.readBoolean();
                if (!allowed) {
                    call.finish();
                    return false;
                }

                UUID jobID = (UUID) call.readObject();

                log("receiving state for " + jobID);

                if (!this.jobID.equals(jobID)) {
                    throw new Exception("received state for wrong job "
                            + "in initialization of copy");
                }

                jobDescription = (ZorillaJobDescription) call.readObject();

                preStageFiles = new InputFile[call.readInt()];
                for (int i = 0; i < preStageFiles.length; i++) {
                    preStageFiles[i] = new InputFile(call, this, node.getTmpDir());
                }

                postStageFiles = new String[call.readInt()];
                for (int i = 0; i < postStageFiles.length; i++) {
                    postStageFiles[i] = call.readString();
                }

                if (call.readBoolean()) {
                    stdout = new CopyOutputStream(call, this);
                }

                if (call.readBoolean()) {
                    stderr = new CopyOutputStream(call, this);
                }

                if (call.readBoolean()) {
                    stdin = new InputFile(call, this, node.getTmpDir());
                }

                workerResources = (Resources) call.readObject();

            } catch (ClassNotFoundException e) {
                throw new Exception(
                        "could not read state" + " from call reply", e);
            }

            readDynamicState(call);
            call.finish();

            if (!jobDescription.isJava()
                    && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
                throw new Exception("running of non-java job not allowed");
            }

            initialized = true;
            log("copy " + this + " now initialized");

            logFile = createLogFile("copy-" + id.toString() + ".log");
        }
        return true;

    }

    private void sendMaxNrOfWorkers() throws IOException, Exception {
        int nrOfWorkers = node.jobService().nrOfResourceSetsAvailable(
                workerResources);

        logger.debug("possible number of NEW workers: " + nrOfWorkers);

        synchronized (this) {

            logger.debug("possible number of NEW2 workers: " + nrOfWorkers);

            nrOfWorkers = nrOfWorkers + localWorkers.size();

            logger.debug("possible number of NEW3 workers: " + nrOfWorkers);

            if (!isJava()
                    && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
                logger.debug("cannot start worker, no native jobs allowed");
                nrOfWorkers = 0;
            }

            logger.debug("total possible number of workers: " + nrOfWorkers);

            if (nrOfWorkers == sendMaxNrOfWorkers) {
                logger
                        .debug("no need to update nr of workers, primary already has the correct number");
                return;
            }

        }

        logger
                .debug("telling primary we can start " + nrOfWorkers
                        + " workers");

        Call call = endPoint.call(primary);

        call.writeInt(Primary.UPDATE_MAX_NR_OF_WORKERS);
        call.writeObject(id);
        call.writeInt(nrOfWorkers);

        call.call();

        readDynamicState(call);
        call.finish();

        synchronized (this) {
            sendMaxNrOfWorkers = nrOfWorkers;
        }
    }

    private void updateState() throws IOException, Exception {
        synchronized (this) {
            if ((System.currentTimeMillis() - lastStateUpdate) < JOB_STATE_REFRESH_TIMEOUT) {
                return;
            }
        }
        Call call = endPoint.call(primary);

        call.writeInt(Primary.REQUEST_STATE);
        call.writeObject(id);

        call.call();

        readDynamicState(call);
        call.finish();
    }

    @Override
    public synchronized boolean isJava() {
        return jobDescription.isJava();
    }

    /**
     * Tries to create new workers for this job on this node.
     * 
     * @return true if more workers could be created later, false if not.
     */
    private boolean createNewLocalWorkers() throws IOException, Exception {
        if (!isJava() && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
            log("not creating worker, native jobs not allowed");
            return false;
        }

        while (true) {
            UUID workerID;
            Worker worker;

            if (getPhase() > RUNNING) {
                log("cannot create new worker, phase > RUNNING");
                return false;
            }

            if (!getBooleanAttribute("malleable")) {
                // job is not malleable, worker creation signaled by primary
                return true;
            }

            synchronized (this) {
                if (node.jobService()
                        .nrOfResourceSetsAvailable(workerResources) < 1) {
                    log("cannot claim resources");
                    return true; // might get more resources later
                }

                workerID = Node.generateUUID();
                worker = new Worker(this, workerID, node, deadline);

                // claim resources, do not start worker yet.
                localWorkers.put(workerID, worker);
                node.jobService().setResourcesUsed(getID(),
                        workerResources.mult(localWorkers.size()));
            }

            Call call = endPoint.call(primary);
            call.writeInt(Primary.NEW_WORKER);
            call.writeObject(id);
            call.writeObject(workerID);

            call.call();

            boolean allowed = call.readBoolean();

            readDynamicState(call);
            call.finish();

            if (!allowed) {
                log("creation of worker denied by primary");
                synchronized (this) {
                    localWorkers.remove(workerID);
                }
                return false;
            }

            worker.start();
        }
    }

    private boolean removeFinishedLocalWorkers() throws IOException, Exception {
        boolean error = false;
        Worker[] workers;

        synchronized (this) {
            workers = localWorkers.values().toArray(new Worker[0]);
        }

        for (Worker worker : workers) {
            if (worker.finished()) {
                if (worker.failed()) {
                    error = true;
                }
                synchronized (this) {
                    localWorkers.remove(worker.id());
                }

                Call call = endPoint.call(primary);

                call.writeInt(Primary.REMOVE_WORKER);
                call.writeObject(id);
                call.writeObject(worker.id());
                call.writeObject(worker.status());
                call.writeInt(worker.exitStatus());

                call.call();
                // no info in result message

                readDynamicState(call);
                call.finish();
            }
        }
        return error;
    }

    private synchronized void killWorkers() {
        for (Worker worker : localWorkers.values()) {
            worker.signal(0);
        }
    }

    private void finish() throws IOException, Exception {

        CopyOutputStream stdout = getStdout();
        if (stdout != null) {
            stdout.close();
        }

        CopyOutputStream stderr = getStderr();
        if (stderr != null) {
            stderr.close();
        }

        logFile.close();

        // unregister
        Call call = endPoint.call(primary);
        call.writeInt(Primary.UNREGISTER);
        call.writeObject(id);
        call.call();

        readDynamicState(call);
        call.finish();

        ibis.end();
    }

    public void run() {
        boolean moreWorkersPossible = true;

        try {
            if (!initialize()) {
                logger.debug("initialization failed because primary denied us");
                synchronized (this) {
                    ended = true;
                }
                return;
            }
        } catch (Exception e) {
            logger.warn("exception on initializing job", e);
            synchronized (this) {
                ended = true;
            }
            return;
        }

        try {
            //dowbload input files
            for(InputFile file: preStageFiles) {
                file.download();
            }

            while (true) {
                if(removeFinishedLocalWorkers()) {
                    //a worker ended in an error, stop creating new workers
                    //for this job
                    moreWorkersPossible = false;
                }
                        

                if (moreWorkersPossible) {
                    moreWorkersPossible = createNewLocalWorkers();
                }

                // update resource usage
                node.jobService().setResourcesUsed(getID(),
                        workerResources.mult(getNrOfWorkers()));

                if (!getBooleanAttribute("malleable")
                        && getPhase() <= SCHEDULING) {
                    // primary needs nr of workers we can start
                    sendMaxNrOfWorkers();
                }

                updateState();

                synchronized (this) {
                    if (System.currentTimeMillis() > deadline) {
                        // signal workers again
                        end(deadline);
                    }

                    if (phase >= COMPLETED) {
                        killWorkers();
                    }

                    if (!moreWorkersPossible && localWorkers.size() == 0) {
                        finish();
                        return;
                    }
                    try {
                        wait(JOB_WAIT_TIMEOUT);
                    } catch (InterruptedException e) {
                        // IGNORE
                    }
                }
            }
        } catch (Exception e) {
            log("exception while participating in job", e);
            try {
                end(0);
                finish();
            } catch (Exception e2) {
                // IGNORE
            }
            synchronized (this) {
                ended = true;
            }
        }
    }

    private synchronized int getNrOfWorkers() {
        return localWorkers.size();
    }

    public synchronized IbisIdentifier getRandomConstituent() {
        Constituent[] constituents = this.constituents.values().toArray(
                new Constituent[0]);

        int constituentNr = Node.randomInt(constituents.length);

        return constituents[constituentNr].getReceivePort().ibisIdentifier();
    }

    // create new endpoint "local" to this primary
    public EndPoint newEndPoint(String name, Receiver receiver)
            throws IOException {
        return new EndPoint(name, receiver, ibis);
    }

    @Override
    public int getExitStatus() {
        return 0;
    }

    @Override
    public void writeStdin(InputStream in) throws Exception {
        throw new Exception("can only write to standard in where job was submitted");
    }

    @Override
    public void readStderr(OutputStream out) throws Exception {
        throw new Exception("can only read from standard err where job was submitted");
    }

    @Override
    public void readStdout(OutputStream out) throws Exception {
        throw new Exception("can only read from standard out where job was submitted");
    }

    @Override
    public ZoniFileInfo getFileInfo(String sandboxPath) throws Exception {
        throw new Exception("can only get output files where job was submitted");
    }

    @Override
    public void readOutputFile(String sandboxPath, DataOutputStream out) throws Exception {
        throw new Exception("can only get output files where job was submitted");
    }
    
    @Override
    public ZorillaJobDescription getDescription() {
        return jobDescription;
    }

}
