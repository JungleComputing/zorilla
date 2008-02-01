package nl.vu.zorilla.primaryCopy;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePortIdentifier;
import ibis.util.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import nl.vu.zorilla.Config;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.Resources;
import nl.vu.zorilla.Worker;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.io.ObjectInput;
import nl.vu.zorilla.io.ObjectOutput;
import nl.vu.zorilla.io.ZorillaPrintStream;
import nl.vu.zorilla.jobNet.Call;
import nl.vu.zorilla.jobNet.EndPoint;
import nl.vu.zorilla.jobNet.Invocation;
import nl.vu.zorilla.jobNet.Receiver;
import nl.vu.zorilla.util.TypedProperties;

import org.apache.log4j.Logger;
import org.gridlab.gat.URI;

public final class Copy extends Job implements Receiver, Runnable {

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

    private String[] arguments;

    private Map<String, String> environment;

    private URI executable;

    // *** FILES *** \\

    private InputFile[] preStageFiles;

    private String[] postStageFiles;

    private CopyOutputStream stdout;

    private CopyOutputStream stderr;

    private InputFile stdin;

    // *** DISTRIBUTED STATE *** \\

    private Map<String, String> status;

    private TypedProperties attributes;

    private int phase;

    private Map<UUID, Constituent> constituents;

    private long deadline;

    // *** LOCAL DATA *** \\

    private final UUID id;

    private final Map<UUID, Worker> localWorkers;

    private ZorillaPrintStream logFile = null;

    private final Ibis ibis;

    private final PortType callPortType;

    private final PortType replyPortType;

    private final EndPoint endPoint;

    private boolean initialized = false;

    private boolean ended = false;

    private final Node node;

    private final String cluster;

    // resources NEEDED per worker
    private Resources workerResources;

    Random random = new Random();

    private long lastStateUpdate = 0;

    // what did we tell the primary last time...
    private int sendMaxNrOfWorkers = 0;

    static final int STATE_UPDATE = 6;

    public Copy(ObjectInput in, Node node) throws IOException, ZorillaException {
        int nameserverPort;
        String nameserverHost;

        this.node = node;

        id = Node.generateUUID();

        cluster = node.config().getCluster();
        
        try {
            jobID = (UUID) in.readObject();
            primary = (ReceivePortIdentifier) in.readObject();
            nameserverPort = in.readInt();
            nameserverHost = in.readString();
        } catch (ClassNotFoundException e) {
            throw new ZorillaException("failed to create copy from bootstrap",
                    e);
        }

        localWorkers = new HashMap<UUID, Worker>();

        ibis = createIbis(nameserverHost, nameserverPort, jobID.toString());

        callPortType = createCallPortType(ibis);
        replyPortType = createReplyPortType(ibis);
        endPoint = new EndPoint(id.toString(), this, ibis, callPortType,
                replyPortType);

        // PUT DEFAULT VALUES FOR VARIABLES IN STATE

        arguments = new String[0];
        environment = new HashMap<String, String>();
        executable = null;
        preStageFiles = new InputFile[0];
        postStageFiles = new String[0];
        stdout = null;
        stderr = null;
        stdin = null;
        workerResources = new Resources();

        status = new HashMap<String, String>();
        status.put("initialized", "false");
        attributes = new TypedProperties();
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
            logger.debug(this.toString() + ": " + message, e);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized void readDynamicState(ObjectInput in)
            throws IOException, ZorillaException {
        try {
            status = (Map<String, String>) in.readObject();
            attributes = (TypedProperties) in.readObject();
            phase = in.readInt();
            constituents = (Map<UUID, Constituent>) in.readObject();

            long now = System.currentTimeMillis();

            end(now + in.readLong());

            lastStateUpdate = now;

        } catch (ClassNotFoundException e) {
            throw new ZorillaException("error while reading state", e);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized void readDynamicState(ReadMessage in)
            throws IOException, ZorillaException {
        try {
            status = (Map<String, String>) in.readObject();
            attributes = (TypedProperties) in.readObject();
            phase = in.readInt();
            constituents = (Map<UUID, Constituent>) in.readObject();

            long now = System.currentTimeMillis();

            end(now + in.readLong());

            lastStateUpdate = now;
        } catch (ClassNotFoundException e) {
            throw new ZorillaException("error while reading state", e);
        }
    }

    @Override
    public UUID getID() {
        return jobID;
    }

    @Override
    public void updateAttributes(Map<String, String> attributes)
            throws ZorillaException {
        throw new ZorillaException("updating of attributes"
                + " only possible at node where job was submitted");
    }

    @Override
    public void cancel() throws ZorillaException {
        throw new ZorillaException("cancelation of job "
                + "only possible at node where job was submitted");
    }

    @Override
    protected synchronized void end(long deadline) {
        if (deadline < this.deadline) {
            this.deadline = deadline;
            for (Worker worker : localWorkers.values()) {
                worker.signal(deadline);
            }

            notifyAll();
        }
    }

    @Override
    protected synchronized boolean zombie() {
        return ended;
    }

    @Override
    protected synchronized Resources usedResources() {
        return workerResources.mult(localWorkers.size());
    }

    @Override
    public synchronized Map<String, String> getStatus() {
        Map<String, String> result = new HashMap<String, String>(status);

        if (initialized) {
            result.put("primary", "false");
            result.put("current.nr.of.workers", "unknown");
            result.put("workers.at.this.node", String.valueOf(localWorkers
                    .size()));
            // TODO: add some more info to this status report
        }

        return result;
    }

    @Override
    public synchronized URI getExecutable() throws ZorillaException {
        if (!initialized) {
            throw new ZorillaException("copy not initialized");
        }
        return executable;
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
    public synchronized String[] getArguments() {
        return arguments.clone();
    }

    @Override
    protected synchronized String cluster() {
        return cluster;
    }

    @Override
    public synchronized Map<String, String> getEnvironment() {
        return new HashMap<String, String>(environment);
    }

    @Override
    protected ZorillaPrintStream createLogFile(String fileName)
            throws ZorillaException, IOException {
        synchronized (this) {
            if (!initialized) {
                throw new ZorillaException("cannot create log file for "
                        + "unitialized copy");
            }
        }
        Call call = endPoint.call(primary);

        call.writeInt(Primary.CREATE_LOG_FILE);
        call.writeObject(id);
        call.writeString(fileName);

        call.call(Config.CALL_TIMEOUT);

        CopyOutputStream result = new CopyOutputStream(call, this);

        readDynamicState(call);
        call.finish();

        return new ZorillaPrintStream(result);
    }

    protected CopyOutputStream createOutputFile(String virtualFilePath)
    throws ZorillaException, IOException {
        synchronized (this) {
            if (!initialized) {
                throw new ZorillaException("cannot create otuput file for "
                        + "unitialized copy");
            }
        }
        Call call = endPoint.call(primary);

        call.writeInt(Primary.GET_OUTPUT_FILE);
        call.writeObject(id);
        call.writeString(virtualFilePath);

        call.call(Config.CALL_TIMEOUT);

        CopyOutputStream result = new CopyOutputStream(call, this);

        readDynamicState(call);
        call.finish();

        return result;
    }

    @Override
    protected void writeOutputFile(String virtualFilePath, InputStream data)
            throws ZorillaException, IOException {
        CopyOutputStream out = createOutputFile(virtualFilePath);

        out.readFrom(data);
        out.close();
    }

    @Override
    protected void flush() throws ZorillaException {
        // NOTHING
    }

    @Override
    public synchronized Map<String, String> getAttributes() {
        HashMap<String, String> result = new HashMap<String, String>();

        for (Map.Entry entry : attributes.entrySet()) {
            result.put((String) entry.getKey(), (String) entry.getValue());
        }
        return result;
    }

    @Override
    public synchronized int getPhase() {
        return phase;
    }

    @Override
    protected synchronized boolean getBooleanAttribute(String name) {
        return attributes.booleanProperty(name);
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
    protected synchronized CopyOutputStream getStdout() throws ZorillaException {
        if (!initialized) {
            throw new ZorillaException("copy not initialized");
        }
        return stdout;
    }

    @Override
    protected synchronized InputFile getStdin() throws ZorillaException {
        if (!initialized) {
            throw new ZorillaException("copy not initialized");
        }
        return stdin;
    }

    @Override
    protected synchronized CopyOutputStream getStderr() throws ZorillaException {
        if (!initialized) {
            throw new ZorillaException("copy not initialized");
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

    private synchronized void handleCreateWorkers(Invocation invocation)
            throws IOException {
        log("got create workers request from primary");

        int nrOfWorkers = invocation.readInt();
        invocation.finishRead();

        for (int i = 0; i < nrOfWorkers; i++) {
            if (!node.config().nativeJobs() && isNative()) {
                log("cannot create native worker");
                break;
            }
            if (node.nrOfResourceSetsAvailable(workerResources) < 1) {
                log("cannot claim resources");
                break;
            }

            UUID workerID = Node.generateUUID();
            Worker worker = new Worker(this, workerID, node, deadline);

            // claim resources, do not start worker yet.
            localWorkers.put(workerID, worker);
        }

        invocation.writeObject(localWorkers.keySet().toArray(new UUID[0]));
        invocation.finish();
    }

    public void invoke(Invocation invocation) throws ZorillaException {

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
                ZorillaException e = new ZorillaException(
                        "unknown incoming invocation to copy: " + opcode);
                log(e.getMessage(), e);
                throw e;
            }
        } catch (IOException e) {
            throw new ZorillaException("Exeption on handling request", e);
        }

    }

    @SuppressWarnings("unchecked")
    private boolean initialize() throws IOException, ZorillaException {
        log("initializing copy: " + this);

        Call call = endPoint.call(primary);

        call.writeInt(Primary.REGISTER);
        call.writeObject(id);
        call.writeObject(endPoint.getID());

        log("doing initialization call");

        call.call(Config.CALL_TIMEOUT);

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
                    throw new ZorillaException("received state for wrong job "
                            + "in initialization of copy");
                }

                arguments = (String[]) call.readObject();

                environment = (Map<String, String>) call.readObject();

                executable = (URI) call.readObject();

                preStageFiles = new InputFile[call.readInt()];
                for (int i = 0; i < preStageFiles.length; i++) {
                    preStageFiles[i] = new InputFile(call, this);
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
                    stdin = new InputFile(call, this);
                }

                workerResources = (Resources) call.readObject();

            } catch (ClassNotFoundException e) {
                throw new ZorillaException("could not read state"
                        + " from call reply", e);
            }

            readDynamicState(call);
            call.finish();

            String scheme = executable.getScheme();
            if ((scheme == null || !scheme.equalsIgnoreCase("java"))
                    && !node.config().nativeJobs()) {
                throw new ZorillaException(
                        "running of non-java job not allowed");
            }

            initialized = true;
            log("copy " + this + " now initialized");

            logFile = createLogFile("copy-" + id.toString() + ".log");
        }
        return true;

    }

    private void sendMaxNrOfWorkers() throws IOException, ZorillaException {
        int nrOfWorkers;
        synchronized (this) {

            nrOfWorkers = node.nrOfResourceSetsAvailable(workerResources)
                    + localWorkers.size();

            if (isNative() && !node.config().nativeJobs()) {
                nrOfWorkers = 0;
            }

            if (nrOfWorkers == sendMaxNrOfWorkers) {
                return;
            }

        }

        Call call = endPoint.call(primary);

        call.writeInt(Primary.UPDATE_MAX_NR_OF_WORKERS);
        call.writeObject(id);
        call.writeInt(nrOfWorkers);

        call.call(Config.CALL_TIMEOUT);

        readDynamicState(call);
        call.finish();

        synchronized (this) {
            sendMaxNrOfWorkers = nrOfWorkers;
        }
    }

    private void updateState() throws IOException, ZorillaException {
        synchronized (this) {
            if ((System.currentTimeMillis() - lastStateUpdate) < Config.JOB_STATE_REFRESH_TIMEOUT) {
                return;
            }
        }
        Call call = endPoint.call(primary);

        call.writeInt(Primary.REQUEST_STATE);
        call.writeObject(id);

        call.call(Config.CALL_TIMEOUT);

        readDynamicState(call);
        call.finish();
    }

    @Override
    public synchronized boolean isNative() {
        return (executable == null || executable.getScheme() == null || !executable
                .getScheme().equals("java"));
    }

    /**
     * Tries to create new workers for this job on this node.
     * 
     * @return true if more workers could be created later, false if not.
     */
    private boolean createNewLocalWorkers() throws IOException,
            ZorillaException {
        if (isNative() && !node.config().nativeJobs()) {
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
                if (node.nrOfResourceSetsAvailable(workerResources) < 1) {
                    log("cannot claim resources");
                    return true; // might get more resources later
                }

                workerID = Node.generateUUID();
                worker = new Worker(this, workerID, node, deadline);

                // claim resources, do not start worker yet.
                localWorkers.put(workerID, worker);
            }

            Call call = endPoint.call(primary);
            call.writeInt(Primary.NEW_WORKER);
            call.writeObject(id);
            call.writeObject(workerID);

            call.call(Config.CALL_TIMEOUT);

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

    private void removeFinishedLocalWorkers() throws IOException,
            ZorillaException {
        Worker[] workers;

        synchronized (this) {
            workers = localWorkers.values().toArray(new Worker[0]);
        }

        for (Worker worker : workers) {
            if (worker.finished()) {
                synchronized (this) {
                    localWorkers.remove(worker.id());
                }

                Call call = endPoint.call(primary);

                call.writeInt(Primary.REMOVE_WORKER);
                call.writeObject(id);
                call.writeObject(worker.id());
                call.writeObject(worker.status());

                call.call(Config.CALL_TIMEOUT);
                // no info in result message

                readDynamicState(call);
                call.finish();
            }
        }

    }

    private synchronized void killWorkers() {
        for (Worker worker : localWorkers.values()) {
            worker.signal(0);
        }
    }

    private void finish() throws IOException, ZorillaException {

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
        call.call(Config.CALL_TIMEOUT);

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
            while (true) {
                removeFinishedLocalWorkers();

                if (moreWorkersPossible) {
                    moreWorkersPossible = createNewLocalWorkers();
                }

                if (!getBooleanAttribute("malleable") && getPhase() <= SCHEDULING) {
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
                        wait(Config.JOB_WAIT_TIMEOUT);
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

    public synchronized IbisIdentifier getRandomConstituent() {
        Constituent[] constituents = this.constituents.values().toArray(
                new Constituent[0]);

        int constituentNr = random.nextInt(constituents.length);

        return constituents[constituentNr].getReceivePort().ibis();
    }

    // create new endpoint "local" to this primary
    public EndPoint newEndPoint(String name, Receiver receiver)
            throws IOException {
        return new EndPoint(name, receiver, ibis, callPortType, replyPortType);
    }

    @Override
    public void writeBootstrap(ObjectOutput out) throws IOException {
         throw new IOException("cannot write bootstrap from copy");
    }
}
