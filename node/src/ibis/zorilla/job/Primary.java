package ibis.zorilla.job;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePortIdentifier;
import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.io.ObjectOutput;
import ibis.zorilla.io.ZorillaPrintStream;
import ibis.zorilla.job.Worker.Status;
import ibis.zorilla.job.net.Call;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Factory;
import ibis.zorilla.job.net.Invocation;
import ibis.zorilla.job.net.Receiver;
import ibis.zorilla.job.net.WriteMessage;
import ibis.zorilla.zoni.ZorillaJobDescription;
import ibis.zorilla.zoni.ZoniInputFile;
import ibis.zorilla.zoni.ZoniFileInfo;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public final class Primary extends Job implements Runnable, Receiver {

    public static final long MIN_ADVERT_TIMEOUT = 5 * 1000;

    public static final long MAX_ADVERT_TIMEOUT = 60 * 1000;

    public static final int MAX_ADVERT_RADIUS = 15;

    public static final long WAIT_TIMEOUT = 100;

    static final int REGISTER = 0;

    static final int UPDATE_MAX_NR_OF_WORKERS = 1;

    static final int REQUEST_STATE = 2;

    static final int NEW_WORKER = 3;

    static final int CREATE_LOG_FILE = 4;

    static final int GET_OUTPUT_FILE = 5;

    static final int REMOVE_WORKER = 6;

    static final int UNREGISTER = 7;

    private static final Logger logger = Logger.getLogger(Primary.class);

    // *** STATIC INFO ON JOB *** \\

    private final UUID id;

    private final ZorillaJobDescription jobDescription;

    // *** FILES *** \\

    private final InputFile[] preStageFiles;

    private final PrimaryOutputStream[] postStageFiles;

    private final ArrayList<PrimaryOutputStream> logFiles;

    private final PrimaryOutputStream stdout;

    private final PrimaryOutputStream stderr;

    private final InputFile stdin;

    // *** DISTRIBUTED STATE *** \\

    private JobAttributes attributes;

    private int phase;

    private int exitStatus;

    private final Map<UUID, Constituent> constituents;

    private long deadline;

    // *** CENTRAL STATE ***\\

    private final ZorillaPrintStream log;

    private long lastAdvertisement = 0;

    private long advertTimeout;

    private int advertCount;

    private long submissiontime;

    private long starttime = 0;

    private long stoptime = 0;

    // *** LOCAL DATA ***\\

    private final File logDir;

    private final Node node;

    private final Map<UUID, Worker> localWorkers;

    private final Callback callback;

    // resources NEEDED per worker
    private final Resources workerResources;

    private final String cluster;

    /*
     * If true the distributed state has been changed, and needs to be
     * propegated to the copies.
     */
    private boolean dirty = false;

    // the ibis used to communicate with all the copies.
    private final Ibis ibis;

    private final EndPoint endPoint;

    Random random = new Random();

    public Primary(ZorillaJobDescription description, Callback callback,
            Node node) throws Exception, IOException {

        this.node = node;

        cluster = node.config().getProperty(Config.CLUSTER_NAME);

        id = Node.generateUUID();
        this.jobDescription = description;

        this.callback = callback;

        logDir = new File(node.getNodeDir(), "job-" + id.toString());
        logDir.mkdirs();

        logFiles = new ArrayList<PrimaryOutputStream>();

        log = createLogFile("job.log");

        exitStatus = 0;

        try {

            attributes = new JobAttributes(description.getAttributes());

            submissiontime = System.currentTimeMillis();
            long wallTimeMillis = attributes.getLongProperty(JobAttributes.WALLTIME_MAX) * 60 * 1000;
            deadline = submissiontime + wallTimeMillis;

            // create ibis

            ibis = Factory.createIbis(id.toString());

            endPoint = newEndPoint(id.toString(), this);

            // initialize files

            ZoniInputFile[] zoniInputFiles = description.getInputFiles();

            preStageFiles = new InputFile[zoniInputFiles.length];

            for (int i = 0; i < preStageFiles.length; i++) {
                ZoniInputFile file = zoniInputFiles[i];
                preStageFiles[i] = new InputFile(file.getFile(), file
                        .getSandboxPath(), this);
            }

            ArrayList<PrimaryOutputStream> postStageFiles = new ArrayList<PrimaryOutputStream>();

            if (description.isInteractive()) {
                stdout = new PrimaryOutputStream("##stdout##", File
                        .createTempFile(id.toString(), ".stdout", node
                                .getTmpDir()), this);

                stderr = new PrimaryOutputStream("##stderr##", File
                        .createTempFile(id.toString(), ".stderr", node
                                .getTmpDir()), this);

                // TODO: support standard in too :)
                stdin = null;

                for (Map.Entry<String, File> entry : description
                        .getOutputFiles().entrySet()) {
                    postStageFiles.add(new PrimaryOutputStream(entry.getKey(),
                            File.createTempFile(id.toString(), ".output", node
                                    .getTmpDir()), this));
                }
            } else {
                stdout = new PrimaryOutputStream("##stdout##", description
                        .getStdoutFile(), this);

                stderr = new PrimaryOutputStream("##stderr##", description
                        .getStderrFile(), this);

                if (description.getStdinFile() == null) {
                    stdin = null;
                } else {
                    stdin = new InputFile(description.getStdinFile(),
                            "<<stdin>>", this);
                }

                for (Map.Entry<String, File> entry : description
                        .getOutputFiles().entrySet()) {
                    postStageFiles.add(new PrimaryOutputStream(entry.getKey(),
                            entry.getValue(), this));
                }
            }

            this.postStageFiles = postStageFiles
                    .toArray(new PrimaryOutputStream[0]);

            advertCount = (int) Math.round(Math.log10(maxNrOfWorkers()));
            advertTimeout = MIN_ADVERT_TIMEOUT;

            if (advertCount < 1) {
                advertCount = 1;
            } else if (advertCount > MAX_ADVERT_RADIUS) {
                advertCount = MAX_ADVERT_RADIUS;
            }

            // LOCAL workers
            localWorkers = new HashMap<UUID, Worker>();

            workerResources = new Resources(attributes);

            constituents = new HashMap<UUID, Constituent>();
            // register self
            constituents.put(id, new Constituent(id, endPoint.getID()));

            log("Primary created for " + id);
            logger.info("new job " + id.toString().substring(0, 7)
                    + " submitted");
            setPhase(INITIAL);

            ThreadPool.createNew(this, "Primary of " + this);

        } catch (Exception e) {
            log("could not create primary", e);
            throw e;
        }
    }

    void log(String message) {
        try {
            log.printlog(message);
        } catch (IOException e) {
            logger.warn("could not write to log");
        }
        logger.debug(this.toString() + ": " + message);
    }

    void log(String message, Exception e) {
        try {
            log.printlog(message);
            log.print(e);
        } catch (IOException e2) {
            logger.warn("could not write message to log file", e);
        }
        logger.debug(this.toString() + ": " + message, e);
    }

    @Override
    public boolean isJava() {
        return jobDescription.isJava();
    }

    @Override
    public synchronized int getExitStatus() {
        return exitStatus;
    }

    private synchronized void setExitStatus(int exitStatus) {
        if (exitStatus > this.exitStatus) {
            this.exitStatus = exitStatus;
        }
    }

    private int maxNrOfWorkers() {
        try {
            return attributes.getIntProperty(JobAttributes.COUNT);
        } catch (NumberFormatException e) {
            log("could not find nr of workers", e);
            setPhase(ERROR);
            return 0;
        }
    }

    @Override
    public UUID getID() {
        return id;
    }

    public UUID getNodeID() {
        return node.getID();
    }

    // create new endpoint "local" to this primary
    public EndPoint newEndPoint(String name, Receiver receiver)
            throws IOException {
        return new EndPoint(name, receiver, ibis);
    }

    @Override
    public synchronized void updateAttributes(Map<String, String> attributes)
            throws Exception {
        for (String key : attributes.keySet()) {
            if (!(key.equalsIgnoreCase(JobAttributes.COUNT) && getBooleanAttribute(JobAttributes.MALLEABLE))) {
                throw new Exception("can not update attribute " + key
                        + " while the job is running");
            }
        }

        JobAttributes jobAttributes = new JobAttributes(attributes);
        jobAttributes.checkAttributes();

        this.attributes.putAll(attributes);
        dirty = true;
        notifyAll();
    }

    @Override
    public synchronized void cancel() {
        if (phase < COMPLETED) {
            setPhase(CANCELLED);
        }
    }

    @Override
    public synchronized void end(long deadline) {
        if (deadline < this.deadline) {
            this.deadline = deadline;
            for (Worker worker : localWorkers.values()) {
                worker.signal(deadline);
            }

            dirty = true;
            notifyAll();
        }
    }

    @Override
    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        result.put("ID", getID().toString());
        result.put("primary", "yes");
        result.put("total.workers", String.valueOf(getNrOfWorkers()));
        result.put("local.workers", String.valueOf(localWorkers.size()));
        result.put("phase", phaseString());
        result.put("deadline", Long.toString(deadline));
        result.put("submission.time", new Date(submissiontime).toString());
        result.put("start.time", new Date(starttime).toString());
        result.put("stop.time", new Date(stoptime).toString());
        result.put("executable", jobDescription.getExecutable());

        if (isJava()) {
            result.put("java", "yes");
        } else {
            result.put("java", "no");
        }

        if (stdout != null) {
            result.put("stout", stdout.sandboxPath());
        }
        if (stderr != null) {
            result.put("sterr", stderr.sandboxPath());
        }
        if (stdin != null) {
            result.put("stdin", stdin.sandboxPath());
        }

        if (preStageFiles.length > 0) {
            String inputs = "";
            for (InputFile file : preStageFiles) {
                inputs += file.toString() + "\n";
            }
            result.put("prestage", inputs);
        }

        if (postStageFiles.length > 0) {
            String outputs = "";
            for (PrimaryOutputStream file : postStageFiles) {
                outputs += file.toString() + "\n";
            }
            result.put("poststage", outputs);
        }

        return result;
    }

    @Override
    protected InputFile[] getPreStageFiles() {
        return preStageFiles.clone();
    }

    @Override
    protected String[] getPostStageFiles() {
        String[] result = new String[postStageFiles.length];
        for (int i = 0; i < postStageFiles.length; i++) {
            result[i] = postStageFiles[i].sandboxPath();
        }
        return result;
    }

    protected OutputStream getStdout() {
        return stdout;
    }

    protected InputFile getStdin() {
        return stdin;
    }

    protected OutputStream getStderr() {
        return stderr;
    }

    @Override
    protected String cluster() {
        return cluster;
    }

    @Override
    protected ZorillaPrintStream createLogFile(String fileName)
            throws IOException, Exception {
        return new ZorillaPrintStream(createLogFile(fileName, false));
    }

    private synchronized PrimaryOutputStream createLogFile(String fileName,
            boolean export) throws IOException, Exception {
        File file = new File(logDir, fileName);

        PrimaryOutputStream result = new PrimaryOutputStream(file, this, export);

        logFiles.add(result);

        return result;
    }

    @Override
    protected void flush() {
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
    public synchronized boolean zombie() {
        // primary jobs are NEVER removable from the job list.
        return false;
    }

    public synchronized int getNrOfWorkers() {
        int result = 0;

        for (Constituent constituent : constituents.values()) {
            result += constituent.nrOfWorkers();
        }

        return result;
    }

    private synchronized boolean newWorker(Constituent constituent,
            UUID workerID) {
        if (!getBooleanAttribute(JobAttributes.MALLEABLE)) {
            // workers started by scheduler
            return false;
        }

        if (getNrOfWorkers() >= maxNrOfWorkers()) {
            log("creation of a new worker denied, we already "
                    + "have at least " + maxNrOfWorkers() + " workers");
            return false;
        }

        if (!(phase == SCHEDULING || phase == RUNNING)) {
            return false;
        }
        setPhase(RUNNING);

        // phase == RUNNING

        constituent.addWorker(workerID);

        if (closedWorld() && getNrOfWorkers() == maxNrOfWorkers()) {
            setPhase(CLOSED);
        }

        log("added new worker " + workerID + " on " + constituent + " now "
                + getNrOfWorkers() + " workers");

        logger.info("added new worker " + workerID + " on " + constituent
                + " now " + getNrOfWorkers() + " workers");
        return true;
    }

    private synchronized boolean closedWorld() {
        return !getBooleanAttribute(JobAttributes.MALLEABLE);
    }

    private synchronized void removeWorker(UUID workerID,
            Constituent constituent, Status status, int theExitStatus) {
        // FIXME: refactor all these if statements

        constituent.removeWorker(workerID);

        log("removed worker " + workerID + " on " + constituent
                + " with exit status " + status + "(" + theExitStatus
                + ") now " + getNrOfWorkers() + " workers");

        if (status != Status.DONE) {
            log("worker exited with status: " + status);

        }

        // update exit status
        setExitStatus(exitStatus);

        if (phase == RUNNING) {
            if ((status == Status.DONE && getStringAttribute(JobAttributes.ON_USER_EXIT)
                    .equalsIgnoreCase("close.world"))
                    || (status == Status.USER_ERROR && getStringAttribute(
                            JobAttributes.ON_USER_ERROR).equalsIgnoreCase("close.world"))) {

                setPhase(CLOSED);
            }

            if ((status == Status.DONE && getStringAttribute(JobAttributes.ON_USER_EXIT)
                    .equalsIgnoreCase("cancel.job"))
                    || (status == Status.USER_ERROR && getStringAttribute(
                            JobAttributes.ON_USER_ERROR).equalsIgnoreCase("cancel.job"))) {

                setPhase(CANCELLED);
            }

            if ((status == Status.DONE && getStringAttribute(JobAttributes.ON_USER_EXIT)
                    .equalsIgnoreCase("job.error"))
                    || (status == Status.USER_ERROR && getStringAttribute(
                            JobAttributes.ON_USER_ERROR).equalsIgnoreCase("job.error"))) {

                setPhase(ERROR);
            }

        }

        if (phase == CLOSED) {
            if ((status == Status.DONE && getStringAttribute(JobAttributes.ON_USER_EXIT)
                    .equalsIgnoreCase("cancel.job"))
                    || (status == Status.USER_ERROR && getStringAttribute(
                            JobAttributes.ON_USER_ERROR).equalsIgnoreCase("cancel.job"))) {

                setPhase(CANCELLED);
            }

            if ((status == Status.DONE && getStringAttribute(JobAttributes.ON_USER_EXIT)
                    .equalsIgnoreCase("job.error"))
                    || (status == Status.USER_ERROR && getStringAttribute(
                            JobAttributes.ON_USER_ERROR).equalsIgnoreCase("job.error"))) {

                setPhase(ERROR);
            }
        }

        if (phase == RUNNING && !getBooleanAttribute(JobAttributes.MALLEABLE)) {
            setPhase(CLOSED);
        }

        log("now " + getNrOfWorkers() + " workers");
    }

    private synchronized void removeFinishedLocalWorkers() {
        // clean dead workers
        Iterator<Entry<UUID, Worker>> iterator = localWorkers.entrySet()
                .iterator();
        while (iterator.hasNext()) {
            Entry<UUID, Worker> entry = iterator.next();
            Worker worker = entry.getValue();

            if (worker.finished()) {
                iterator.remove();
                Constituent constituent = constituents.get(id);

                if (constituent == null) {
                    Exception e = new Exception(
                            "cannot find ourselves in constituents!");
                    log(e.getMessage(), e);
                    return;
                }

                removeWorker(worker.id(), constituent, worker.status(), worker
                        .exitStatus());
            }
        }

        if (localWorkers.size() == 0 && phase > RUNNING) {
            if (constituents.remove(id) != null) {
                log("unregisterred outselves");
            }
            log("now " + constituents.size() + " constituents");
            for (UUID id : constituents.keySet()) {
                log(id.toString());
            }
        }

    }

    private synchronized void purgeExpiredConstituents() {
        Iterator<Constituent> iterator = constituents.values().iterator();

        while (iterator.hasNext()) {
            Constituent constituent = iterator.next();
            if (constituent.expired()) {
                if (constituent.getID().equals(endPoint.getID())) {
                    // expired ourselves :(
                    constituent.resetExpirationDate();
                } else {
                    iterator.remove();
                    log("removed expired constituent: " + constituent);
                }
            }
        }
    }

    private void sendStateUpdate() {
        Constituent[] constituents;

        synchronized (this) {
            if (!dirty) {
                return;
            }
            constituents = this.constituents.values().toArray(
                    new Constituent[0]);
        }

        // initiate a callback
        if (callback != null) {
            callback.callback();
        }

        for (int i = 0; i < constituents.length; i++) {
            if (constituents[i].getID().equals(id)) {
                // do not send message to ourselves
                continue;
            }

            try {
                WriteMessage m = endPoint
                        .send(constituents[i].getReceivePort());
                m.writeInt(Copy.STATE_UPDATE);
                writeDynamicState(m);
                m.finish();
            } catch (IOException e) {
                log("could not send update to constituent: " + constituents[i],
                        e);
            }
        }
        synchronized (this) {
            dirty = false;
        }
    }

    /**
     * advertizes this job to neighbours. Uses an increasing radius
     */
    private void advertise() {
        String metric;
        int count;

        synchronized (this) {
            if (getNrOfWorkers() >= maxNrOfWorkers()) {
                logger.debug("not advertising " + this
                        + ": worker limit already reached");
                return;
            }

            if (phase >= CLOSED) {
                logger.debug("not advertising, no more nodes needed");
                return;
            }

            long currentTime = System.currentTimeMillis();

            if (currentTime < (lastAdvertisement + advertTimeout)) {
                logger.debug("not advertising " + this + " to keep down spam");
                return;
            }
            lastAdvertisement = currentTime;

            metric = attributes.getProperty(JobAttributes.ADVERT_METRIC);

            count = advertCount;

            // next advert, advertize further...
            advertCount++;
            // ...and wait twice as long...
            advertTimeout *= 2;

            // ...but not too long
            if (advertTimeout > MAX_ADVERT_TIMEOUT) {
                advertTimeout = MAX_ADVERT_TIMEOUT;
            }
        }

        log("new advertisement  with count: " + count);

        try {
            node.floodService().advertise(
                    new PrimaryCopyAdvert(id, metric, count, endPoint.getID()));

        } catch (Exception e) {
            log("could not send advertizement", e);
            setPhase(ERROR);
        }

    }

    private synchronized void setPhase(int newPhase) {
        if (newPhase == phase) {
            return;
        } else if (newPhase < phase) {
            Exception e = new Exception("tried to revert phase!");
            log("error on setting phase", e);
            phase = ERROR;
        } else {
            phase = newPhase;
        }
        log("phase now " + phaseString());
        logger.info("phase for job " + this + " now " + phaseString());
        dirty = true;
        notifyAll();

        if (phase >= Job.RUNNING && starttime == 0) {
            starttime = System.currentTimeMillis();
        }
        if (phase >= Job.COMPLETED && stoptime == 0) {
            stoptime = System.currentTimeMillis();
        }
    }

    private synchronized void createNewLocalWorkers() {
        if (!isJava() && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
            logger.debug("not creating worker, native jobs not allowed");
            return;
        }

        while (true) {
            if (!getBooleanAttribute("malleable")) {
                // workers started by scheduler
                return;
            }

            if (!(phase == SCHEDULING || phase == RUNNING)) {
                logger.debug("cannot create new worker, wrong phase");
                return;
            }

            if (node.jobService().nrOfResourceSetsAvailable(workerResources) < 1) {
                logger.debug("cannot claim resources");
                return;
            }

            UUID workerID = Node.generateUUID();
            if (!newWorker(constituents.get(id), workerID)) {
                logger.debug("creation of worker denied by newWorker()");
                return;
            }
            Worker worker = new Worker(this, workerID, node, deadline);
            localWorkers.put(workerID, worker);
            node.jobService().setResourcesUsed(getID(),
                    workerResources.mult(localWorkers.size()));

            worker.start();
        }

    }

    private synchronized void killWorkers() {
        for (Worker worker : localWorkers.values()) {
            worker.signal(0);
        }
    }

    private synchronized void finish() {
        try {
            for (InputFile file : preStageFiles) {
                file.close();
            }

            for (PrimaryOutputStream file : postStageFiles) {
                file.unexport();
            }

            for (PrimaryOutputStream file : logFiles) {
                file.unexport();
            }

            if (stdout != null) {
                stdout.unexport();
            }

            if (stdin != null) {
                stdin.close();
            }

            if (stderr != null) {
                stderr.unexport();
            }

            endPoint.close();
            ibis.end();
        } catch (Exception e) {
            log("could not finish job", e);
        }
        log("finished");
    }

    private void handleRegister(Invocation invocation) throws IOException,
            Exception, ClassNotFoundException {
        UUID constituentID = (UUID) invocation.readObject();
        ReceivePortIdentifier receivePort = (ReceivePortIdentifier) invocation
                .readObject();
        invocation.finishRead();

        Constituent constituent = new Constituent(constituentID, receivePort);

        synchronized (this) {
            if (phase > RUNNING) {
                invocation.writeBoolean(false);
                return;
            }

            log("adding new constituent: "
                    + constituentID.toString().substring(0, 7));

            constituents.put(constituentID, constituent);
        }

        invocation.writeBoolean(true);

        // write static part of state
        invocation.writeObject(id);

        invocation.writeObject(jobDescription);

        // write file info

        invocation.writeInt(preStageFiles.length);
        for (InputFile file : preStageFiles) {
            file.writeBootStrap(invocation);
        }

        invocation.writeInt(postStageFiles.length);
        for (PrimaryOutputStream file : postStageFiles) {
            invocation.writeString(file.sandboxPath());
        }

        invocation.writeBoolean(stdout != null);
        if (stdout != null) {
            stdout.writeBootStrap(invocation);
        }

        invocation.writeBoolean(stderr != null);
        if (stderr != null) {
            stderr.writeBootStrap(invocation);
        }

        invocation.writeBoolean(stdin != null);
        if (stdin != null) {
            stdin.writeBootStrap(invocation);
        }

        invocation.writeObject(workerResources);
        writeDynamicState(invocation);
    }

    private void handleUpdateMaxNrOfWorkers(Invocation invocation,
            Constituent constituent) throws IOException {
        int maxNrOfWorkers = invocation.readInt();

        invocation.finishRead();

        constituent.setMaxNrOfWorkers(maxNrOfWorkers);
    }

    private void handleNewWorker(Invocation invocation, Constituent constituent)
            throws IOException, ClassNotFoundException {
        UUID workerID = (UUID) invocation.readObject();

        invocation.finishRead();

        // try to add worker to constituent, report result
        invocation.writeBoolean(newWorker(constituent, workerID));
    }

    private void handleNewLogFile(Invocation invocation, Constituent constituent)
            throws IOException, Exception {
        String fileName = invocation.readString();

        // creates an exported log file
        PrimaryOutputStream file = createLogFile(fileName, true);

        file.writeBootStrap(invocation);
    }

    private void handleGetOutputFile(Invocation invocation,
            Constituent constituent) throws IOException, Exception {
        String path = invocation.readString();

        for (int i = 0; i < postStageFiles.length; i++) {
            if (path.equals(postStageFiles[i].sandboxPath())) {
                postStageFiles[i].writeBootStrap(invocation);
                return;
            }
        }
        throw new Exception("cannot find output file: " + path);
    }

    private void handleRemoveWorker(Invocation invocation,
            Constituent constituent) throws IOException, ClassNotFoundException {
        UUID workerID = (UUID) invocation.readObject();
        Status status = (Status) invocation.readObject();
        int exitStatus = invocation.readInt();

        invocation.finishRead();

        removeWorker(workerID, constituent, status, exitStatus);
    }

    private synchronized void handleUnregister(Invocation invocation,
            Constituent constituent) throws Exception {

        // remove constituent from list
        constituents.remove(constituent.getID());

        log("removed constituent "
                + constituent.getID().toString().substring(0, 7));

        if (constituent.nrOfWorkers() > 0) {
            throw new Exception("removed constituent with workers remaining");
        }
    }

    private synchronized void writeDynamicState(ObjectOutput output)
            throws IOException {
        output.writeObject(getStats());
        output.writeObject(attributes);
        output.writeInt(phase);
        output.writeObject(constituents);
        output.writeLong(deadline - System.currentTimeMillis());
    }

    public void invoke(Invocation invocation) throws Exception {
        Constituent constituent;

        try {
            int opcode = invocation.readInt();

            if (opcode == Primary.REGISTER) {
                handleRegister(invocation);

                return;
            }

            UUID constituentID = (UUID) invocation.readObject();

            log("received an invocation (" + opcode + ") from " + constituentID);

            synchronized (this) {
                constituent = constituents.get(constituentID);
            }

            if (constituent == null) {
                throw new Exception("unknown costituent ("
                        + constituentID.toString().substring(0, 7)
                        + ") in request");
            }

            switch (opcode) {
            case Primary.REQUEST_STATE:
                // NOTHING
                break;
            case Primary.UPDATE_MAX_NR_OF_WORKERS:
                handleUpdateMaxNrOfWorkers(invocation, constituent);
                break;
            case Primary.NEW_WORKER:
                handleNewWorker(invocation, constituent);
                break;
            case Primary.CREATE_LOG_FILE:
                handleNewLogFile(invocation, constituent);
                break;
            case Primary.GET_OUTPUT_FILE:
                handleGetOutputFile(invocation, constituent);
                break;
            case Primary.REMOVE_WORKER:
                handleRemoveWorker(invocation, constituent);
                break;
            case Primary.UNREGISTER:
                handleUnregister(invocation, constituent);
                break;
            default:
                throw new Exception("unknown opcode in message");
            }
            writeDynamicState(invocation);
        } catch (ClassNotFoundException e) {
            log("class not found on reading from message", e);
            throw new Exception("class not found on reading from message", e);
        } catch (IOException e) {
            log("IOEception on handling request", e);
            throw new Exception("IOEception on handling request", e);
        } catch (Exception e) {
            log("execption on handling incoming request", e);
            throw e;
        }
    }

    public void receive(ReadMessage message) {
        Exception e = new Exception("primary received message");
        log(e.getMessage(), e);
    }

    private synchronized UUID[] createWorkers(int nrOfWorkers) {
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

            // claim resources, do not start worker yet.
            localWorkers.put(workerID, worker);
            node.jobService().setResourcesUsed(getID(),
                    workerResources.mult(localWorkers.size()));

        }

        return localWorkers.keySet().toArray(new UUID[0]);
    }

    private void updateLocalMaxWorkers() {
        Resources resources;

        synchronized (this) {
            resources = new Resources(workerResources);
        }

        int maxNrOfWorkers = node.jobService().nrOfResourceSetsAvailable(
                resources);

        synchronized (this) {
            Constituent constituent = constituents.get(id);

            if (constituent == null) {
                log("could not find ourselves in constituents");
                setPhase(ERROR);
                return;
            }

            if (!isJava()
                    && !node.config().getBooleanProperty(Config.NATIVE_JOBS)) {
                maxNrOfWorkers = 0;
            }

            constituent.setMaxNrOfWorkers(maxNrOfWorkers);
        }
    }

    private synchronized Constituent[] getConstituents() {
        return constituents.values().toArray(new Constituent[0]);
    }

    @SuppressWarnings("unchecked")
    private void claimNodes() {
        if (getBooleanAttribute(JobAttributes.MALLEABLE)) {
            log("ERROR: claiming nodes for a malleable job", new Exception());
            setPhase(ERROR);
            return;
        }

        log("claiming nodes");
        Constituent[] constituents;

        constituents = getConstituents();

        int potentialWorkers = 0;
        for (Constituent constituent : constituents) {
            potentialWorkers += constituent.getMaxNrOfWorkers();
        }

        log("potential workers = " + potentialWorkers);

        int neededNrOfWorkers = maxNrOfWorkers();
        if (potentialWorkers < neededNrOfWorkers) {
            log("not enough potential workers");
            return;
        }

        int currentNrOfWorkers = getNrOfWorkers();

        if (currentNrOfWorkers != 0) {
            log("ERROR: number of workers not 0 before claiming workers");
        }

        log("creating workers");

        for (Constituent constituent : constituents) {
            if (currentNrOfWorkers == neededNrOfWorkers) {
                break;
            }

            try {
                if (constituent.getID().equals(id)) {
                    // that's us :)
                    UUID[] workers = createWorkers(neededNrOfWorkers
                            - currentNrOfWorkers);
                    constituent.setWorkers(workers);
                    currentNrOfWorkers += workers.length;
                    log(workers.length + " created at " + constituent);
                    continue;
                }

                Call call = endPoint.call(constituent.getReceivePort());
                call.writeInt(Copy.CREATE_WORKERS);
                call.writeInt(neededNrOfWorkers - currentNrOfWorkers);
                call.call();
                synchronized (this) {
                    UUID[] workers = (UUID[]) call.readObject();
                    constituent.setWorkers(workers);
                    currentNrOfWorkers += workers.length;
                    log(workers.length + " created at " + constituent);
                }
                call.finish();
            } catch (Exception e) {
                synchronized (this) {
                    this.constituents.remove(constituent.getID());
                }
                log("exception on creating worker, removed constituent", e);
            }
        }
        // one or more constituents might have been removed
        constituents = getConstituents();

        currentNrOfWorkers = getNrOfWorkers();
        log("now " + currentNrOfWorkers + " workers");

        // we chould have enought workers now, try to start them...
        try {
            if (getNrOfWorkers() == neededNrOfWorkers) {
                log("starting workers");

                for (Constituent constituent : constituents) {
                    if (constituent.getID().equals(id)) {
                        // that's us :)
                        synchronized (this) {
                            for (Worker worker : localWorkers.values()) {
                                worker.start();
                            }
                        }
                        continue;
                    } else {
                        Call call = endPoint.call(constituent.getReceivePort());
                        call.writeInt(Copy.START_WORKERS);
                        call.call();
                        call.finish();
                    }
                }

                log("succes in claiming nodes");
                setPhase(CLOSED);
                return;
            }

        } catch (Exception e) {
            log("exception on starting workers", e);
        }

        log("destroying workers");
        for (Constituent constituent : constituents) {
            if (constituent.getID().equals(id)) {
                // that's us :)
                synchronized (this) {
                    for (Worker worker : localWorkers.values()) {
                        worker.signal(0); // tell the worker to go away
                    }
                    localWorkers.clear();
                    constituent.setWorkers(new UUID[0]);
                }
                continue;
            }

            try {
                Call call = endPoint.call(constituent.getReceivePort());
                call.writeInt(Copy.DESTROY_WORKERS);
                call.call();
                call.finish();
                // clear worker set
                synchronized (this) {
                    constituent.setWorkers(new UUID[0]);
                }
            } catch (Exception e) {
                synchronized (this) {
                    this.constituents.remove(constituent.getID());
                }
                log("exception on destroying workers, removed constituent", e);
            }
        }
    }

    public void run() {
        boolean done = false;

        setPhase(PRE_STAGE);
        // no work needed for pre-stage

        setPhase(SCHEDULING);

        while (!done) {
            purgeExpiredConstituents();
            removeFinishedLocalWorkers();
            createNewLocalWorkers();
            advertise();
            sendStateUpdate();

            // update resource usage
            node.jobService().setResourcesUsed(getID(),
                    workerResources.mult(localWorkers.size()));

            if (getPhase() == SCHEDULING && !getBooleanAttribute(JobAttributes.MALLEABLE)) {
                updateLocalMaxWorkers();
                claimNodes();
            }

            synchronized (this) {
                if (System.currentTimeMillis() > deadline) {
                    // auto cancel job
                    setPhase(CANCELLED);
                }

                // update state if needed when all constituents exit
                if (phase == CLOSED && constituents.size() == 0) {
                    setPhase(POST_STAGE);
                    setPhase(COMPLETED);
                }

                if (phase >= COMPLETED) {
                    killWorkers();

                    if (constituents.size() == 0) {
                        log("job done, nobody left but us, turning of the light");
                        done = true;
                    }
                }

            }
            synchronized (this) {
                try {
                    wait(WAIT_TIMEOUT);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            }
        }
        // issue final state update callbacks
        sendStateUpdate();
        finish();
        log("job finished");
    }

    @Override
    public synchronized IbisIdentifier getRandomConstituent() {
        ReceivePortIdentifier[] IDs;
        int constituentNr = random.nextInt(constituents.size());

        IDs = constituents.keySet().toArray(new ReceivePortIdentifier[0]);

        return IDs[constituentNr].ibisIdentifier();
    }

    @Override
    protected PrimaryOutputStream getOutputFile(String sandboxPath)
            throws Exception, IOException {
        for (int i = 0; i < postStageFiles.length; i++) {
            if (sandboxPath.equals(postStageFiles[i].sandboxPath())) {
                return postStageFiles[i];
            }
        }
        throw new Exception("cannot find output file: " + sandboxPath);
    }

    @Override
    protected void writeOutputFile(String virtualFilePath, InputStream data)
            throws Exception, IOException {
        PrimaryOutputStream out = getOutputFile(virtualFilePath);

        out.readFrom(data);
    }

    @Override
    public void readStderr(OutputStream out) throws Exception {
        stderr.streamTo(out);
    }

    @Override
    public void readStdout(OutputStream out) throws Exception {
        stdout.streamTo(out);
    }

    public void readOutputFile(String sandboxPath, DataOutputStream out)
            throws Exception {
        PrimaryOutputStream file = getOutputFile(sandboxPath);

        logger.debug("size of output file " + sandboxPath + " = "
                + file.length());

        out.writeLong(file.length());
        file.streamTo(out);
    }

    @Override
    public void writeStdin(InputStream in) throws Exception {
        // TODO: implement stdin forwarding
    }

    @Override
    public ZoniFileInfo getFileInfo(String sandboxPath) throws Exception {
        PrimaryOutputStream file = getOutputFile(sandboxPath);

        if (file == null) {
            throw new Exception(sandboxPath + " does not exist");
        }

        String name = new File(sandboxPath).getName();

        return new ZoniFileInfo(sandboxPath, name, false, new ZoniFileInfo[0]);

    }

    @Override
    public ZorillaJobDescription getDescription() {
        return jobDescription;
    }

}
