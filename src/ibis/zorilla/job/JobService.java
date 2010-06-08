package ibis.zorilla.job;

import ibis.util.ThreadPool;
import ibis.zorilla.Node;
import ibis.zorilla.Config;
import ibis.zorilla.Service;
import ibis.zorilla.zoni.ZorillaJobDescription;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.log4j.Logger;

import ibis.smartsockets.virtual.VirtualSocket;

public final class JobService implements Service, Runnable {

    public static final int JOB_MAINTENANCE_INTERVAL = 60 * 1000;

    public static final int ONE_GB = 1024; // MB

    private static final Logger logger = Logger.getLogger(JobService.class);

    private final Node node;

    private final Map<UUID, ZorillaJob> jobs;

    private boolean killed = false;

    private final int nodes;

    private final String[] hostnames;

    private final Resources nodeResources;

    private final Worker[] workers;

    // return 90% of physical memory size as max memory available
    private static int freeMemory() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            long total = (Long) server.getAttribute(new ObjectName(
                    "java.lang:type=OperatingSystem"),
                    "TotalPhysicalMemorySize");

            return (int) ((total / 1024.0 / 1024.0) * 0.9);
        } catch (Throwable t) {
            logger.error("could not determine"
                    + " total memory of this machine, using 1Gb", t);
            return ONE_GB;
        }
    }

    public JobService(Node node) throws Exception {
        this.node = node;
        jobs = new HashMap<UUID, ZorillaJob>();

        createWorkerSecurityFile(new File(node.config().getConfigDir(),
                "worker.security.policy"));

        int availableCores = node.config().getIntProperty(
                Config.RESOURCE_CORES,
                Runtime.getRuntime().availableProcessors());
        logger.info("Available cores on each node: " + availableCores);

        int freeMemory = node.config().getIntProperty(Config.RESOURCE_MEMORY,
                freeMemory());
        logger.info("Total memory available: " + freeMemory + " Mb");

        int usableDiskSpace = (int) (node.config().getTmpDir().getUsableSpace() / 1024.0 / 1024.0);
        logger.info("Total diskspace available: " + usableDiskSpace + " Mb");

        nodeResources = new Resources(availableCores, freeMemory,
                usableDiskSpace);

        nodes = node.config().getIntProperty(Config.RESOURCE_NODES, 1);
        String[] hostnames = node.config().getHosts();
        if (hostnames == null) {
            hostnames = new String[nodes];
            for (int i = 0; i < nodes; i++) {
                hostnames[i] = "localhost";
            }
        }

        if (hostnames.length < nodes) {
            throw new Exception("cannot initialize JobService, only "
                    + hostnames.length + " hostnames provided, but " + nodes
                    + " required.");
        }

        this.hostnames = hostnames;

        workers = new Worker[nodes];

        logger.info("Available nodes: " + nodes);
    }

    private static void createWorkerSecurityFile(File file) throws Exception {
        if (file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            throw new Exception(
                    "could not create worker security file, already a directory: "
                            + file);
        }

        logger.info("Creating worker security file: " + file);

        try {

            FileWriter writer = new FileWriter(file);

            writer
                    .write("// Zorilla worker security file. All applications will be limited\n"
                            + "// to these permissions when running. To print a list of all permissions used\n"
                            + "// by an application set the java.security.debug property to \"access\"\n"
                            + "grant {\n"
                            + "\tpermission java.io.FilePermission \"-\", \"read, write, execute, delete\";\n"
                            + "\tpermission java.io.FilePermission \".\", \"read, write, execute, delete\";\n"
                            + "\tpermission java.net.SocketPermission \"*\", \"resolve,accept,connect,listen\";\n"
                            + "\n"
                            + "\t//for System.getProperties()\n"
                            + "\tpermission java.util.PropertyPermission \"*\", \"read,write\";\n"

                            + "\t//to create Classloaders\n"
                            + "\tpermission java.lang.RuntimePermission \"createClassLoader\";\n"
                            + "\n"
                            + "\t//for overriding serialization code (used in Ibis)\n"
                            + "\tpermission java.io.SerializablePermission \"enableSubclassImplementation\", \"\";\n"
                            + "\tpermission java.lang.reflect.ReflectPermission \"suppressAccessChecks\", \"\";\n"
                            + "\tpermission java.lang.RuntimePermission \"accessClassInPackage.sun.misc\", \"\";\n"
                            + "\tpermission java.lang.RuntimePermission \"accessDeclaredMembers\", \"\";\n"
                            + "\tpermission java.lang.RuntimePermission \"shutdownHooks\", \"\";\n"

                            + "};\n");

            writer.flush();
            writer.close();

        } catch (IOException e) {
            throw new Exception(
                    "could not create worker security file " + file, e);
        }
    }

    /**
     * returns the job with the given ID
     * 
     * @throws Exception
     *             if there is no ZorillaJobDescription for the given ID
     */
    public synchronized ZorillaJob getJob(UUID jobID) throws Exception {
        ZorillaJob result = jobs.get(jobID);

        if (result == null) {
            throw new Exception("requested job: " + jobID + " does not exist");
        }

        return result;
    }

    public synchronized ZorillaJob[] getJobs() {
        return jobs.values().toArray(new ZorillaJob[0]);
    }

    public ZorillaJob submitJob(ZorillaJobDescription description,
            Callback callback) throws Exception {

        synchronized (this) {
            if (killed) {
                throw new Exception("job service already killed");
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("New job submitted: " + description.toMultilineString());
        } else {
            logger.info("New job submitted: " + description.toString());
        }

        ZorillaJob job = new Primary(description, callback, node);

        synchronized (this) {
            jobs.put(job.getID(), job);
        }

        return job;
    }

    public void handleJobAdvert(Advert advert) {
        try {
            ZorillaJob job;
            UUID jobID = advert.getJobID();
            synchronized (this) {
                job = jobs.get(jobID);

                if (job == null) {
                    logger.debug("recevied job advert for "
                            + jobID.toString().substring(0, 7));

                    job = ZorillaJob.createConstituent(advert, node);
                    jobs.put(jobID, job);
                }
            }
        } catch (Exception e) {
            logger.warn("error on handling job advert", e);
        }
    }

    public boolean resourcesAvailable(Resources workerResources) {
        return nodeResources.subtract(workerResources).greaterOrEqualZero();
    }

    public synchronized String addWorker(Worker worker) {
        for (int i = 0; i < workers.length; i++) {
            if (workers[i] == null || workers[i].finished()) {
                workers[i] = worker;
                return hostnames[i];
            }
        }

        // no node available
        return null;
    }

    public void start() {
        ThreadPool.createNew(this, "job service");
        logger.info("Started Job service");
    }

    public void handleConnection(VirtualSocket socket) {
        logger.error("Incoming connection to JobService");
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();
        return result;
    }

    /**
     * cleanup and maintenance thread
     */
    public synchronized void run() {
        while (true) {
            try {
                wait(JOB_MAINTENANCE_INTERVAL);
            } catch (InterruptedException e) {
                // IGNORE
            }

            // purge dead jobs
            Iterator<ZorillaJob> iterator = jobs.values().iterator();
            while (iterator.hasNext()) {
                ZorillaJob job = iterator.next();

                if (job.zombie()) {
                    iterator.remove();
                }
            }
        }
    }

    public void killAllJobs() {
        ZorillaJob[] jobs = getJobs();

        for (ZorillaJob job : jobs) {
            try {
                job.cancel();
            } catch (Exception e) {
                logger.error("Exception while cancelling job", e);
            }
        }
        synchronized (this) {
            killed = true;
        }
    }

    public Resources getResources() {
        return nodeResources;
    }

    public synchronized int freeNodes() {
        int result = 0;

        for (Worker worker : workers) {
            if (worker == null || worker.finished()) {
                result++;
            }

        }
        return result;
    }

}
