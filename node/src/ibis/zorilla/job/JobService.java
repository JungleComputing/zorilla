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

import ibis.smartsockets.direct.DirectSocket;

public final class JobService implements Service, Runnable {

    public static final int JOB_MAINTENANCE_INTERVAL = 60 * 1000;

    public static final int ONE_GB = 1024; // MB

    private static final Logger logger = Logger.getLogger(JobService.class);

    private final Node node;

    private final Map<UUID, Job> jobs;

    private boolean killed = false;

    private final Resources availableResources;

    private final int maxWorkers;

    private final Map<UUID, Resources> usedResources;

    private static int totalMemory() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            long result = (Long) server.getAttribute(new ObjectName(
                    "java.lang:type=OperatingSystem"),
                    "TotalPhysicalMemorySize");
            return (int) (result / 1024.0 / 1024.0);
        } catch (Throwable t) {
            logger.error("could not determine"
                    + " total memory of this machine, using 1Gb", t);
            return ONE_GB;
        }
    }

    public JobService(Node node) throws Exception {
        this.node = node;
        jobs = new HashMap<UUID, Job>();

        createWorkerSecurityFile(new File(node.config().getConfigDir(),
                "worker.security.policy"));

        int defaultWorkers;
        if (node.config().isMaster()) {
            defaultWorkers = 0;
        } else {
            defaultWorkers = Runtime.getRuntime().availableProcessors();
        }

        maxWorkers = node.config().getIntProperty(Config.MAX_WORKERS,
                defaultWorkers);
        logger.info("Maximum of workers on this node: " + maxWorkers);
        
        int totalMemory = totalMemory();
        logger.info("Total memory available: " + totalMemory + " Mb");
        
        int usableDiskSpace = (int) (node.config().getTmpDir().getUsableSpace() / 1024.0 / 1024.0);
        logger.info("Total diskspace available: " + usableDiskSpace + " Mb");

        availableResources = new Resources(false, maxWorkers, totalMemory,
                usableDiskSpace);

        usedResources = new HashMap<UUID, Resources>();
    }

    public int getMaxWorkers() {
        return maxWorkers;
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
    public synchronized Job getJob(UUID jobID) throws Exception {
        Job result = jobs.get(jobID);

        if (result == null) {
            throw new Exception("requested job: " + jobID + " does not exist");
        }

        return result;
    }

    public synchronized Job[] getJobs() {
        return jobs.values().toArray(new Job[0]);
    }

    public Job submitJob(ZorillaJobDescription description, Callback callback)
            throws Exception {

        synchronized (this) {
            if (killed) {
                throw new Exception("job service already killed");
            }
        }
        
        
        if (logger.isDebugEnabled()) {
            logger.debug("New job submitted: " + description.toString());
        }

        Job job = new Primary(description, callback, node);

        synchronized (this) {
            jobs.put(job.getID(), job);
        }

        return job;
    }

    public void handleJobAdvert(Advert advert) {
        try {
            Job job;
            UUID jobID = (UUID) advert.getJobID();
            synchronized (this) {
                job = jobs.get(jobID);

                if (job == null) {
                    logger.debug("recevied job advert for "
                            + jobID.toString().substring(0, 7));

                    job = Job.createConstituent(advert, node);
                    jobs.put(jobID, job);
                }
            }
        } catch (Exception e) {
            logger.warn("error on handling job advert", e);
        }
    }

    public synchronized boolean setResourcesUsed(UUID id, Resources resources) {
        usedResources.put(id, resources);

        return true;
    }

    /**
     * Useful to get resources for workers and such
     */
    public synchronized int nrOfResourceSetsAvailable(Resources request) {
        Resources free = availableResources;

        logger.debug("getting request for resources: " + request);

        for (Resources resources : usedResources.values()) {
            free = free.subtract(resources);
        }

        logger.debug("free resources: " + free);

        if (free.negative()) {
            logger.warn("negative resources free: " + free);
        }

        int result = 0;

        if (request.zero()) {
            // infinite-loop-preventer
            logger
                    .warn(
                            "tried to check number of times \"zero\" resources are available, returning 0",
                            new Exception());
            return 0;
        }

        free = free.subtract(request);
        while (free.greaterOrEqualZero()) {
            result++;

            free = free.subtract(request);
        }

        logger.debug("result for resource request: " + result);

        return result;
    }

    public void start() {
        ThreadPool.createNew(this, "job service");
        logger.info("Started Job service");
    }

    public void handleConnection(DirectSocket socket) {
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
            Iterator<Job> iterator = jobs.values().iterator();
            while (iterator.hasNext()) {
                Job job = iterator.next();

                if (job.zombie()) {
                    iterator.remove();
                }
            }
        }
    }

    public void killAllJobs() {
        Job[] jobs = getJobs();

        for (Job job : jobs) {
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

}
