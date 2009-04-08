package ibis.zorilla.slave;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.engine.util.StreamForwarder;
import org.gridlab.gat.io.File;
import org.gridlab.gat.resources.JavaSoftwareDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.security.CertificateSecurityContext;
import org.gridlab.gat.security.SecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.smartsockets.virtual.VirtualSocket;
import ibis.zorilla.Node;
import ibis.zorilla.Service;
import ibis.zorilla.ZorillaProperties;

public class SlaveService implements Service {

    private static final Logger logger = LoggerFactory
            .getLogger(SlaveService.class);

    private static String[] parseSlaveHostnames(String[] strings) {
        if (strings == null || strings.length == 0) {
            return new String[0];
        }

        ArrayList<String> result = new ArrayList<String>();

        for (String string : strings) {
            if (string.contains("[")) {
                String prefix = string.substring(0, string.indexOf('['));
                String range = string.substring(string.indexOf('[') + 1, string
                        .indexOf(']'));
                String postfix = string.substring(string.indexOf(']') + 1,
                        string.length());

                String[] ranges = range.split("-");
                int start = Integer.parseInt(ranges[0]);
                int end = Integer.parseInt(ranges[1]);
                int width = ranges[0].length();

                for (int i = start; i <= end; i++) {
                    result.add(String.format("%s%0" + width + "d%s", prefix, i,
                            postfix));
                }

            } else {
                result.add(string);
            }
        }

        return result.toArray(new String[0]);
    }

    private static GATContext createGATContext() throws Exception {
        GATContext context = new GATContext();
        SecurityContext securityContext = new CertificateSecurityContext(null,
                null, System.getProperty("user.name"), null);
        context.addSecurityContext(securityContext);

        // ensure files are readable on the other side
        context.addPreference("file.chmod", "0755");
        // make sure non existing files/directories will be created on the
        // fly during a copy
        context.addPreference("file.create", "true");

        context.addPreference("resourcebroker.adaptor.name", "sshtrilead");

        context.addPreference("file.adaptor.name", "local,sshtrilead");

        return context;

    }

    private static JobDescription createJobDescription(String nodeAddress, GATContext context)
            throws Exception {
        JavaSoftwareDescription sd = new JavaSoftwareDescription();

        sd.setExecutable(System.getProperty("java.home")
                + java.io.File.separator + "bin" + java.io.File.separator
                + "java");

        // main class and options
        sd.setJavaMain("ibis.zorilla.Main");

        List<String> arguments = new ArrayList<String>();

        arguments.add("--worker");
        arguments.add("--random-ports");
        arguments.add("--peers");
        arguments.add(nodeAddress);

        sd.addAttribute("sandbox.delete", "false");

        sd.setJavaArguments(arguments.toArray(new String[0]));

        // add libraries to pre-stage.
        String[] pathElements = System.getProperty("java.class.path").split(
                java.io.File.pathSeparator);
        for (String pathElement : pathElements) {
            File file = GAT.createFile(context, pathElement);

            if (file.isFile()) {
                logger.debug("adding prestage file: " + pathElement);
                sd.addPreStagedFile(file);
            }
        }

        File file = GAT.createFile(context, "log4j.properties");

        if (file.isFile()) {
            logger.debug("adding prestage file: " + "log4j.properties");
            sd.addPreStagedFile(file);
        }

        sd.setJavaClassPath(".:*");

        sd.enableStreamingStderr(true);
        sd.enableStreamingStdout(true);

        JobDescription result = new JobDescription(sd);

        result.setProcessCount(1);
        result.setResourceCount(1);

        return result;
    }

    private final String nodeAddress;

    private final String[] hostNames;

    private Job[] gatJobs = null;

    public SlaveService(Node node) throws Exception {
        hostNames = parseSlaveHostnames(node.config().getStringList(
                ZorillaProperties.SLAVES));

        nodeAddress = node.network().getAddress().toString();
    }

    @Override
    public Map<String, String> getStats() {
        return new HashMap<String, String>();
    }

    @Override
    public void handleConnection(VirtualSocket socket) {
        logger
                .error("Incoming connection: Connection to slave service not supported");
    }

    @Override
    public synchronized void start() throws Exception {
        if (hostNames == null || hostNames.length == 0) {
            return;
        }

        ArrayList<Job> gatJobs = new ArrayList<Job>();

        GATContext context = createGATContext();
        GAT.setDefaultGATContext(context);

        JobDescription jobDescription = createJobDescription(nodeAddress, context);

        for (String host : hostNames) {
            try {

                logger.info("starting slave on: " + host);
                

                ResourceBroker jobBroker = GAT.createResourceBroker(context,
                        new URI("ssh://" + host));

                Job job = jobBroker.submitJob(jobDescription);

                new StreamForwarder(job.getStdout(), System.out);
                new StreamForwarder(job.getStderr(), System.err);

                gatJobs.add(job);
            } catch (Exception e) {
                logger.error("Error on starting slave on " + host, e);
                this.gatJobs = gatJobs.toArray(new Job[0]);
                return;
            }
        }
        this.gatJobs = gatJobs.toArray(new Job[0]);
    }

    public synchronized void end() {
        if (gatJobs == null) {
            return;
        }

        for (Job job : gatJobs) {
            try {
                job.stop();
            } catch (GATInvocationException e) {
                logger.warn("error on stopping slave", e);
            }
        }

        GAT.end();
    }

}
