package ibis.zorilla.job;

import ibis.util.TypedProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class JobAttributes extends TypedProperties {

    private static Logger logger = Logger.getLogger(JobAttributes.class);

    private static final long serialVersionUID = 1L;

    public static final String DIRECTORY = "directory";

    public static final String COUNT = "count";

    public static final String HOST_COUNT = "host.count";

    public static final String TIME_MAX = "time.max";

    public static final String WALLTIME_MAX = "walltime.max";

    public static final String CPUTIME_MAX = "cputime.max";

    public static final String JOB_TYPE = "job.type";

    public static final String PROJECT = "project";

    public static final String DRY_RUN = "dryrun";

    public static final String MEMORY_MIN = "memory.min";

    public static final String MEMORY_MAX = "memory.max";

    public static final String DISK_SPACE = "disk.space";

    public static final String SAVE_STATE = "savestate";

    public static final String RESTART = "restart";

    public static final String ON_USER_EXIT = "on.user.exit";

    public static final String ON_USER_ERROR = "on.user.error";

    public static final String MALLEABLE = "malleable";
    public static final String CLASSPATH = "classpath";
    public static final String SPLIT_STDOUT = "split.stdout";
    public static final String SPLIT_STDERR = "split.stderr";

    public static final String ADVERT_METRIC = "advert.metric";

    // constants

    public static final long MAX_JOB_LIFETIME = 8 * 60; // 8 hours

    private static final String[][] propertiesList = {
            { DIRECTORY, null, "unused" },
            { COUNT, null, "Number of executables started" },
            { HOST_COUNT, null, "Number of machines used" },
            { TIME_MAX, null, "unused" },
            { WALLTIME_MAX, "15",
                    "Maximum run time of each executable in minutes" },
            { CPUTIME_MAX, null, "unused" },
            { JOB_TYPE, null, "unused" },
            { PROJECT, null, "unused" },
            { DRY_RUN, null, "unused" },
            { MEMORY_MIN, null, "unused" },
            { MEMORY_MAX, "768", "Memory needed per executable in MB" },
            { DISK_SPACE, "100", "Diskspace needed per executable in MB" },
            { SAVE_STATE, null, "unused" },
            { RESTART, null, "unused" },
            { ON_USER_EXIT, "close.world",
                    "Action taken when a worker exists with status 0" },
            { ON_USER_ERROR, "job.error",
                    "Action taken when a worker exists with non 0 status" },

            { MALLEABLE, "true",
                    "If true, workers can be added and removed during the run" },
            { CLASSPATH, null, "unused" },
            { SPLIT_STDOUT, "false",
                    "if true, stdout is a directory with a file for each worker" },
            { SPLIT_STDERR, "false",
                    "if true, stdout is a directory with a file for each worker" }, };

    JobAttributes(Map<String, String> values) throws Exception {
        super(getHardcodedProperties());

        for (Map.Entry<String, String> entry : values.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }

        checkAttributes();
    }

    JobAttributes() throws Exception {
        super();
    }

    void checkAttributes() throws Exception {

        TypedProperties wrong = checkProperties(null, JobAttributes
                .getValidKeys(), null, false);

        if (wrong.size() > 0) {
            logger.warn("invalid attributes: " + wrong.toString());
        }

        if (getIntProperty(JobAttributes.COUNT, 1) < 1) {
            throw new Exception("count must be a positive integer");
        }

        if (getIntProperty(JobAttributes.HOST_COUNT, 1) < 1) {
            throw new Exception("host count must be a positive integer");
        }

        if (getProperty(JobAttributes.COUNT) == null
                && getProperty(JobAttributes.HOST_COUNT) == null) {
            throw new Exception("must either specify " + JobAttributes.COUNT
                    + " or " + JobAttributes.HOST_COUNT);
        }
       
        if (getIntProperty(MEMORY_MAX, 1) < 0) {
            throw new Exception("max.memory attribute invalid: "
                    + getProperty("max.memory"));
        }

        if (getIntProperty(MEMORY_MIN, 1) < 0) {
            throw new Exception("max.memory attribute invalid: "
                    + getProperty("max.memory"));
        }

        String onUserExit = getProperty(ON_USER_EXIT);

        if (onUserExit != null
                && !(onUserExit.equalsIgnoreCase("ignore")
                        || onUserExit.equalsIgnoreCase("cancel.job")
                        || onUserExit.equalsIgnoreCase("close.world") || onUserExit
                        .equalsIgnoreCase("job.error"))) {
            throw new Exception("invalid value for " + ON_USER_EXIT + " : "
                    + onUserExit);
        }

        String onUserError = getProperty(ON_USER_ERROR);

        if (onUserError != null
                && !(onUserError.equalsIgnoreCase("ignore")
                        || onUserError.equalsIgnoreCase("cancel.job")
                        || onUserError.equalsIgnoreCase("close.world") || onUserError
                        .equalsIgnoreCase("job.error"))) {
            throw new Exception("invalid value for " + ON_USER_ERROR + ": "
                    + onUserError);
        }

        long lifetime = getLongProperty(WALLTIME_MAX);
        if (lifetime > MAX_JOB_LIFETIME) {
            throw new Exception("job lifetime cannot be more than "
                    + MAX_JOB_LIFETIME);
        }
    }

    /**
     * Returns the built-in properties of Ibis.
     * 
     * @return the resulting properties.
     */
    public static Properties getHardcodedProperties() {
        Properties properties = new Properties();

        for (String[] element : propertiesList) {
            if (element[1] != null) {
                properties.setProperty(element[0], element[1]);
            }
        }

        return properties;
    }

    public static String[] getValidKeys() {
        ArrayList<String> result = new ArrayList<String>();

        for (int i = 0; i < propertiesList.length; i++) {
            result.add(propertiesList[i][0]);
        }

        return result.toArray(new String[0]);
    }

    /**
     * Returns a map mapping hard-coded property names to their descriptions.
     * 
     * @return the name/description map.
     */
    public static Map<String, String> getDescriptions() {
        Map<String, String> result = new LinkedHashMap<String, String>();

        for (String[] element : propertiesList) {
            result.put(element[0], element[2]);
        }

        return result;
    }

    public File getFileProperty(String key) {
        String property = getProperty(key);

        if (property == null) {
            return null;
        }

        return new File(getProperty(key));
    }

    public int getMaxWorkers() {
        if (getProperty(COUNT) != null) {
            return getIntProperty(COUNT);
        }

        if (getProperty(HOST_COUNT) != null) {
            return getIntProperty(HOST_COUNT);
        }

        return 1;
    }

    public Map<String, String> getStringMap() {
        Map<String, String> result = new HashMap<String, String>();

        for (Enumeration<?> e = propertyNames(); e.hasMoreElements();) {
            String name = (String) e.nextElement();
            
            result.put(name, getProperty(name));
        }

        return result;
    }

}
