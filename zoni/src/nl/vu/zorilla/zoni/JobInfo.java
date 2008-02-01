package nl.vu.zorilla.zoni;

import java.util.HashMap;
import java.util.Map;

public final class JobInfo {

    private final String jobID;

    private final String executable;

    private final Map attributes;

    private final Map status;

    private final int phase;

    public JobInfo(String jobID, String executable, Map attributes, Map status,
        int phase) {
        this.jobID = jobID;
        this.executable = executable;
        this.attributes = new HashMap(attributes);
        this.status = new HashMap(status);
        this.phase = phase;
    }

    public Map getAttributes() {
        return attributes;
    }

    public String getExecutable() {
        return executable;
    }

    public String getJobID() {
        return jobID;
    }

    public int getPhase() {
        return phase;
    }

    public Map getStatus() {
        return status;
    }

}
