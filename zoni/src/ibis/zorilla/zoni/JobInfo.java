package ibis.zorilla.zoni;

import java.util.HashMap;
import java.util.Map;

public final class JobInfo {

    private final String jobID;

    private final String executable;

    private final Map<String,String> attributes;

    private final Map<String,String> status;

    private final int phase;
    
    private final int exitStatus;

    public JobInfo(String jobID, String executable, Map<String, String> attributes, Map<String,String> status,
        int phase, int exitStatus) {
        this.jobID = jobID;
        this.executable = executable;
        this.attributes = new HashMap<String,String>(attributes);
        this.status = new HashMap<String,String>(status);
        this.phase = phase;
        this.exitStatus = exitStatus;
    }

    public Map<String,String> getAttributes() {
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
    
    public int getExitStatus() {
        return exitStatus;
    }

    public Map<String,String> getStatus() {
        return status;
    }
    
    public String toString() {
        return jobID + "-" + ZoniProtocol.PHASES[phase];
    }

}
