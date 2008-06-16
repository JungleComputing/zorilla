package ibis.zorilla.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * specification of a set of resources. Could be used to specify needed
 * resources, avalable resources, etc.
 */
public final class Resources implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean claimNode;
    
    private final int processors;

    private final int memory; // MB

    private final int diskSpace; // MB

    public Resources() {
        this.claimNode = false;
        this.processors = 0;
        this.memory = 0;
        this.diskSpace = 0;
    }

    public Resources(boolean claimNode,  int processors, int memory, int diskspace) {
        this.claimNode = claimNode;
        this.processors = processors;
        this.memory = memory;
        this.diskSpace = diskspace;
    }

    
    public Resources(JobAttributes attributes) {
    	//FIXME:hack
        //claimNode = attributes.getProperty(JobAttributes.HOST_COUNT) != null;
        claimNode = false;
        processors = attributes.getIntProperty(JobAttributes.COUNT);
        memory = attributes.getIntProperty(JobAttributes.MEMORY_MAX);
        diskSpace = attributes.getIntProperty(JobAttributes.DISK_SPACE);
    }
    
    public Resources(Resources original) {
        this.claimNode = original.claimNode;
        this.processors = original.processors;
        this.memory = original.memory;
        this.diskSpace = original.diskSpace;
    }

    public Resources subtract(Resources other) {
        return new Resources(claimNode, processors - other.processors, memory - other.memory, diskSpace - other.diskSpace);
    }

    public Resources add(Resources other) {
        return new Resources(claimNode, processors + other.processors,
            memory + other.memory, diskSpace + other.diskSpace);
    }

    public Resources mult(int factor) {
        return new Resources(claimNode, processors * factor, memory
            * factor, diskSpace * factor);
    }

    public boolean zero() {
        return processors == 0 && memory == 0 & diskSpace == 0;
    }

    public boolean greaterOrEqualZero() {
        return processors >= 0 && memory >= 0 && diskSpace >= 0;
    }
    
    public boolean negative() {
        return !greaterOrEqualZero();
    }

    public String toString() {
        return "resources: claim node=" + claimNode + ", processors = " + processors
            + ", memory=" + memory + ", diskSpace=" + diskSpace;
    }
    
    public Map<String, String> asStringMap() {
        Map<String, String> result = new HashMap<String, String>();
        
        result.put("claimNode", "" + claimNode);
        result.put("processors", Integer.toString(processors));
        result.put("memory", Integer.toString(memory));
        result.put("disk.space", Integer.toString(diskSpace));
        
        return result;
        
    }

}
