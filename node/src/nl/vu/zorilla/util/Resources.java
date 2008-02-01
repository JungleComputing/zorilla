package nl.vu.zorilla.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * specification of a set of resources. Could be used to specify needed
 * resources, avalable resources, etc.
 */
public final class Resources implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int node;

    private final int processors;

    private final long memory; // bytes

    private final long diskSpace; // bytes

    public Resources() {
        this.node = 0;
        this.processors = 0;
        this.memory = 0;
        this.diskSpace = 0;
    }

    public Resources(int node, int processors, long memory, long diskSpace) {
        if (node > 1) {
            throw new Error (
                "cannot create negative resources, or have more than one node");
        }
        this.node = node;
        this.processors = processors;
        this.memory = memory;
        this.diskSpace = diskSpace;

    }
    
    public Resources(Resources original) {
        this.node = original.node;
        this.processors = original.processors;
        this.memory = original.memory;
        this.diskSpace = original.diskSpace;
    }

    public Resources subtract(Resources other) {
        return new Resources(node - other.node, processors - other.processors,
            memory - other.memory, diskSpace - other.diskSpace);
    }

    public Resources add(Resources other) {
        return new Resources(node + other.node, processors + other.processors,
            memory + other.memory, diskSpace + other.diskSpace);
    }

    public Resources mult(int factor) {
        return new Resources(node * factor, processors * factor, memory
            * factor, diskSpace * factor);
    }

    public boolean zero() {
        return node == 0 && processors == 0 && memory == 0 & diskSpace == 0;
    }

    public boolean greaterOrEqualZero() {
        return node >= 0 && processors >= 0 && memory >= 0 && diskSpace >= 0;
    }

    public String toString() {
        return "resources: node=" + node + ", processors=" + processors
            + ", memory=" + memory + ", diskSpace=" + diskSpace;
    }
    
    public Map<String, String> asStringMap() {
        Map<String, String> result = new HashMap<String, String>();
        
        result.put("node", Integer.toString(node));
        result.put("processors", Integer.toString(processors));
        
        result.put("memory", Long.toString(memory));
        result.put("disk.space", Long.toString(diskSpace));
        
        return result;
        
    }

}
