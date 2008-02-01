package nl.vu.zorilla.stats;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Object with usefull statistical information
 */
public final class Stats implements Serializable {
    
    private static final long serialVersionUID = 0L;

    private final long creationDate;
    
    private final Map<String, Map<String, Object>> stats;
    
    private final UUID nodeID;

    public Stats(UUID nodeID) {
        creationDate = System.currentTimeMillis();
        
        this.nodeID = nodeID;
        
        stats = new HashMap<String, Map<String,Object>>();
    }
    
    public long creationDate() {
        return creationDate;
    }
    
    public UUID getNodeID() {
        return nodeID;
    }
    
    public void put(String category, String name, Object value) {
        Map<String, Object> cat = stats.get(category);
        
        if (cat == null) {
            cat = new HashMap<String, Object>();
            stats.put(category, cat);
        }
        
        cat.put(name, value);
    }
        
    public void put(String category, Map<String, Object> entries) {
        Map<String, Object> cat = stats.get(category);
        
        if (cat == null) {
            cat = new HashMap<String, Object>();
            stats.put(category, cat);
        }

        cat.putAll(entries);
    }
    
    public Object get(String category, String name) {
        Map<String, Object> cat = stats.get(category);
        
        if (cat == null) {
            return null;
        }
        
        return cat.get(name);
    }
    
    public Map<String, Object> get(String category) {
        Map<String, Object> cat = stats.get(category);
        
        if (cat == null) {
            return null;
        }
        return new HashMap<String, Object>(cat);
    }
    
    public String[] getCatagories() {
        return stats.keySet().toArray(new String[0]);
    }
    
}
