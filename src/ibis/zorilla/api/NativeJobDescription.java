package ibis.zorilla.api;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class NativeJobDescription extends ZorillaJobDescription {

    private static final long serialVersionUID = 1L;

    private File executable;
    
    private String[] arguments;

    private Map<String, String> environment;
    
    public NativeJobDescription() {
        environment = new HashMap<String, String>();
        arguments = new String[0];
    }
    
    public synchronized File getExecutable() {
        return executable;
    }


    public synchronized void setEnvironment(String key, String value) {
        environment.put(key, value);
    }

    public synchronized void setExecutable(File executable) {
        this.executable = executable;
    }
    
    public synchronized Map<String, String> getEnvironment() {
        return environment;
    }

    
    public synchronized void setArguments(String[] arguments) {
        if (arguments == null) {
            this.arguments = new String[0];
        } else {
            this.arguments = arguments.clone();
        }
    }
    
    public synchronized String[] getArguments() {
        return arguments.clone();
    }

   

    public synchronized void setEnvironment(Map<String, String> environment) {
        if (environment == null) {
            this.environment = new HashMap<String, String>();
        } else {
            this.environment = new HashMap<String, String>(environment);
        }
    }

    public Map<String, String> toStringMap() {
        Map<String, String> result = new HashMap<String, String>();

        result.put("executable", executable.toString());
        result.put("arguments", Arrays.toString(arguments));
        result.put("environment", toString(environment));
        
        return result;
    }

    public String toString() {
            return "Native job \"" + executable + "\"";
    }

    public String toMultilineString() {
        String result = "";

        result += "\nExecutable = " + executable;
        result += "\nArguments = " + Arrays.toString(arguments);
        result += "\nEnvironment:" + toNewLineString(environment);
        return result;
    }

    
    
}
