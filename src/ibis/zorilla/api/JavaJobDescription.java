package ibis.zorilla.api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JavaJobDescription extends ZorillaJobDescription {
    
    public JavaJobDescription() {
        super();
        
        systemProperties = new HashMap<String, String>();
        arguments = new String[0];
    }

    private static final long serialVersionUID = 1L;
    
    private Map<String, String> systemProperties;

    private String main;

    private String[] arguments;

    private String classPath;
    
    public synchronized String[] getArguments() {
        return arguments.clone();
    }

    public synchronized String getJavaMain() {
        return main;
    }

    public synchronized String getJavaClassPath() {
        return classPath;
    }

    public synchronized Map<String, String> getSystemProperties() {
        return systemProperties;
    }
    
    public synchronized void setMain(String main) {
        this.main = main;
    }

    public synchronized void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public synchronized void setSystemProperties(
            Map<String, String> properties) {
        if (properties == null) {
            this.systemProperties = new HashMap<String, String>();
        } else {
            this.systemProperties = new HashMap<String, String>(properties);
        }
    }

    public synchronized void setSystemProperty(String key, String value) {
        systemProperties.put(key, value);
    }
    
    public Map<String, String> toStringMap() {
        Map<String, String> result = new HashMap<String, String>();

        result.put("System.properties", toString(systemProperties));
        result.put("Main", main);
        result.put("Arguments", Arrays.toString(arguments));
        result.put("Classpath", classPath);

        return result;
    }

    public String toString() {
            return "Java job \"" + main + "\"";
    }

    public String toMultilineString() {
        String result = super.toMultilineString();

        result += "\nArguments = " + Arrays.toString(arguments);
        
        result += "\nSystem Properties:"
                + toNewLineString(systemProperties);
        result += "\nMain = " + main;
        result += "\nClasspath = " + classPath;

        return result;
    }

}
