package ibis.zorilla.api;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class ZorillaJobDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, String> attributes;
    
    private String[] arguments;
 
    //path used to resolve relative input and output file paths into
    //absolute files (on the submitting node file system)
    private File workingDirectory;
    
    // file on hdd, sandbox path
    private final Map<File, String> inputFiles;

    // sandboxPath, file on hdd
    private final Map<String, File> outputFiles;

    private File stdinFile;

    private File stdoutFile;

    private File stderrFile;

    // if interactive, stdin/out/err are not used
    private boolean interactive;

    protected ZorillaJobDescription() {
        attributes = new HashMap<String, String>();
        
        //set to current working directory by default
        workingDirectory = new File(System.getProperty("user.dir"));
        inputFiles = new HashMap<File, String>();
        outputFiles = new HashMap<String, File>();

        interactive = false;
        
        arguments = new String[0];
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

    public synchronized void setWorkingDirectory(File workingDirectory) {
        this.workingDirectory = workingDirectory;
    }
    
    public synchronized File getWorkingDirectory() {
        return workingDirectory;
    }

    public synchronized void addInputFile(File file, String sandboxPath) {
        inputFiles.put(file, sandboxPath);
    }

    public synchronized void addOutputFile(String sandboxPath, File file) {
        outputFiles.put(sandboxPath, file);
    }

  

    public synchronized Map<String, String> getAttributes() {
        return attributes;
    }

 
 
    public synchronized Map<File, String> getInputFiles() {
        return inputFiles;
    }

   

    public synchronized Map<String, File> getOutputFiles() {
        return outputFiles;
    }

    public synchronized File getStderrFile() {
        return stderrFile;
    }

    public synchronized File getStdinFile() {
        return stdinFile;
    }

    public synchronized File getStdoutFile() {
        return stdoutFile;
    }

    public synchronized boolean isInteractive() {
        return interactive;
    }
    
    public synchronized void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    public synchronized void setAttribute(String key, String value) {
        attributes.put(key, value);
    }

    public synchronized void setAttributes(Map<String, String> attributes) {
        if (attributes == null) {
            this.attributes = new HashMap<String, String>();
        } else {
            this.attributes = new HashMap<String, String>(attributes);
        }
    }
   
    public synchronized void setStderrFile(File file) {
        this.stderrFile = file.getAbsoluteFile();
    }

    public synchronized void setStdinFile(File file) {
        this.stdinFile = file.getAbsoluteFile();
    }

    public synchronized void setStdoutFile(File file) {
        this.stdoutFile = file.getAbsoluteFile();
    }

    protected String toNewLineString(Map<String, String> map) {
        if (map == null || map.size() == 0) {
            return "\n\t-";
        }

        String result = "";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result += "\n\t" + entry.getKey() + " = " + entry.getValue();
        }
        return result;
    }

    protected String toString(Map<String, String> map) {
        if (map == null || map.size() == 0) {
            return "";
        }

        String result = "";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result += entry.getKey() + " = " + entry.getValue() + "<br>";
        }
        return result;
    }
    
    public Map<String, String> toStringMap() {
        Map<String, String> result = new HashMap<String, String>();

        result.put("attributes", toString(attributes));

        result.put("interactive", Boolean.toString(interactive));

        String inputFileString = "";
        for (Map.Entry<File, String> entry : inputFiles.entrySet()) {
            inputFileString += entry.getKey() + " = " + entry.getValue()
                    + "<br>";
        }
        result.put("input.files", inputFileString);

        String outputFileString = "";
        for (Map.Entry<String, File> entry : outputFiles.entrySet()) {
            outputFileString += entry.getKey() + " = " + entry.getValue()
                    + "<br>";
        }
        result.put("output.files", outputFileString);

        result.put("stdin", "" + stdinFile);
        result.put("stdout", "" + stdoutFile);

        result.put("stderr", "" + stderrFile);

        return result;
    }

    public String toMultilineString() {
        String result = "";

        result += "\nAttributes:" + toNewLineString(attributes);
        result += "\nInteractive = " + interactive;

        result += "\nInput files:";
        for (Map.Entry<File, String> entry : inputFiles.entrySet()) {
            result += "\n\t" + entry.getKey() + " = " + entry.getValue();
        }

        result += "\nOutput files:";
        for (Map.Entry<String, File> entry : outputFiles.entrySet()) {
            result += "\n\t" + entry.getKey() + " = " + entry.getValue();
        }

        result += "\nStdin = " + stdinFile;
        result += "\nStdout = " + stdoutFile;
        result += "\nStderr = " + stderrFile;

        return result;
    }

   

   
}
