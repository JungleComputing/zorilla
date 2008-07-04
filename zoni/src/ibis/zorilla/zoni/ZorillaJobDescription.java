package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZorillaJobDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private String executable;

    private String[] arguments;

    private Map<String, String> environment;

    private Map<String, String> attributes;

    private Map<String, String> javaSystemProperties;

    private String javaMain;

    private String[] javaArguments;

    private String javaClassPath;

    private final List<ZoniInputFile> inputFiles;

    // sandboxPath, file on hdd (file only used in batch files)
    private final Map<String, File> outputFiles;

    private File stdinFile;

    private File stdoutFile;

    private File stderrFile;

    // if interactive, stdin/out/err are not used
    private boolean interactive;

    public ZorillaJobDescription() {
        environment = new HashMap<String, String>();
        attributes = new HashMap<String, String>();
        javaSystemProperties = new HashMap<String, String>();
        inputFiles = new ArrayList<ZoniInputFile>();
        outputFiles = new HashMap<String, File>();
        
        arguments = new String[0];
        javaArguments = new String[0];

        interactive = false;
    }

    @SuppressWarnings("unchecked")
    public ZorillaJobDescription(ObjectInputStream in, File tmpDir)
            throws IOException, ClassNotFoundException {
        executable = (String) in.readObject();
        arguments = (String[]) in.readObject();
        environment = (Map<String, String>) in.readObject();
        attributes = (Map<String, String>) in.readObject();

        javaSystemProperties = (Map<String, String>) in.readObject();
        javaMain = (String) in.readObject();
        javaArguments = (String[]) in.readObject();
        javaClassPath = (String) in.readObject();

        interactive = in.readBoolean();

        inputFiles = new ArrayList<ZoniInputFile>();
        int nrOfInputFiles = in.readInt();
        for (int i = 0; i < nrOfInputFiles; i++) {
            inputFiles.add(new ZoniInputFile(in, tmpDir));
        }

        outputFiles = (Map<String, File>) in.readObject();

        stdinFile = (File) in.readObject();
        stdoutFile = (File) in.readObject();
        stderrFile = (File) in.readObject();
    }

    public synchronized void addInputFile(ZoniInputFile file) {
        inputFiles.add(file);
    }

    public synchronized void addOutputFile(String sandboxPath, File file) {
        outputFiles.put(sandboxPath, file);
    }

    public synchronized String[] getArguments() {
        return arguments.clone();
    }

    public synchronized Map<String, String> getAttributes() {
        return attributes;
    }

    public synchronized Map<String, String> getEnvironment() {
        return environment;
    }

    public synchronized String getExecutable() {
        return executable;
    }

    public synchronized ZoniInputFile[] getInputFiles() {
        return inputFiles.toArray(new ZoniInputFile[0]);
    }

    public synchronized String[] getJavaArguments() {
        return javaArguments.clone();
    }

    public synchronized String getJavaMain() {
        return javaMain;
    }

    public synchronized String getJavaClassPath() {
        return javaClassPath;
    }

    public synchronized Map<String, String> getJavaSystemProperties() {
        return javaSystemProperties;
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

    public synchronized boolean isJava() {
        return (javaMain != null) && (javaMain.length() != 0);
    }

    public synchronized void setArguments(String[] arguments) {
    	if (arguments == null) {
    		this.arguments = new String[0];
    	} else {
    		this.arguments = arguments.clone();
    	}
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

    public synchronized void setEnvironment(Map<String, String> environment) {
        if (environment == null) {
            this.environment = new HashMap<String, String>();
        } else {
            this.environment = new HashMap<String, String>(environment);
        }
    }

    public synchronized void setEnvironment(String key, String value) {
        environment.put(key, value);
    }

    public synchronized void setExecutable(String executable) {
        this.executable = executable;
    }

    public synchronized void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    public synchronized void setJavaArguments(String[] javaArguments) {
    	if (javaArguments == null) {
    		this.javaArguments = new String[0];
    	} else {
        this.javaArguments = javaArguments.clone();
    	}
    }

    public synchronized void setJavaMain(String javaMain) {
        this.javaMain = javaMain;
    }

    public synchronized void setJavaClassPath(String javaClassPath) {
        this.javaClassPath = javaClassPath;
    }

    public synchronized void setJavaSystemProperties(
            Map<String, String> properties) {
        if (properties == null) {
            this.javaSystemProperties = new HashMap<String, String>();
        } else {
            this.javaSystemProperties = new HashMap<String, String>(properties);
        }
    }

    public synchronized void setJavaSystemProperty(String key, String value) {
        javaSystemProperties.put(key, value);
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

    synchronized void writeTo(ObjectOutputStream out) throws IOException {
        out.writeObject(executable);
        out.writeObject(arguments);
        out.writeObject(environment);
        out.writeObject(attributes);
        out.writeObject(javaSystemProperties);
        out.writeObject(javaMain);
        out.writeObject(javaArguments);
        out.writeObject(javaClassPath);

        out.writeBoolean(interactive);

        out.writeInt(inputFiles.size());
        for (ZoniInputFile file : inputFiles) {
            file.writeTo(out, interactive);
        }

        out.writeObject(outputFiles);

        out.writeObject(stdinFile);
        out.writeObject(stdoutFile);
        out.writeObject(stderrFile);
    }

    private String toNewLineString(Map<String, String> map) {
        if (map == null || map.size() == 0) {
            return "\n\t-";
        }

        String result = "";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result += "\n\t" + entry.getKey() + " = " + entry.getValue();
        }
        return result;
    }

    private String toString(Map<String, String> map) {
        if (map == null || map.size() == 0) {
            return "";
        }

        String result = "";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result += entry.getKey() + " = " + entry.getValue() + "<br>";
        }
        return result;
    }

    private String toString(String[] array) {
        if (array == null) {
            return "";
        }

        String result = "";
        for (String element : array) {
            result += element + " ";
        }
        return result;
    }

    public Map<String, String> toStringMap() {
        Map<String, String> result = new HashMap<String, String>();

        result.put("executable", executable);
        result.put("arguments", toString(arguments));
        result.put("environment", toString(environment));
        result.put("attributes", toString(attributes));

        result.put("java.system.properties", toString(javaSystemProperties));
        result.put("java Main", javaMain);
        result.put("java Arguments", toString(javaArguments));
        result.put("java Classpath", javaClassPath);

        result.put("interactive", Boolean.toString(interactive));

        String inputFileString = "";
        for (ZoniInputFile file : inputFiles) {
            inputFileString += file + "<br>";
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

    public String toString() {
        String result = "";

        result += "\nExecutable = " + executable;
        result += "\nArguments = " + toString(arguments);
        result += "\nEnvironment:" + toNewLineString(environment);
        result += "\nAttributes:" + toNewLineString(attributes);
        result += "\nJava System Properties:"
                + toNewLineString(javaSystemProperties);
        result += "\nJava Main = " + javaMain;
        result += "\nJava Arguments = " + toString(javaArguments);
        result += "\nJava Classpath = " + javaClassPath;
        result += "\nInteractive = " + interactive;

        result += "\nInput files:";
        for (ZoniInputFile file : inputFiles) {
            result += "\n\t" + file;
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
