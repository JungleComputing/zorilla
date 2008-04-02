package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZorillaJobDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private String executable;

    private String[] arguments;

    private final Map<String, String> environment;

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

        interactive = false;
    }

    public ZorillaJobDescription(ZoniInputStream in, File tmpDir)
            throws IOException {
        executable = in.readString();
        arguments = in.readStringArray();
        environment = in.readStringMap();
        attributes = in.readStringMap();

        javaSystemProperties = in.readStringMap();
        javaMain = in.readString();
        javaArguments = in.readStringArray();
        javaClassPath = in.readString();

        interactive = in.readBoolean();

        inputFiles = new ArrayList<ZoniInputFile>();
        int nrOfInputFiles = in.readInt();
        for (int i = 0; i < nrOfInputFiles; i++) {
            inputFiles.add(new ZoniInputFile(in, tmpDir));
        }

        outputFiles = new HashMap<String, File>();
        int nrOfOutputFiles = in.readInt();
        for (int i = 0; i < nrOfOutputFiles; i++) {
            outputFiles.put(in.readString(), in.readFile());
        }

        stdinFile = in.readFile();
        stdoutFile = in.readFile();
        stderrFile = in.readFile();
    }

    public void addInputFile(ZoniInputFile file) {
        inputFiles.add(file);
    }

    public void addOutputFile(String sandboxPath, File file) {
        outputFiles.put(sandboxPath, file);
    }

    public String[] getArguments() {
        return arguments.clone();
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public String getExecutable() {
        return executable;
    }

    public ZoniInputFile[] getInputFiles() {
        return inputFiles.toArray(new ZoniInputFile[0]);
    }

    public String[] getJavaArguments() {
        return javaArguments.clone();
    }

    public String getJavaMain() {
        return javaMain;
    }

    public String getJavaClassPath() {
        return javaClassPath;
    }

    public Map<String, String> getJavaSystemProperties() {
        return javaSystemProperties;
    }

    public Map<String, File> getOutputFiles() {
        return outputFiles;
    }

    public File getStderrFile() {
        return stderrFile;
    }

    public File getStdinFile() {
        return stdinFile;
    }

    public File getStdoutFile() {
        return stdoutFile;
    }

    public boolean isInteractive() {
        return interactive;
    }

    public boolean isJava() {
        return (javaMain != null) && (javaMain.length() != 0);
    }

    public void setArguments(String[] arguments) {
        this.arguments = arguments.clone();
    }

    public void setAttribute(String key, String value) {
        attributes.put(key, value);
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = new HashMap<String, String>(attributes);
    }

    public void setEnvironment(Map<String, String> environment) {
        this.attributes = new HashMap<String, String>(environment);
    }

    public void setEnvironment(String key, String value) {
        environment.put(key, value);
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    public void setJavaArguments(String[] javaArguments) {
        this.javaArguments = javaArguments.clone();
    }

    public void setJavaMain(String javaMain) {
        this.javaMain = javaMain;
    }

    public void setJavaClassPath(String javaClassPath) {
        this.javaClassPath = javaClassPath;
    }

    public void setJavaSystemProperties(Map<String, String> properties) {
        this.javaSystemProperties = new HashMap<String, String>(properties);
    }

    public void setJavaSystemProperty(String key, String value) {
        javaSystemProperties.put(key, value);
    }

    public void setStderrFile(File file) {
        this.stderrFile = file.getAbsoluteFile();
    }

    public void setStdinFile(File file) {
        this.stdinFile = file.getAbsoluteFile();
    }

    public void setStdoutFile(File file) {
        this.stdoutFile = file.getAbsoluteFile();
    }

    void writeTo(ZoniOutputStream out) throws IOException {
    	//FIXME: remove all these flushes :(
    	
        out.writeString(executable);
        out.flush();
        out.writeStringArray(arguments);
        out.flush();

        out.writeStringMap(environment);
        out.flush();

        out.writeStringMap(attributes);
        out.flush();

        out.writeStringMap(javaSystemProperties);
        out.flush();

        out.writeString(javaMain);
        out.flush();

        out.writeStringArray(javaArguments);
        out.flush();

        out.writeString(javaClassPath);
        out.flush();

        out.writeBoolean(interactive);

        out.writeInt(inputFiles.size());
        for (ZoniInputFile file : inputFiles) {
            file.writeTo(out, interactive);
        }

        out.writeInt(outputFiles.size());
        for (Map.Entry<String, File> entry : outputFiles.entrySet()) {
            out.writeString(entry.getKey());
            out.writeFile(entry.getValue());
        }

        out.writeFile(stdinFile);
        out.writeFile(stdoutFile);
        out.writeFile(stderrFile);
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
        result += "\nJava System Properties:" + toNewLineString(javaSystemProperties);
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
