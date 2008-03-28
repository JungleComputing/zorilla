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

    private String[] javaOptions;

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

        javaOptions = in.readStringArray();
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

    public String[] getJavaOptions() {
        return javaOptions.clone();
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

    public void setJavaOptions(String[] javaOptions) {
        this.javaOptions = javaOptions.clone();
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
        out.writeString(executable);
        out.writeStringArray(arguments);

        out.writeStringMap(environment);
        out.writeStringMap(attributes);

        out.writeStringArray(javaOptions);
        out.writeStringMap(javaSystemProperties);
        out.writeString(javaMain);
        out.writeStringArray(javaArguments);
        out.writeString(javaClassPath);
        
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

    private String toString(Map<String, String> map) {
        if (map == null || map.size() == 0) {
            return "\n\t-";
        }

        String result = "";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result += "\n\t" + entry.getKey() + " = " + entry.getValue();
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


    public String toString() {
        String result = "";

        result += "\nExecutable = " + executable;
        result += "\nArguments = " + toString(arguments);
        result += "\nEnvironment:" + toString(environment);
        result += "\nAttributes:" + toString(attributes);
        result += "\nJava Options = " + toString(javaOptions);
        result += "\nJava System Properties:" + toString(javaSystemProperties);
        result += "\nJava Main = " + javaMain;
        result += "\nJava Arguments = " + toString(javaArguments);
        result += "\nJava Classpath = " + javaClassPath;
        result += "\nInteractive = " + interactive;
        
        result += "\nInput files:";
        for(ZoniInputFile file: inputFiles) {
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
