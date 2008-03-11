package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobDescription {

    private String executable;

    private String[] arguments;

    private final Map<String, String> environment;

    private final Map<String, String> attributes;

    private final List<ZoniInputFile> inputFiles;

    private final List<ZoniOutputFile> outputFiles;

    private File stdinFile;

    private File stdoutFile;

    private File stderrFile;

    // if interactive, stdin/out/err are not used
    private boolean interactive;

    public JobDescription() {
        environment = new HashMap<String, String>();
        attributes = new HashMap<String, String>();
        inputFiles = new ArrayList<ZoniInputFile>();
        outputFiles = new ArrayList<ZoniOutputFile>();

        interactive = false;
    }

    public JobDescription(ZoniInputStream in, File tmpDir) throws IOException {
        executable = in.readString();
        arguments = in.readStringArray();

        environment = in.readStringMap();
        attributes = in.readStringMap();

        interactive = in.readBoolean();

        inputFiles = new ArrayList<ZoniInputFile>();
        int nrOfInputFiles = in.readInt();
        for (int i = 0; i < nrOfInputFiles; i++) {
            inputFiles.add(new ZoniInputFile(in, tmpDir));
        }

        outputFiles = new ArrayList<ZoniOutputFile>();
        int nrOfOutputFiles = in.readInt();
        for (int i = 0; i < nrOfOutputFiles; i++) {
            outputFiles.add(new ZoniOutputFile(in));
        }

        stdinFile = in.readFile();
        stdoutFile = in.readFile();
        stderrFile = in.readFile();
    }

    void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(executable);
        out.writeStringArray(arguments);

        out.writeStringMap(environment);
        out.writeStringMap(attributes);

        out.writeBoolean(interactive);

        out.writeInt(inputFiles.size());
        for (ZoniInputFile file : inputFiles) {
            file.writeTo(out, interactive);
        }

        out.writeInt(outputFiles.size());
        for (ZoniOutputFile file : outputFiles) {
            file.writeTo(out);
        }

        out.writeFile(stdinFile);
        out.writeFile(stdoutFile);
        out.writeFile(stderrFile);
    }

    public void addInputFile(ZoniInputFile file) {
        inputFiles.add(file);
    }

    public ZoniInputFile[] getInputFiles() {
        return inputFiles.toArray(new ZoniInputFile[0]);
    }

    public void addOutputFile(ZoniOutputFile file) {
        outputFiles.add(file);
    }

    public ZoniOutputFile[] getOutputFiles() {
        return outputFiles.toArray(new ZoniOutputFile[0]);
    }

    public String[] getArguments() {
        return arguments;
    }

    public void setArguments(String[] arguments) {
        this.arguments = arguments;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttribute(String key, String value) {
        attributes.put(key, value);
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public void setEnvironment(String key, String value) {
        environment.put(key, value);
    }

    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    public File getStderrFile() {
        return stderrFile;
    }

    public void setStderrFile(File file) {
        this.stderrFile = file.getAbsoluteFile();
    }

    public File getStdinFile() {
        return stdinFile;
    }

    public void setStdinFile(File file) {
        this.stdinFile = file.getAbsoluteFile();
    }

    public File getStdoutFile() {
        return stdoutFile;
    }

    public void setStdoutFile(File file) {
        this.stdoutFile = file.getAbsoluteFile();
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    public boolean isInteractive() {
        return interactive;
    }

}
