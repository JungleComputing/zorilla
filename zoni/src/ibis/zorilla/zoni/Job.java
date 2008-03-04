package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Job {

    private final String executable;

    private final String[] arguments;

    private final Map<String, String> environment;

    private final Map<String, String> attributes;

    private final InputFile[] inputFiles;

    private final OutputFile[] outputFiles;

    private final InputFile stdin;

    private final OutputFile stdout;

    private final OutputFile stderr;

    public Job(String executable, String[] arguments,
            Map<String, String> environment, Map<String, String> attributes,
            InputFile[] inputFiles, OutputFile[] outputFiles, InputFile stdin,
            OutputFile stdout, OutputFile stderr) {
        this.executable = executable;
        this.arguments = arguments.clone();

        this.environment = new HashMap<String, String>(environment);
        this.attributes = new HashMap<String, String>(attributes);
        this.inputFiles = inputFiles.clone();
        this.outputFiles = outputFiles.clone();
        this.stdin = stdin;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    Job(ZoniInputStream in, File tmpDir) throws IOException {
        executable = in.readString();
        arguments = in.readStringArray();

        environment = in.readStringMap();
        attributes = in.readStringMap();

        inputFiles = new InputFile[in.readInt()];
        for (int i = 0; i < inputFiles.length; i++) {
            inputFiles[i] = new InputFile(in, tmpDir);
        }
        
        outputFiles = new OutputFile[in.readInt()];
        for (int i = 0; i < outputFiles.length; i++) {
            outputFiles[i] = new OutputFile(in);
        }
        
        stdin = new InputFile(in, tmpDir);
        
        stdout = new OutputFile(in);
        stderr = new OutputFile(in);
    }
    
    void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(executable);
        out.writeStringArray(arguments);
        
        out.writeStringMap(environment);
        out.writeStringMap(attributes);
        
        out.writeInt(inputFiles.length);
        for (InputFile file: inputFiles) {
            file.writeTo(out);
        }
        
        out.writeInt(outputFiles.length);
        for (OutputFile file: outputFiles) {
            file.writeTo(out);
        }
    }
        

}
