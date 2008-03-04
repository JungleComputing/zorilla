package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;

public class OutputFile {

    private final String sandboxPath;
    private final File file;

    /**
     * Output file which has to be retrieved
     * 
     * @param sandboxPath
     *            location of this file within the sandbox
     */
    public OutputFile(String sandboxPath) {
        this.sandboxPath = sandboxPath;
        this.file = null;
    }

    /**
     * Output file which is automatically copied to the given file
     * 
     * @param sandboxPath
     *            location of this file within the sandbox
     * @param file
     *            final location of the output file (can also be a directory)
     */
    public OutputFile(String sandboxPath, File file) {
        this.sandboxPath = sandboxPath;
        this.file = file.getAbsoluteFile();
    }

    OutputFile(ZoniInputStream in) throws IOException {
        sandboxPath = in.readString();
        file = in.readFile();
    }

    void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(sandboxPath);
        out.writeFile(file);
    }
    


}
