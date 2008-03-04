package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;

public class OutputFile {

    private final String sandboxPath;

    private final File file;

    private final boolean exists;

    private final boolean isDirectory;

    private final OutputFile[] children;

    /**
     * Output file which has to be retrieved.
     * 
     * @param sandboxPath
     *            location of this file within the sandbox
     * @param isDirectory
     *            is this file a directory.
     * 
     */
    public OutputFile(String sandboxPath, boolean isDirectory) {
        this.sandboxPath = sandboxPath;
        this.isDirectory = isDirectory;

        file = null;
        exists = false;
        children = new OutputFile[0];
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
        this.isDirectory = file.isDirectory();
        this.file = file.getAbsoluteFile();

        exists = false;
        children = new OutputFile[0];
    }

    OutputFile(ZoniInputStream in) throws IOException {
        sandboxPath = in.readString();
        file = in.readFile();
        exists = in.readBoolean();
        isDirectory = in.readBoolean();
        children = new OutputFile[in.readInt()];

        // recursive :)
        for (int i = 0; i < children.length; i++) {
            children[i] = new OutputFile(in);
        }
    }

    void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(sandboxPath);
        out.writeFile(file);
        out.writeBoolean(exists);
        out.writeBoolean(isDirectory);

        out.writeInt(children.length);
        // recursive :)
        for (OutputFile file : children) {
            file.writeTo(out);
        }
    }

    public boolean exists() {
        return exists;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

}
