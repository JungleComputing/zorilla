package ibis.zorilla.zoni;

import java.io.File;
import java.io.IOException;

public class ZoniOutputFile {

    private final String sandboxPath;

    private final File file;

    //variables used to retrieve output files from the zorilla node when the
    //job has finished
    
    private final boolean isDirectory;

    private final ZoniOutputFile[] children;

    /**
     * Output file which has to be retrieved.
     * 
     * @param sandboxPath
     *            location of this file within the sandbox
     * @param isDirectory
     *            is this file a directory.
     * 
     */
    public ZoniOutputFile(String sandboxPath) {
        this.sandboxPath = sandboxPath;
        
        file = null;
        isDirectory = false;
        children = new ZoniOutputFile[0];
    }

    /**
     * Output file which can be automatically copied to the given file
     * 
     * @param sandboxPath
     *            location of this file within the sandbox
     * @param file
     *            final location of the output file (can also be a directory)
     */
    public ZoniOutputFile(String sandboxPath, File file) {
        this.sandboxPath = sandboxPath;
        this.file = file.getAbsoluteFile();

        isDirectory = false;
        children = new ZoniOutputFile[0];
    }

    ZoniOutputFile(ZoniInputStream in) throws IOException {
        sandboxPath = in.readString();
        file = in.readFile();
        isDirectory = in.readBoolean();
        children = new ZoniOutputFile[in.readInt()];

        // recursive :)
        for (int i = 0; i < children.length; i++) {
            children[i] = new ZoniOutputFile(in);
        }
    }

    //format equal to zorilla.job.OutputFile
    void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(sandboxPath);
        out.writeFile(file);
        out.writeBoolean(isDirectory);

        out.writeInt(children.length);
        // recursive :)
        for (ZoniOutputFile file : children) {
            file.writeTo(out);
        }
    }

    public boolean isDirectory() {
        return isDirectory;
    }
    
    public File getFile() {
        return file;
    }
    
    public String getSandboxPath() {
        return sandboxPath;
    }

    public ZoniOutputFile[] getChildren() {
        return children.clone();
    }

}
