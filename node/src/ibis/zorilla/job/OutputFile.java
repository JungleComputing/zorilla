package ibis.zorilla.job;

import ibis.zorilla.zoni.ZoniOutputStream;

import java.io.IOException;

public class OutputFile {
    
    private final String sandboxPath;
    
    private final boolean isDirectory;
    
    private final OutputFile[] children;

    public OutputFile(String sandboxPath, boolean isDirectory, OutputFile[] children) {
        super();
        this.sandboxPath = sandboxPath;
        this.isDirectory = isDirectory;
        this.children = children;
    }

    public OutputFile[] getChildren() {
        return children;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public String getSandboxPath() {
        return sandboxPath;
    }
    
    //  format equal to zorilla.job.OutputFile
    public void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(sandboxPath);
        out.writeFile(null);
        out.writeBoolean(isDirectory);

        out.writeInt(children.length);
        // recursive :)
        for (OutputFile file : children) {
            file.writeTo(out);
        }
    }
}
