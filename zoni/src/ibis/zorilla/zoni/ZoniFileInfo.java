package ibis.zorilla.zoni;

import java.io.Serializable;

public class ZoniFileInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sandboxPath;

    private final String name;

    private final boolean isDirectory;

    private final ZoniFileInfo[] children;

    public ZoniFileInfo(String sandboxPath, String name, boolean isDirectory,
            ZoniFileInfo[] children) {
        this.sandboxPath = sandboxPath;
        this.name = name;
        this.isDirectory = isDirectory;
        this.children = children.clone();
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public String getName() {
        return name;
    }

    public String getSandboxPath() {
        return sandboxPath;
    }

    public ZoniFileInfo[] getChildren() {
        return children.clone();
    }

    public String toString() {
        return "FileInfo: sandboxPath = " + sandboxPath + ", name = " + name
                + ", isDirectory = " + isDirectory + ", children length = "
                + children.length;
    }

}
