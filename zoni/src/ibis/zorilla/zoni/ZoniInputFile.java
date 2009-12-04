package ibis.zorilla.zoni;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ZoniInputFile implements Serializable {

    private static final long serialVersionUID = 1L;

    // location of this file in the sandbox
    private final String sandboxPath;

    // size of file
    private final long size;

    // file on local disk
    private final File file;

    /**
     * Input file described by an actual file on disk. This file is read by the
     * zorilla node after the job has been submitted. Alternatively, it can also
     * be streamed to the zorilla node by setting the "interactive" flag of the
     * job
     */
    public ZoniInputFile(String sandboxPath, File file) throws Exception {
        this.sandboxPath = sandboxPath;

        this.file = file.getAbsoluteFile();
        this.size = file.length();
    }

    /**
     * Constructor to receive input files.
     */
    ZoniInputFile(ObjectInputStream in, File tmpDir) throws IOException,
            ClassNotFoundException {
        sandboxPath = in.readUTF();
        size = in.readLong();
        File file = (File) in.readObject();
        boolean contentIncluded = in.readBoolean();

        // file send by value, not by reference
        if (contentIncluded) {
            file = File.createTempFile("input-file", null, tmpDir);
            FileOutputStream out = new FileOutputStream(file);

            long bytesLeft = size;
            byte[] buffer = new byte[1024];

            while (bytesLeft > 0) {

                int tryRead = (int) Math.min(bytesLeft, buffer.length);

                int read = in.read(buffer, 0, tryRead);

                if (read == -1) {
                    throw new IOException("EOF on reading input file");
                }

                out.write(buffer, 0, read);

                bytesLeft = bytesLeft - read;
            }

            out.close();
        }
        this.file = file;
    }

    /**
     * Write an input file to a stream
     */
    void writeTo(ObjectOutputStream out, boolean writeContent)
            throws IOException {
        out.writeUTF(sandboxPath);
        out.writeLong(size);
        out.writeObject(file);
        out.writeBoolean(writeContent);

        // file send by value, not send-by-reference
        if (writeContent) {
            InputStream in = new FileInputStream(file);
            try {

                long bytesLeft = size;
                byte[] buffer = new byte[1024];

                while (bytesLeft > 0) {
                    int tryRead = (int) Math.min(bytesLeft, buffer.length);

                    int read = in.read(buffer, 0, tryRead);

                    if (read == -1) {
                        throw new IOException("EOF on reading input file");
                    }

                    out.write(buffer, 0, read);

                    bytesLeft = bytesLeft - read;
                }
            } finally {
                in.close();
            }
        }
    }

    public File getFile() {
        return file;
    }

    public String getSandboxPath() {
        return sandboxPath;
    }

    public String toString() {
        return sandboxPath + " (" + file + ")";
    }

}