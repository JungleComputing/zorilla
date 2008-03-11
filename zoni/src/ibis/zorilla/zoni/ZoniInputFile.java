package ibis.zorilla.zoni;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class ZoniInputFile {
    // location of this file in the sandbox
    private final String sandboxPath;

    // size of file
    private final long size;

    // file on local disk
    private final File file;

    // input stream (send over connection when job submitted)
    private final InputStream inputStream;

    /**
     * Input file offered as stream. It is send over to the zorilla node when
     * the job is submitten
     */
    public ZoniInputFile(String sandboxPath, InputStream inputStream, long size)
            throws Exception {
        this.sandboxPath = sandboxPath;
        this.file = null;
        this.size = size;
        this.inputStream = inputStream;
    }

    /**
     * Input file described by an actual file on disk. This file is read by the
     * zorilla node after the job has been submitted. Alternatively, it can also
     * be streamed to the zorilla node by setting the "interactive" flag of the
     * job
     */
    public ZoniInputFile(String sandboxPath, File file) throws Exception {
        this.sandboxPath = sandboxPath;

        this.file = file.getAbsoluteFile();
        this.inputStream = null;
        this.size = file.length();
    }

    /**
     * Constructor to receive input files.
     */
    ZoniInputFile(ZoniInputStream in, File tmpDir) throws IOException {
        sandboxPath = in.readString();
        size = in.readLong();
        File file = in.readFile();
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

        inputStream = null;
    }

    /**
     * Write an input file to a stream
     */
    void writeTo(ZoniOutputStream out, boolean writeContent) throws IOException {
        out.writeString(sandboxPath);
        out.writeLong(size);
        out.writeFile(file);
        out.writeBoolean(writeContent);

        // file send by value, not send-by-reference
        if (writeContent) {
            if (inputStream == null) {
                throw new IOException("cannot write content, "
                        + "no inputstream given");
            }

            long bytesLeft = size;
            byte[] buffer = new byte[1024];

            while (bytesLeft > 0) {
                int tryRead = (int) Math.min(bytesLeft, buffer.length);

                int read = inputStream.read(buffer, 0, tryRead);

                if (read == -1) {
                    throw new IOException("EOF on reading input file");
                }

                out.write(buffer, 0, read);

                bytesLeft = bytesLeft - read;
            }
        }
    }

    public File getFile() {
        return file;
    }

    public void closeStream() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    public String getSandboxPath() {
        return sandboxPath;
    }

}