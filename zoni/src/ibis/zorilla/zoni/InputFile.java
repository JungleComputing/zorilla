package ibis.zorilla.zoni;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputFile {
    // location of this file in the sandbox
    private final String path;

    // size of file
    private long size;

    // file on local disk
    private final File file;

    // input stream (send over connection when job submitted)
    private final InputStream inputStream;

    /**
     * Input file which is send over the connection when the job is submitted
     */
    public InputFile(String sandboxPath, InputStream inputStream, long size)
            throws Exception {
        this.path = sandboxPath;
        this.file = null;
        this.size = size;
        this.inputStream = inputStream;
    }

    /**
     * Constructor to receive input files.
     */
    InputFile(ZoniInputStream in, File tmpDir) throws IOException {
        path = in.readString();

        size = in.readLong();

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

        inputStream = null;
    }

    /**
     * Write an input file to a stream
     */
    void writeTo(ZoniOutputStream out) throws IOException {
        out.writeString(path);
        out.writeLong(size);

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

    public File getFile() {
        return file;
    }
}