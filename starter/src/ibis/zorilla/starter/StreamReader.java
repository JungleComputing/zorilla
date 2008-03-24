package ibis.zorilla.starter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamReader extends Thread {

    private final InputStream in;

    private final OutputStream out;

    StreamReader(InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;
    }

    public void run() {
        byte[] buffer = new byte[1024];

        while (true) {
            try {
                int read = in.read(buffer);

                if (read == -1) {
                    // done
                    return;
                }

                if (out != null) {
                    out.write(buffer, 0, read);
                }
            } catch (IOException e) {
                // IGNORE
                return;
            }
        }
    }
}
