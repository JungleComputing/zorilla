package ibis.zorilla.util;

import ibis.zorilla.Main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection to the stdin/stdout of a node running with the --remote option
 * 
 * @author Niels Drost
 * 
 */
public class Remote extends Thread {

    private static final Logger logger = LoggerFactory
            .getLogger(Remote.class);

    // standard out of server process. Turns into an input stream on this side
    private final BufferedReader stdout;

    // standard in of server process. We can write to it using an output stream
    private final OutputStream stdin;

    // stream to forward output from server to
    private final PrintStream output;

    private final String outputPrefix;

    private String address = null;

    private IOException exception = null;

    public Remote(InputStream stdout, OutputStream stdin, PrintStream output,
            String outputPrefix) {

        this.stdout = new BufferedReader(new InputStreamReader(stdout));
        this.stdin = stdin;
        this.output = output;

        this.outputPrefix = outputPrefix;

        // thread for parsing address
        setName("remote client");
        setDaemon(true);
        start();
    }

    /**
     * Returns the address of the server as a string.
     * 
     * @param timeout
     *            time to wait until address is available
     * 
     * @return the address of the server.
     * 
     * @throws IOException
     *             if server fails to start, or address is not available within
     *             the specified time.
     */
    public synchronized String getAddress(long timeout) throws IOException {
        long deadline = System.currentTimeMillis() + timeout;
        while (address == null) {
            if (exception != null) {
                throw exception;
            }

            long waitTime = deadline - System.currentTimeMillis();

            if (waitTime > 0) {
                try {
                    wait(waitTime);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            } else {
                throw new IOException("server did not produce address in time");
            }
        }

        return address;
    }

    /**
     * End Server by closing stream.
     */
    public void end() {
        try {
            stdin.close();
        } catch (IOException e) {
            // IGNORE
        }
    }

    private synchronized void setException(IOException exception) {
        this.exception = exception;
        notifyAll();
        end();
    }

    private synchronized void parseAddress(String line) {
        logger.debug("Parsing address from line: \"" + line + "\"");

        int prefixIndex = line.lastIndexOf(Main.ADDRESS_LINE_PREFIX);
        int postfixIndex = line.indexOf(Main.ADDRESS_LINE_POSTFIX);

        if (prefixIndex == -1 || postfixIndex == -1) {
            // address not in this line after all, print line to output
            logger.warn("Address prefix+postfix not found in line \"" + line
                    + "\"");
            output.println(line);
            return;
        }

        if ((prefixIndex + Main.ADDRESS_LINE_PREFIX.length()) >= postfixIndex) {
            logger.warn("Invalid address in line \"" + line + "\"");
        }

        try {
            this.address = line.substring(prefixIndex
                    + Main.ADDRESS_LINE_PREFIX.length(), postfixIndex);
        } catch (IndexOutOfBoundsException e) {
            logger.warn("Invalid address in line \"" + line + "\"");
            return;
        }

        notifyAll();
    }

    /**
     * Forwards standard out of server to given output stream. Filters out line
     * containing server address.
     */
    public void run() {
        String address = null;
        while (true) {
            try {
                String line = stdout.readLine();

                if (line == null) {
                    setException(new IOException("server terminated"));
                    return;
                }

                if (address == null
                        && line.contains(Main.ADDRESS_LINE_PREFIX)) {
                    parseAddress(line);
                } else {
                    output.println(outputPrefix + line);
                }
            } catch (IOException e) {
                setException(e);
                return;
            }
        }

    }

}
