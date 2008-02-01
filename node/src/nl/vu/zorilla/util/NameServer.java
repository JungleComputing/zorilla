package nl.vu.zorilla.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.gridlab.gat.util.OutputForwarder;


public final class NameServer {

    private final Process process;

    private final OutputForwarder outWriter;

    private final OutputForwarder errWriter;

    private final int port;

    private static final Logger logger = Logger.getLogger(NameServer.class);

    public NameServer(OutputStream outFile, int port) throws IOException,
            Exception {
        
        String[] nameServerCommand = nameServerCommand(port);

        if (logger.isDebugEnabled()) {
            String message = "running nameserver command:";
            for (int i = 0; i < nameServerCommand.length; i++) {
                message += " " + nameServerCommand[i];
            }
            logger.debug(message);
        }

        process = Runtime.getRuntime().exec(nameServerCommand);

        // no need for standard in in the nameserver
        process.getOutputStream().close();

        errWriter = new OutputForwarder(new BufferedInputStream(process.getErrorStream()), outFile);

        InputStream in = new BufferedInputStream(process.getInputStream());

        this.port = getPortFromStream(in, outFile);

        outWriter = new OutputForwarder(in, outFile);
    }

    // fetch the port the nameserver is running on;
    private static int getPortFromStream(InputStream in, OutputStream out)
            throws IOException, Exception {
        String match = "NameServer: created server on ServerSocket[addr=0.0.0.0/0.0.0.0,port=0,localport=";
        String current = "";
        // single byte buffer
        byte[] buffer = new byte[1];

        logger.debug("trying to find string: " + match);

        while (!current.equals(match)) {
            int read = in.read(buffer);

            if (read <= 0) {
                throw new Exception("could not find port number in nameserver output");
            }

            out.write(buffer);

            // append character
            current = current + new String(buffer);

            if (current.length() > match.length()) {
                current = current.substring(current.length() - match.length());
            }

        }

        logger.debug("match!");

        // the next characters should now be the port number...

        String portString = "";
        do {
            int read = in.read(buffer);

            if (read <= 0) {
                throw new Exception("could not find port number in nameserver output");
            }

            out.write(buffer);

            // append character
            portString = portString + new String(buffer);

        } while (!portString.endsWith("]"));

        // cut of the "]" at the end
        portString = portString.substring(0, portString.length() - 1);
        try {
            int result = Integer.parseInt(portString);
            logger.debug("result = " + result);
            return result;
        } catch (NumberFormatException e) {
            throw new Exception("could not parse part string", e);
        }
    }

    private static String[] nameServerCommand(int port) {

        String javaHome = System.getProperty("java.home");
        String classPath = System.getProperty("java.class.path");
        String fileSeparator = System.getProperty("file.separator");
        // String pathSeparator = System.getProperty("path.separator");
        
        // command as a list of Strings
        ArrayList<String> result = new ArrayList<String>();

        // java executable
        result.add(javaHome + fileSeparator + "bin" + fileSeparator + "java");

        // class path
        result.add("-cp");
        result.add(classPath);

        // specify the port, 0 for a unspecified (free) port
        result.add("-Dibis.name_server.port=" + Integer.toString(port));

        // main class and options
        result.add("ibis.impl.nameServer.tcp.NameServer");
        result.add("-single");
        result.add("-verbose");

        return result.toArray(new String[0]);
    }

    public int port() {
        return port;
    }

    public void kill() {
        logger.debug("killing nameserver");
        process.destroy();
        logger.debug("killed nameserver, waiting for writers");
        outWriter.waitUntilFinished();
        errWriter.waitUntilFinished();
        logger.debug("killed nameserver done");
    }
}
