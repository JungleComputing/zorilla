package ibis.zorilla.zoni;

import ibis.util.IPUtils;
import ibis.util.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Generator for callbacks. Creates a serversocket, waits for incoming
 * connections from the Zorilla node, and passes the callback to the given
 * handler.S
 */
public class CallbackReceiver extends Thread {

    private static final Logger logger =
        Logger.getLogger(CallbackReceiver.class);

    private final Callback callback;

    private final ServerSocket serverSocket;

    public CallbackReceiver(Callback callback) throws IOException {
        this.callback = callback;

        serverSocket = new ServerSocket(0);

        ThreadPool.createNew(this, "callback generator");
        
        logger.debug("created new callback receiver on " + serverSocket);
    }

    public void close() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            // IGNORE
        }
    }

    InetSocketAddress[] addresses() throws IOException {
        int port = serverSocket.getLocalPort();

        InetAddress[] addresses = IPUtils.getLocalHostAddresses();

        InetSocketAddress[] result = new InetSocketAddress[addresses.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new InetSocketAddress(addresses[i], port);
        }

        return result;
    }

    private void handleJobInfo(ZoniInputStream in, ZoniOutputStream out,
            Socket socket) throws IOException {
        String id = in.readString();
        String executable = in.readString();
        Map<String, String> attributes = in.readStringMap();
        Map<String, String> jobStatus = in.readStringMap();
        int phase = in.readInt(); // phase
        int exitStatus = in.readInt();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");

        // close socket so node doesn't wait for it
        out.flush();
        socket.close();

        JobInfo info =
            new JobInfo(id, executable, attributes, jobStatus, phase, exitStatus);

        logger.debug("doing callback, receive info = " + info);
        callback.callback(info);
    }

    public void run() {
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (serverSocket.isClosed()) {
                    return;
                }
                logger.error(
                    "Zorilla: error on accepting socket in callback generator",
                    e);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // IGNORE
                }
            }

            ThreadPool.createNew(this, "callback generator");

            if (socket == null) {
                return;
            }
            
            logger.debug("callback receiver received connection from " + socket.getRemoteSocketAddress());

            try {

                ZoniInputStream in =
                    new ZoniInputStream(new BufferedInputStream(
                            socket.getInputStream()));
                ZoniOutputStream out =
                    new ZoniOutputStream(new BufferedOutputStream(
                            socket.getOutputStream()));

                int protocolVersion = in.readInt();
                
                if (protocolVersion != ZoniProtocol.VERSION) {
                    throw new IOException("wrong protocol version: " + protocolVersion);
                }
                
                int opcode = in.readInt();

                if (opcode == ZoniProtocol.CALLBACK_JOBINFO) {
                    handleJobInfo(in, out, socket);
                } else {
                    throw new IOException("unknown opcode " + opcode
                            + " in callback");
                }
            } catch (IOException e) {
                logger.error("Zorilla: error on handling callback", e);
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (Exception e) {
                        // IGNORE
                    }

                }
            }

        }

    }

}