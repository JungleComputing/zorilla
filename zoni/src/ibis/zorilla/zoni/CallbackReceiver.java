package ibis.zorilla.zoni;

import ibis.smartsockets.virtual.VirtualServerSocket;
import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator for callbacks. Creates a server socket, waits for incoming
 * connections from the Zorilla node, and passes the callback to the given
 * handler.S
 */
public class CallbackReceiver implements Runnable {

    private static final Logger logger = LoggerFactory
            .getLogger(CallbackReceiver.class);

    private final Callback callback;

    private final VirtualServerSocket serverSocket;

    public CallbackReceiver(Callback callback, VirtualSocketFactory factory)
            throws IOException {
        this.callback = callback;

        serverSocket = factory.createServerSocket(0, 50,
                new HashMap<String, Object>());

        newThread();

        logger.debug("created new callback receiver on " + serverSocket);
    }

    private void newThread() {
        Thread thread = new Thread(this);
        thread.setName("callback receiver");
        thread.setDaemon(true);
        thread.start();
    }

    public String getAddress() {
        return serverSocket.getLocalSocketAddress().toString();
    }

    public void close() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            // IGNORE
        }
    }

    @SuppressWarnings("unchecked")
    private void handleJobInfo(ObjectInputStream in, ObjectOutputStream out,
            VirtualSocket socket) throws IOException, ClassNotFoundException {
        String id = in.readUTF();
        String executable = in.readUTF();
        Map<String, String> attributes = (Map<String, String>) in.readObject();
        Map<String, String> jobStatus = (Map<String, String>) in.readObject();
        int phase = in.readInt(); // phase
        int exitStatus = in.readInt();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");

        // close socket so node doesn't wait for it
        out.flush();
        socket.close();

        JobInfo info = new JobInfo(id, executable, attributes, jobStatus,
                phase, exitStatus);

        logger.debug("doing callback, receive info = " + info);
        callback.callback(info);
    }

    public void run() {
        VirtualSocket socket = null;
        logger.debug("Accepting connection on " + serverSocket);
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

        newThread();

        if (socket == null) {
            return;
        }

        logger.debug("callback receiver received connection from "
                + socket.getRemoteSocketAddress());

        try {

            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));

            int protocolVersion = in.readInt();

            if (protocolVersion != ZoniProtocol.VERSION) {
                throw new IOException("wrong protocol version: "
                        + protocolVersion);
            }

            int opcode = in.readInt();

            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            if (opcode == ZoniProtocol.CALLBACK_JOBINFO) {
                handleJobInfo(in, out, socket);
            } else {
                throw new IOException("unknown opcode " + opcode
                        + " in callback");
            }
        } catch (Exception e) {
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
