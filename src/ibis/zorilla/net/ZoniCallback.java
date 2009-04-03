package ibis.zorilla.net;

import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketAddress;
import ibis.util.ThreadPool;
import ibis.zorilla.Node;
import ibis.zorilla.job.Callback;
import ibis.zorilla.job.Job;
import ibis.zorilla.zoni.ZoniProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

public class ZoniCallback implements Runnable, Callback {
    
    private static final int CONNECT_TIMEOUT = 10000; 

    private static final Logger logger = Logger.getLogger(ZoniCallback.class);

    private final Node node;

    private final VirtualSocketAddress address;

    private int lastPhase;

    private Job job;

    public ZoniCallback(String address, Node node) throws Exception {
        this.address = new VirtualSocketAddress(address);
        this.node = node;

        job = null;
        lastPhase = Job.INITIAL;
    }

    public synchronized void setJob(Job job) {
        this.job = job;
    }

    // initiate sending an update (if needed)
    public void callback() {
        ThreadPool.createNew(this, "zoni callback");
    }

    public synchronized void run() {
        if (job == null) {
            // no update possible (yet)
            return;
        }

        int phase = job.getPhase();

        if (phase == lastPhase) {
            // no update needed
            return;
        }

        VirtualSocket socket = null;
        try {

            socket = node.network().getSocketFactory().createClientSocket(
                    address, CONNECT_TIMEOUT, false, null);

            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            out.writeInt(ZoniProtocol.VERSION);
            out.writeInt(ZoniProtocol.CALLBACK_JOBINFO);
            out.writeUTF(job.getID().toString());
            out.writeUTF(job.getDescription().getExecutable().toString());
            out.writeObject(job.getAttributes().getStringMap());
            out.writeObject(job.getStats());
            out.writeInt(phase);
            out.writeInt(job.getExitStatus());
            out.flush();

            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));

            int status = in.readInt();
            String message = in.readUTF();

            if (status != ZoniProtocol.STATUS_OK) {
                throw new IOException("error on doing callback: " + message);
            }

            lastPhase = phase;

        } catch (Exception e) {
            logger.warn("error on sending callback to client", e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // IGNORE
                }
            }
        }
    }
}
