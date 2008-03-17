package ibis.zorilla.net;

import ibis.util.ThreadPool;
import ibis.zorilla.job.Callback;
import ibis.zorilla.job.Job;
import ibis.zorilla.zoni.ZoniInputStream;
import ibis.zorilla.zoni.ZoniOutputStream;
import ibis.zorilla.zoni.ZoniProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.log4j.Logger;

public class ZoniCallback implements Runnable, Callback {

    private final Logger logger = Logger.getLogger(ZoniCallback.class);

    InetSocketAddress[] addresses;

    int lastPhase;

    Job job;

    public ZoniCallback(InetSocketAddress[] addresses) {
        this.addresses = addresses;

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

        Socket socket = null;

        for (int i = 0; i < addresses.length && socket == null; i++) {
            try {
                socket =
                    new Socket(addresses[i].getAddress(),
                            addresses[i].getPort());
            } catch (IOException e) {
                logger.debug("could not connect to client at " + addresses[i],
                    e);
            }
        }

        if (socket == null) {
            logger.warn("could not connect to client for callback");
            return;
        }

        try {

            ZoniInputStream in =
                new ZoniInputStream(new BufferedInputStream(
                        socket.getInputStream()));
            ZoniOutputStream out =
                new ZoniOutputStream(new BufferedOutputStream(
                        socket.getOutputStream()));

            out.writeInt(ZoniProtocol.VERSION);
            out.writeInt(ZoniProtocol.CALLBACK_JOBINFO);
            out.writeString(job.getID().toString());
            out.writeString(job.getDescription().getExecutable().toString());
            out.writeStringMap(job.getAttributes());
            out.writeStringMap(job.getStats());
            out.writeInt(phase);
            out.writeInt(job.getExitStatus());
            out.flush();

            int status = in.readInt();
            String message = in.readString();

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
