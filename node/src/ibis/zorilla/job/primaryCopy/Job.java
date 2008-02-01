package ibis.zorilla.job.primaryCopy;

import ibis.ipl.IbisIdentifier;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Receiver;

import java.io.File;
import java.io.IOException;


public abstract class Job extends ibis.zorilla.job.Job {

    final static String fileName(File file) throws IOException {
        String[] pathElements = file.getPath().split("/");

        if (pathElements.length == 0) {
            throw new IOException("could not find filename in given file"
                    + " path: " + file.getPath());
        }

        return pathElements[pathElements.length - 1];
    }

    public final String toString() {
        return getID().toString().substring(0, 7);
    }

    abstract void log(String message);

    abstract void log(String message, Exception exception);

    abstract EndPoint newEndPoint(String name, Receiver receiver)
            throws IOException, Exception;

    abstract IbisIdentifier getRandomConstituent();

}
