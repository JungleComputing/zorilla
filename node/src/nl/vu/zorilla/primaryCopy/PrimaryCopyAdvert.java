package nl.vu.zorilla.primaryCopy;

import java.util.UUID;

import ibis.ipl.ReceivePortIdentifier;
import nl.vu.zorilla.JobAdvert;

public class PrimaryCopyAdvert extends JobAdvert {

    private static final long serialVersionUID = 1L;

    private final UUID jobID;

    private final String metric;
    private final int count;

    private final ReceivePortIdentifier primaryReceivePort;

    private final int nameserverPort;

    private String nameserverHost;

    public PrimaryCopyAdvert(UUID jobID, String metric, int count,
            ReceivePortIdentifier primaryReceivePort, int nameserverPort,
            String nameserverHost) {

        this.jobID = jobID;
        this.metric = metric;
        this.count = count;
        this.primaryReceivePort = primaryReceivePort;
        this.nameserverPort = nameserverPort;
        this.nameserverHost = nameserverHost;
    }

    @Override
    public UUID getJobID() {
        return jobID;
    }

    @Override
    public String getJobImplementationType() {
        return "primaryCopy";
    }

    @Override
    public String getMetric() {
        return metric;
    }
    
    @Override
    public int getCount() {
        return count;
    }

    public ReceivePortIdentifier getPrimaryReceivePort() {
        return primaryReceivePort;
    }

    public int getNameserverPort() {
        return nameserverPort;
    }

    public String getNameserverHost() {
        return nameserverHost;
    }

}
