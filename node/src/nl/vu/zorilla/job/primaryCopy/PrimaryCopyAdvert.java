package nl.vu.zorilla.job.primaryCopy;

import java.util.UUID;

import ibis.ipl.ReceivePortIdentifier;
import nl.vu.zorilla.job.Advert;

public class PrimaryCopyAdvert extends Advert {

    private static final long serialVersionUID = 1L;

    private final UUID jobID;

    private final String metric;
    private final int count;

    private final ReceivePortIdentifier primaryReceivePort;

    public PrimaryCopyAdvert(UUID jobID, String metric, int count,
            ReceivePortIdentifier primaryReceivePort) {

        this.jobID = jobID;
        this.metric = metric;
        this.count = count;
        this.primaryReceivePort = primaryReceivePort;
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
}
