package ibis.zorilla.job;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;
import java.util.UUID;

public class Advert implements Serializable {

    private static final long serialVersionUID = 1L;
	protected final UUID jobID;
	protected final String metric;
	protected final int count;
	protected final ReceivePortIdentifier primaryReceivePort;
	
	 public Advert(UUID jobID, String metric, int count,
	            ReceivePortIdentifier primaryReceivePort) {

	        this.jobID = jobID;
	        this.metric = metric;
	        this.count = count;
	        this.primaryReceivePort = primaryReceivePort;
	    }

	public String toString() {
        return getJobID().toString() + " " + getMetric() + " " + getCount();
    }

	public UUID getJobID() {
	    return jobID;
	}

	public String getMetric() {
	    return metric;
	}

	public int getCount() {
	    return count;
	}

	public ReceivePortIdentifier getPrimaryReceivePort() {
	    return primaryReceivePort;
	}

}
