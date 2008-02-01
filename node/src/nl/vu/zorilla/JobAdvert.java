package nl.vu.zorilla;

import java.io.Serializable;
import java.util.UUID;


public abstract class JobAdvert implements Serializable {

    public abstract UUID getJobID();

    public abstract String getJobImplementationType();
    
    public abstract String getMetric();
    public abstract int getCount();

}
