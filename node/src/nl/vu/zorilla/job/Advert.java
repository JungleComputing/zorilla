package nl.vu.zorilla.job;

import java.io.Serializable;
import java.util.UUID;


public abstract class Advert implements Serializable {

    public abstract UUID getJobID();

    public abstract String getJobImplementationType();
    
    public abstract String getMetric();
    public abstract int getCount();

}
