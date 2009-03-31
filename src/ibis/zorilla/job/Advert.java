package ibis.zorilla.job;

import java.io.Serializable;
import java.util.UUID;

public abstract class Advert implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract UUID getJobID();

    public abstract String getJobImplementationType();

    public abstract String getMetric();

    public abstract int getCount();

    public String toString() {
        return getJobID().toString() + " " + getMetric() + " " + getCount();
    }

}
