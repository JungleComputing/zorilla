package nl.vu.zorilla.bamboo;


public final class Neighbour {
    private Address info;
    private double latency;
    

    Neighbour(Address info, double latency) {
        this.info = info;
        this.latency = latency;
    }

    public Address address() {
        return info;
    }
    
    public double latency() {
        return latency;
    }
}
