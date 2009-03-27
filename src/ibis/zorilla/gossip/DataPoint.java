package ibis.zorilla.gossip;

public final class DataPoint {
	private double time;
	private double pns;
	private double exchangeCount;
	
	DataPoint(double time, double pns, double exchangeCount) {
		this.time = time;
		this.pns = pns;
		this.exchangeCount = exchangeCount;
	}
	
	public double getExchangeCount() {
		return exchangeCount;
	}
	public double getPNS() {
		return pns;
	}
	public double getTime() {
		return time;
	}
        
        public String toString() {
            return time + " " + pns + " " + exchangeCount; 
        }

	
}
