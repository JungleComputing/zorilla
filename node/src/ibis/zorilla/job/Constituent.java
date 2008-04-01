package ibis.zorilla.job;

import ibis.ipl.ReceivePortIdentifier;
import ibis.zorilla.NodeInfo;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

class Constituent implements Serializable {

    private static final long serialVersionUID = -3561973367471333047L;

    public static final long EXPIRATION_TIMEOUT = 1200 * 1000;

    private final UUID id;

    private final ReceivePortIdentifier receivePort;
    
    private final NodeInfo info;

    private Set<UUID> workers;

    private long expirationDate;

    private int maxNrOfWorkers = 0;

    Constituent(UUID id, ReceivePortIdentifier receivePort, NodeInfo info) {
        this.id = id;
        this.receivePort = receivePort;
        this.info = info;

        workers = new HashSet<UUID>();

        resetExpirationDate();
    }

    synchronized boolean expired() {
        return System.currentTimeMillis() > expirationDate;
    }

    synchronized void resetExpirationDate() {
        expirationDate = System.currentTimeMillis() + EXPIRATION_TIMEOUT;
    }

    UUID getID() {
        return id;
    }

    ReceivePortIdentifier getReceivePort() {
        return receivePort;
    }

    synchronized void addWorker(UUID id) {
        workers.add(id);
    }

    synchronized void removeWorker(UUID id) {
        workers.remove(id);
    }

    synchronized void setWorkers(UUID[] workers) {
        this.workers = new HashSet<UUID>();
        for (UUID worker : workers) {
            this.workers.add(worker);
        }
    }

    synchronized int nrOfWorkers() {
        return workers.size();
    }

    synchronized int getMaxNrOfWorkers() {
        return maxNrOfWorkers;
    }

    synchronized void setMaxNrOfWorkers(int maxNrOfWorkers) {
        this.maxNrOfWorkers = maxNrOfWorkers;
    }

    public String toString() {
        return id.toString().substring(0, 8);
    }

	public NodeInfo getInfo() {
		return info;
	}
}