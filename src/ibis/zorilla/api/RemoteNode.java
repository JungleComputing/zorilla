package ibis.zorilla.api;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;

import ibis.zorilla.api.rpc.SocketRPC;

/**
 * Remote interface to node. Connects using the RPC mechanism in Zorilla.
 * Returns "remote" objects for some functions.
 * 
 */
public class RemoteNode implements NodeInterface {
    
    public static final int DEFAULT_PORT=5666;

    private final NodeInterface proxy;
    private final int port;

    public RemoteNode(int port) throws RemoteException {
        this.port = port;

        proxy = SocketRPC.createProxy(NodeInterface.class, "zorilla node", port);

        // call a function too see if the node is reachable
        proxy.hasEnded();

    }

    @Override
    public void end() throws RemoteException {
        proxy.end();
    }

    @Override
    public void killNetwork() throws RemoteException, Exception {
        proxy.killNetwork();
    }

    @Override
    public boolean hasEnded() throws RemoteException {
        return proxy.hasEnded();
    }

    @Override
    public JobInterface[] getJobs() throws RemoteException {
        UUID[] uuids = proxy.getJobIDs();

        JobInterface[] result = new JobInterface[uuids.length];

        for (int i = 0; i < result.length; i++) {
            result[i] = SocketRPC.createProxy(JobInterface.class,
                    uuids[i].toString(), port);
        }

        return result;
    }

    @Override
    public JobInterface getJob(UUID jobID) throws RemoteException, Exception {
        // return a new proxy, to the job
        JobInterface result = SocketRPC.createProxy(JobInterface.class,
                jobID.toString(), port);

        // check if this job actually exists
        result.getPhase();

        return result;
    }

    @Override
    public UUID[] getJobIDs() throws RemoteException {
        return proxy.getJobIDs();
    }

    @Override
    public Map<String, String> getStats() throws RemoteException {
        return proxy.getStats();
    }

    @Override
	public UUID submitJob(ZorillaJobDescription jobDescription) throws RemoteException, Exception {
		return proxy.submitJob(jobDescription);
	}

}
