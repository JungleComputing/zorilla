package ibis.zorilla.api;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;

public interface NodeInterface {

    public void end() throws RemoteException;

    public void killNetwork() throws RemoteException, Exception;

    public boolean hasEnded() throws RemoteException;

    public JobInterface[] getJobs() throws RemoteException;
    
    public UUID[] getJobIDs() throws RemoteException;

    public JobInterface getJob(UUID jobID) throws RemoteException, Exception;
    
    public Map<String, String> getStats() throws RemoteException;
}
