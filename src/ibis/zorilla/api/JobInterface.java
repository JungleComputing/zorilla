package ibis.zorilla.api;

import java.rmi.RemoteException;
import java.util.UUID;

public interface JobInterface {

    public UUID getID() throws RemoteException;

    public JobPhase getPhase() throws RemoteException;

    public int getExitStatus() throws RemoteException;

    public void cancel() throws RemoteException, Exception;
    
    public ZorillaJobDescription getDescription() throws RemoteException;
    
}
