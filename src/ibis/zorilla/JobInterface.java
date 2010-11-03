package ibis.zorilla;

import ibis.ipl.util.rpc.RemoteException;

import java.util.Map;

public interface JobInterface {

    public Map<String, String> getAttributes() throws RemoteException;

    public String getExecutable() throws RemoteException;

    public String getJobID() throws RemoteException;

    public int getPhase() throws RemoteException;

    public int getExitStatus() throws RemoteException;

    public Map<String, String> getStatus() throws RemoteException;

}
