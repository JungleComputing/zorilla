package ibis.zorilla;

import ibis.ipl.util.rpc.RemoteException;

public interface NodeInterface {
    
    public void end() throws RemoteException;
    
    public void killNetwork() throws RemoteException, Exception;
    
    

}
