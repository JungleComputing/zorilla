package ibis.zorilla;

import ibis.zorilla.rpc.RemoteException;

public interface NodeInterface {
    
    public void end() throws RemoteException;
    
    public void killNetwork() throws RemoteException, Exception;
    
    

}
