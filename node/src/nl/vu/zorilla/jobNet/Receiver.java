package nl.vu.zorilla.jobNet;

import java.io.IOException;

import nl.vu.zorilla.ZorillaException;
import ibis.ipl.ReadMessage;

public interface Receiver {
    
    public void receive(ReadMessage message);
    
    public void invoke(Invocation invocation) throws ZorillaException, IOException ;

}
