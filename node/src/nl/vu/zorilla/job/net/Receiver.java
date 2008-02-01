package nl.vu.zorilla.job.net;

import java.io.IOException;

import ibis.ipl.ReadMessage;

public interface Receiver {
    
    public void receive(ReadMessage message);
    
    public void invoke(Invocation invocation) throws Exception, IOException ;

}
