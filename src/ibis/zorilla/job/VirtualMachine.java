package ibis.zorilla.job;

import java.io.File;

import com.sun.xml.ws.commons.virtualbox_3_1.IAppliance;
import com.sun.xml.ws.commons.virtualbox_3_1.IProgress;
import com.sun.xml.ws.commons.virtualbox_3_1.IVirtualBox;
import com.sun.xml.ws.commons.virtualbox_3_1.IWebsessionManager;

/**
 * Virtual machine running in a VirtualBox. We assume virtualbox is running as a
 * webservice on localhost
 * 
 * 
 */
public class VirtualMachine {

    public static final String serviceAddress = "http://localhost:18083/";
    
    private final IWebsessionManager mgr;
    private final IVirtualBox vbox;
    
    private final int port;
    
    VirtualMachine(File ovfFile) {
        //connect to virtualBox
        mgr = new IWebsessionManager("http://localhost:18083/");
        vbox = mgr.logon("test", "test");
        
        IAppliance appliance = vbox.createAppliance();
        
        appliance.read(ovfFile.getAbsolutePath());
        appliance.interpret();
        IProgress progress = appliance.importMachines();
        
        //wait until import is complete
        progress.waitForCompletion(0);

        
        port = 0;
    }
    
    
    void stop() {
        
        
    }
    

    /**
     * Port where ssh daemon is reachable on (NAT forwared)
     * @return
     */
    int getPort() {
        return port;
    }
    
    
    

}
