package ibis.zorilla.job;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.ws.Holder;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.virtualbox_3_1.VirtualSystemDescriptionValueType;


import com.sun.xml.ws.commons.virtualbox_3_1.IAppliance;
import com.sun.xml.ws.commons.virtualbox_3_1.IProgress;
import com.sun.xml.ws.commons.virtualbox_3_1.IVirtualBox;
import com.sun.xml.ws.commons.virtualbox_3_1.IVirtualSystemDescription;
import com.sun.xml.ws.commons.virtualbox_3_1.IWebsessionManager;
import com.sun.xml.ws.commons.virtualbox_3_1.VirtualSystemDescriptionType;

/**
 * Virtual machine running in a VirtualBox. We assume virtualbox is running as a
 * webservice on localhost
 * 
 * 
 */
public class VirtualMachine {
	
	public static final Logger logger = LoggerFactory.getLogger(VirtualMachine.class);

    public static final String serviceAddress = "http://localhost:18083/";
    
    private final IWebsessionManager mgr;
    private final IVirtualBox vbox;
    
    private final int port;
    
    
    String getVmName(IVirtualSystemDescription description) throws Exception {

    	List<String> names = description.getValuesByType(org.virtualbox_3_1.VirtualSystemDescriptionType.NAME, VirtualSystemDescriptionValueType.AUTO);
    	
    	if (names.isEmpty()) {
    		throw new Exception("no name found for VM");
    	}
    	
    	if (names.size() > 1) {
    		throw new Exception("multiple names found for single VM");
    	}
    	
    	return names.get(0);
    	
    }
    
    public VirtualMachine(File ovfFile) throws Exception {
        //connect to virtualBox
        mgr = new IWebsessionManager("http://localhost:18083/");
        vbox = mgr.logon("test", "test");
        
        IAppliance appliance = vbox.createAppliance();
        
        appliance.read(ovfFile.getAbsolutePath());
        appliance.interpret();
        
        List<IVirtualSystemDescription> descriptions = appliance.getVirtualSystemDescriptions();
        if (descriptions.isEmpty()) {
        	throw new Exception("No virtual systems found in " + ovfFile);
        }
        
        String name = getVmName(descriptions.get(0));
        
        logger.info("name = " + name);
        
        IProgress progress = appliance.importMachines();
        
        for(int i = 0; i < 10; i++) 
        {
        logger.info("Progress now " + progress.getPercent());
        
        try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        }
        
        //wait until import is complete
        
        //FIXME: doesn't wait :-(
        progress.waitForCompletion(0);

        logger.info("done!");
        
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
