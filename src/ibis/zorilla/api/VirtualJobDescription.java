package ibis.zorilla.api;

import java.io.File;

/**
 * Native application, run in a Virtual Machine (like Xen, VMWare, or
 * VirtualBox)
 * 
 */
public class VirtualJobDescription extends NativeJobDescription {

    private static final long serialVersionUID = 1L;

    private File vmImage;
    
    public VirtualJobDescription() {
        super();
    }
    
    public synchronized File getVmImage() {
        return vmImage;
    }
    
    public synchronized void setVmImage(File vmImage) {
        this.vmImage = vmImage;
    }

    public String toString() {
        return "Virtual job \"" + getExecutable() + "\"";
    }

}
