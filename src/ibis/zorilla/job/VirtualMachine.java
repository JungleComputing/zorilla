package ibis.zorilla.job;

import java.io.File;
import java.net.ServerSocket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.virtualbox_3_1.DeviceType;
import org.virtualbox_3_1.MachineState;
import org.virtualbox_3_1.VirtualSystemDescriptionValueType;

import com.sun.xml.ws.commons.virtualbox_3_1.IAppliance;
import com.sun.xml.ws.commons.virtualbox_3_1.IConsole;
import com.sun.xml.ws.commons.virtualbox_3_1.IMachine;
import com.sun.xml.ws.commons.virtualbox_3_1.IMediumAttachment;
import com.sun.xml.ws.commons.virtualbox_3_1.IProgress;
import com.sun.xml.ws.commons.virtualbox_3_1.ISession;
import com.sun.xml.ws.commons.virtualbox_3_1.IVirtualBox;
import com.sun.xml.ws.commons.virtualbox_3_1.IVirtualSystemDescription;
import com.sun.xml.ws.commons.virtualbox_3_1.IWebsessionManager;

/**
 * Virtual machine running in a VirtualBox. We assume virtualbox is running as a
 * webservice on localhost
 * 
 * 
 */
public class VirtualMachine {

    public static final Logger logger = LoggerFactory
            .getLogger(VirtualMachine.class);

    public static final String serviceAddress = "http://localhost:18083/";

    private final String id;

    private final int sshPort;

    public static boolean vboxIsAvailable() {
        try {
            IWebsessionManager mgr = new IWebsessionManager(
                    "http://localhost:18083/" );
            IVirtualBox vbox = mgr.logon("test", "test");
            mgr.logoff(vbox);

            // success!
            logger.info("VirtualBox web service available");
            return true;
        } catch (Throwable t) {
            //IGNORE
        }
        logger.info("VirtualBox web service not available");
        return false;
    }

    static String getVmName(IVirtualSystemDescription description)
            throws Exception {

        List<String> names = description.getValuesByType(
                org.virtualbox_3_1.VirtualSystemDescriptionType.NAME,
                VirtualSystemDescriptionValueType.AUTO);

        if (names.isEmpty()) {
            throw new Exception("no name found for VM");
        }

        if (names.size() > 1) {
            throw new Exception("multiple names found for single VM");
        }

        return names.get(0);

    }

    public VirtualMachine(File ovfFile, File sandbox) throws Exception {
        // connect to virtualBox
        IWebsessionManager mgr = new IWebsessionManager(
                "http://localhost:18083/");
        IVirtualBox vbox = mgr.logon("test", "test");

        IAppliance appliance = vbox.createAppliance();

        appliance.read(ovfFile.getAbsolutePath());
        appliance.interpret();

        List<IVirtualSystemDescription> descriptions = appliance
                .getVirtualSystemDescriptions();
        if (descriptions.isEmpty()) {
            throw new Exception("No virtual systems found in " + ovfFile);
        }

        String name = getVmName(descriptions.get(0));

        logger.info("name = " + name);

        IProgress progress = appliance.importMachines();

        while (!progress.getCompleted()) {
            {
                logger.info("Progress now " + progress.getPercent());

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            }
        }

        logger.info("importing done!");

        IMachine machine = vbox.findMachine(name);

        if (machine == null) {
            throw new Exception("could not find virtual machine " + name
                    + " after importing it");
        }

        id = machine.getId();

        ISession session = mgr.getSessionObject(vbox);
        vbox.openSession(session, id); // machine is now locked
        IMachine mutable = session.getMachine(); // obtain mutable machine

        // link sandbox to VirtualMachine
        // mutable.createSharedFolder("zorilla", sandbox.getAbsolutePath(),
        // true);

        // fetch/reserve ephemeral port.
        ServerSocket socket = new ServerSocket(0);
        sshPort = socket.getLocalPort();
        socket.close();

        // set NAT forwarding for ssh
        mutable.setExtraData(
                "VBoxInternal/Devices/pcnet/0/LUN#0/Config/guestssh/Protocol",
                "TCP");
        mutable.setExtraData(
                "VBoxInternal/Devices/pcnet/0/LUN#0/Config/guestssh/GuestPort",
                "22");
        mutable.setExtraData(
                "VBoxInternal/Devices/pcnet/0/LUN#0/Config/guestssh/HostPort",
                "" + sshPort);

        mutable.setMemorySize(512L);

        mutable.saveSettings();
        session.close();

        // start VM

        ISession remoteSession = mgr.getSessionObject(vbox);
        IProgress prog = vbox.openRemoteSession(remoteSession, id, "vrdp", // session
                // type
                ""); // possibly environment setting

        prog.waitForCompletion(10000); // give the process 10 secs
        if (prog.getResultCode() != 0) {
            throw new Exception("Session failed!");
        }

        // session will linger until vm is stopped
        // session.close();

        IConsole console = remoteSession.getConsole();

        logger.info("VM VRDP running on "
                + console.getRemoteDisplayInfo().getPort());

        while (console.getState() == MachineState.STARTING) {
            logger.info("Waiting for machine to finish starting...");

            Thread.sleep(1000);
        }

        remoteSession.close();
        mgr.logoff(vbox);

    }

    // stop VM (in a rather abrupt manner to save time
    public void stop() {
        logger.info("stopping machine");

        IWebsessionManager mgr = new IWebsessionManager(
                "http://localhost:18083/");
        IVirtualBox vbox = mgr.logon("test", "test");

        ISession session = mgr.getSessionObject(vbox);
        IMachine machine = vbox.getMachine(id);

        vbox.openExistingSession(session, id);

        IConsole console = session.getConsole();

        // powerdown, wait until complete
        console.powerDown().waitForCompletion(10000);
        session.close();

        logger.info("state of session " + session.getRef() + " now "
                + session.getState());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // IGNORE
        }

        session = mgr.getSessionObject(vbox);
        vbox.openSession(session, machine.getId()); // machine is now locked
        IMachine mutable = session.getMachine(); // obtain mutable machine

        List<IMediumAttachment> media = mutable.getMediumAttachments();

        logger.info("media size = " + media);

        for (IMediumAttachment medium : media) {
            if (medium != null) {
                logger.info("removing " + medium);
                mutable.detachDevice(medium.getController(), medium.getPort(),
                        medium.getDevice());
            }
        }

        mutable.saveSettings();
        session.close();

        for (IMediumAttachment medium : media) {
            if (medium != null
                    && medium.getMedium() != null
                    && medium.getMedium().getDeviceType() == DeviceType.HARD_DISK) {
                logger.info("deleting " + medium.getMedium().getName());
                medium.getMedium().deleteStorage();

            }
        }

        vbox.unregisterMachine(machine.getId());
        machine.deleteSettings();

        mgr.logoff(vbox);
    }

    /**
     * Port where ssh daemon is reachable on (NAT forwared)
     * 
     * @return
     */
    public int getSshPort() {
        return sshPort;
    }

}
