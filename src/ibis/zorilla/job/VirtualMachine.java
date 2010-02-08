package ibis.zorilla.job;

import java.io.File;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.xml.ws.Holder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.virtualbox_3_1.DeviceType;
import org.virtualbox_3_1.MachineState;
import org.virtualbox_3_1.SessionState;
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
import com.sun.xml.ws.commons.virtualbox_3_1.VirtualSystemDescriptionType;

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

    private final IWebsessionManager mgr;
    private final IVirtualBox vbox;
    private final String name;
    private final IMachine machine;
    private final IConsole console;
    private final ISession remoteSession;

    private final int sshPort;

    String getVmName(IVirtualSystemDescription description) throws Exception {

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
        mgr = new IWebsessionManager("http://localhost:18083/");
        vbox = mgr.logon("test", "test");

        IAppliance appliance = vbox.createAppliance();

        appliance.read(ovfFile.getAbsolutePath());
        appliance.interpret();

        List<IVirtualSystemDescription> descriptions = appliance
                .getVirtualSystemDescriptions();
        if (descriptions.isEmpty()) {
            throw new Exception("No virtual systems found in " + ovfFile);
        }

        // name = getVmName(descriptions.get(0));
        //
        // logger.info("name = " + name);
        //
        // IProgress progress = appliance.importMachines();
        //
        // while (!progress.getCompleted()) {
        // {
        // logger.info("Progress now " + progress.getPercent());
        //
        // try {
        // Thread.sleep(10000);
        // } catch (InterruptedException e) {
        // // IGNORE
        // }
        // }
        // }

        name = "23444_1";

        logger.info("importing done!");

        machine = vbox.findMachine(name);

        if (machine == null) {
            throw new Exception("could not find virtual machine " + name
                    + " after importing it");
        }

        ISession session = mgr.getSessionObject(vbox);
        vbox.openSession(session, machine.getId()); // machine is now locked
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

        mutable.saveSettings();
        session.close();

        // start VM

        remoteSession = mgr.getSessionObject(vbox);
        IProgress prog = vbox.openRemoteSession(remoteSession, machine.getId(),
                "vrdp", // session type
                ""); // possibly environment setting

        prog.waitForCompletion(10000); // give the process 10 secs
        if (prog.getResultCode() != 0) {
            throw new Exception("Session failed!");
        }

        // session will linger until vm is stopped
        // session.close();

        console = remoteSession.getConsole();

        logger.info("VM VRDP running on "
                + console.getRemoteDisplayInfo().getPort());

        while (console.getState() == MachineState.STARTING) {
            logger.info("Waiting for machine to finish starting...");

            Thread.sleep(1000);
        }

        logger.info("session state = " + session.getState());
    }

    // stop VM (in a rather abrupt manner to save time
    public void stop() {
        logger.info("stopping machine");

        // powerdown, wait until complete
        console.powerDown().waitForCompletion(10000);
        remoteSession.close();

        logger.info("state of session " + remoteSession.getRef() + " now "
                + remoteSession.getState());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // IGNORE
        }

        ISession session = mgr.getSessionObject(vbox);
        vbox.openSession(session, machine.getId()); // machine is now locked
        IMachine mutable = session.getMachine(); // obtain mutable machine

        List<IMediumAttachment> media = mutable.getMediumAttachments();

        for (IMediumAttachment medium : media) {
            if (medium != null && medium.getMedium() != null) {
                logger.info("removing " + medium.getMedium());
                machine.detachDevice(medium.getMedium().getName(), medium
                        .getPort(), medium.getDevice());
            }
        }

        mutable.saveSettings();
        session.close();

        for (IMediumAttachment medium : media) {
            if (medium.getMedium().getDeviceType() == DeviceType.HARD_DISK) {
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
