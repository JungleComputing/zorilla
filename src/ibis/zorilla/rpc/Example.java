package ibis.zorilla.rpc;

import java.util.Date;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;

public class Example {

	public interface ExampleInterface {
		
		// Converts epoch time to date string.
		public String millisToString(long millis) throws RemoteException, Exception;
	}

	public class ExampleClass implements ExampleInterface {
		public String millisToString(long millis) throws RemoteException, Exception {
			return "rpc example result = " + new Date(millis).toString();
		}
	}

	/**
	 * Constructor. Actually does all the work too :)
	 */
	private Example(String[] arguments) throws Exception {
		int port = Integer.parseInt(arguments[0]);

		if (port == 0) { // server
			LocalSocketRPC rpc = new LocalSocketRPC(0);

			//create object we want to make remotely accessible
			ExampleClass object = new ExampleClass();

			
			//make object remotely accessible
			rpc.exportObject(
					ExampleInterface.class, object, "my great object");

			//wait for a bit
			Thread.sleep(10000);

			//cleanup, object no longer remotely accessible
			rpc.unexport("my great object");
			
			rpc.end();
		} else { //client
			ExampleInterface interfaceObject = LocalSocketRPC.createProxy(ExampleInterface.class, "my great object", port);
			
			//call remote object, print result
			System.err.println(interfaceObject.millisToString(System.currentTimeMillis()));

			
			
		}
		
		
	}


	public static void main(String args[]) {
		try {
			new Example(args);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
