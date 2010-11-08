package ibis.zorilla.rpc;

import ibis.util.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSocketRPC extends Thread {

	private static final Logger logger = LoggerFactory
			.getLogger(LocalSocketRPC.class);

	private final ServerSocket serverSocket;

	private final Map<String, RemoteObject> objects;

	// initializes RPC which only works on the loopback address
	public LocalSocketRPC(int port) throws IOException {
		// InetAddress loopback = InetAddress.getByName("127.0.0.1");
		InetAddress loopback = InetAddress.getByName(null);

		this.serverSocket = new ServerSocket(port, 0, loopback);

		this.objects = new HashMap<String, RemoteObject>();

		ThreadPool.createNew(this, "rpc connection handler");

		logger.debug("RPC created on port " + getPort());
	}

	public void end() {
		try {
			logger.debug("stopping rpc");
			serverSocket.close();
		} catch (IOException e) {
			logger.warn("could not end RPC properly", e);
		}
	}

	public int getPort() {
		return serverSocket.getLocalPort();
	}

	/**
	 * Exports an object, making it remotely accessible.
	 * 
	 * 
	 * @param <InterfaceType>
	 *            Type of Interface which defines all the remotely accessable
	 *            functions.
	 * @param interfaceClass
	 *            Interface which defines all the remotely accessable functions.
	 *            All functions in this interface must declare to throw a
	 *            {@link RemoteException}.
	 * @param theObject
	 *            the object to be remotely accessible. Must implement the
	 *            interface given
	 * @param name
	 *            the name of the remote object. Must be unique.
	 * @return the RemoteObject
	 * @throws IOException
	 *             if creating the receive port failed
	 * @throws RemoteException
	 *             if the given interface does not meet the requirements.
	 */
	public <InterfaceType extends Object> void exportObject(
			Class<InterfaceType> interfaceClass, InterfaceType theObject,
			String name) throws IOException, RemoteException {
		RemoteObject remote = new RemoteObject(interfaceClass, theObject, name);

		synchronized (this) {
			if (objects.containsKey(name)) {
				throw new RemoteException("Remote object with name \"" + name
						+ "\" already exists");
			}
			objects.put(name, remote);
		}
	}

	/**
	 * Creates a proxy to the remote object specified. Hides all communication,
	 * presenting the user with the given interface. All calls to methods in the
	 * interface will be forwarded to the remote object. In case of a
	 * communication error, a {@link RemoteException} will be thrown.
	 * 
	 * 
	 * @param <InterfaceType>
	 *            Type of Interface which defines all the remotely accessible
	 *            functions.
	 * @param interfaceClass
	 *            Interface which defines all the remotely accessible functions.
	 *            All functions in this interface must declare to throw a
	 *            {@link RemoteException}.
	 * @param address
	 *            Address of the Ibis of the remote object
	 * @param name
	 *            Name of the (receiveport of the) remote object.
	 * @return a proxy to the remote object.
	 * @throws IOException
	 *             if creating the receive port failed
	 * @throws RemoteException
	 *             if the given interface does not meet the requirements.
	 */
	@SuppressWarnings("unchecked")
	public static <InterfaceType extends Object> InterfaceType createProxy(
			Class<InterfaceType> interfaceClass, String name, int port) {

		RPCInvocationHandler handler = new RPCInvocationHandler(name, port);

		InterfaceType result = (InterfaceType) Proxy.newProxyInstance(
				interfaceClass.getClassLoader(),
				new Class[] { interfaceClass }, handler);

		return result;
	}

	private synchronized RemoteObject getObject(String name) {
		return objects.get(name);
	}

	public void run() {

		Socket socket = null;

		try {
			socket = serverSocket.accept();
		} catch (Exception e) {
			if (!serverSocket.isClosed()) {
				logger.error("caught exception while handling connection", e);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					// IGNORE
				}
			}
		}

		// stop handling connections
		if (socket == null && serverSocket.isClosed()) {
			logger.debug("server socket closed, stopping rpc");
			return;
		}

		// create a new thread for the next connection
		ThreadPool.createNew(this, "rpc connection handler");

		// try again in different thread
		if (socket == null) {
			return;
		}

		try {
			ObjectInputStream in = new ObjectInputStream(
					new BufferedInputStream(socket.getInputStream()));

			String objectName = in.readUTF();

			RemoteObject object = getObject(objectName);

			if (object == null) {
				throw new IOException("unknown object " + objectName);
			}

			ObjectOutputStream out = new ObjectOutputStream(
					new BufferedOutputStream(socket.getOutputStream()));
			out.flush();

			object.invoke(in, out);

			out.flush();
			out.close();
			in.close();
		} catch (IOException e) {
			logger.error("Exception on handling incoming RPC connection", e);
		} catch (ClassNotFoundException e) {
			logger.error("Exception on handling incoming RPC connection", e);
		} finally {
			try {
				if (!socket.isClosed()) {
					socket.close();
				}
			} catch (IOException e) {
				logger.warn("Could not close socket", e);
			}
		}
	}

	public synchronized void unexport(String name) throws RemoteException {
		if (!objects.containsKey(name)) {
			throw new RemoteException("Cannot unexport \"" + name
					+ "\", object does not exist");
		}
		objects.remove(name);

	}

}