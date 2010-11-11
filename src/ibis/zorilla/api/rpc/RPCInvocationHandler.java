package ibis.zorilla.api.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RPCInvocationHandler implements InvocationHandler {

	private static final Logger logger = LoggerFactory
			.getLogger(RPCInvocationHandler.class);

	private final String name;
	private final int port;

	RPCInvocationHandler(String name, int port) {
		this.name = name;
		this.port = port;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {

		Socket socket = null;
		try {

			if (logger.isDebugEnabled()) {
				logger.debug("calling remote object " + name + ", method = "
						+ method.getName());
			}

			socket = new Socket(InetAddress.getByName(null), this.port);

			ObjectOutputStream out = new ObjectOutputStream(
					new BufferedOutputStream(socket.getOutputStream()));

			out.writeUTF(this.name);
			out.writeUTF(method.getName());
			out.writeObject(method.getParameterTypes());
			out.writeObject(args);
			out.flush();
			
			ObjectInputStream in = new ObjectInputStream(
					new BufferedInputStream(socket.getInputStream()));
			
			boolean success = in.readBoolean();
			Object result = in.readObject();
			
			if (logger.isDebugEnabled()) {
				logger.debug("remote object \"" + name + "\", method \"" + method.getName()
						+ "\" result = " + result);
			}

			// Close ports.
			out.close();
			in.close();

			if (success) {
				return result;
			} else if (result instanceof InvocationTargetException) {
				InvocationTargetException exception = (InvocationTargetException) result;
				
				//throw user exception
				throw exception.getTargetException();
			} else {
				//some error occured while doing remote call
				throw new RemoteException("exception while performing remote call", (Throwable) result);
			}
		} catch (IOException e) {
			throw new RemoteException("invocation failed", e);
		} finally {
			if (socket != null && !socket.isClosed()) {
				try {
					socket.close();
				} catch (IOException e) {
					throw new RemoteException("Could not close socket", e);
				}
			}
		}
	}
}
