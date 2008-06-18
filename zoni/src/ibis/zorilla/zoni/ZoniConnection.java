package ibis.zorilla.zoni;

import ibis.smartsockets.SmartSocketsProperties;
import ibis.smartsockets.direct.DirectSocket;
import ibis.smartsockets.direct.DirectSocketAddress;
import ibis.smartsockets.direct.DirectSocketFactory;
import ibis.smartsockets.util.TypedProperties;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.log4j.Logger;

public final class ZoniConnection {

	public static final int TIMEOUT = 10 * 1000;

	Logger logger = Logger.getLogger(ZoniConnection.class);

	private final DirectSocket socket;

	private final ObjectInputStream in;

	private final ObjectOutputStream out;

	private final String peerID;

	public ZoniConnection(String address, String id) throws IOException {
		DirectSocketAddress socketAddress;

		try {
			socketAddress = DirectSocketAddress.getByAddress(address);
		} catch (Exception e) {
			socketAddress = DirectSocketAddress.getByAddress(address,
					ZoniProtocol.DEFAULT_PORT);
		}

		TypedProperties factoryProperties = SmartSocketsProperties
				.getDefaultProperties();

		DirectSocketFactory socketFactory = DirectSocketFactory
				.getSocketFactory(factoryProperties);

		socket = socketFactory.createSocket(socketAddress, TIMEOUT, 0, null);

		// signal we are a user connection
		socket.getOutputStream().write(ZoniProtocol.TYPE_USER);

		logger.debug("connected");

		out = new ObjectOutputStream(new BufferedOutputStream(socket
				.getOutputStream()));

		out.writeInt(ZoniProtocol.VERSION);
		if (id == null) {
			id = "anonymous";
		}
		out.writeUTF(id);
		out.writeInt(ZoniProtocol.AUTHENTICATION_NONE);
		out.flush();

		logger.debug("send connection init");

		in = new ObjectInputStream(new BufferedInputStream(socket
				.getInputStream()));

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("connecting failed, peer replied: " + message);
		}
		peerID = in.readUTF();

		logger.debug("reply from peer: status = " + status + ", message = "
				+ message + ", id = " + peerID);

	}

	public String peerID() {
		return peerID;
	}

	public String submitJob(ZorillaJobDescription job,
			CallbackReceiver callbackReceiver) throws IOException {
		logger.debug("submitting job");

		out.writeInt(ZoniProtocol.OPCODE_SUBMIT_JOB);
		job.writeTo(out);

		if (callbackReceiver != null) {
			out.writeBoolean(true);
			out.writeObject(callbackReceiver.addresses());
		} else {
			out.writeBoolean(false);
		}

		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("submission failed: " + message);
		}

		String jobID = in.readUTF();

		return jobID;
	}

	@SuppressWarnings("unchecked")
	public JobInfo getJobInfo(String jobID) throws IOException {
		try {
			out.writeInt(ZoniProtocol.OPCODE_GET_JOB_INFO);
			out.writeUTF(jobID);
			out.flush();

			int status = in.readInt();
			String message = in.readUTF();

			if (status != ZoniProtocol.STATUS_OK) {
				close();
				throw new IOException("exception on getting job info: "
						+ message);
			}

			String id = in.readUTF();
			String executable = in.readUTF();
			Map<String, String> attributes = (Map<String, String>) in
					.readObject();
			Map<String, String> jobStatus = (Map<String, String>) in
					.readObject();
			int phase = in.readInt(); // phase
			int exitStatus = in.readInt();

			return new JobInfo(id, executable, attributes, jobStatus, phase,
					exitStatus);

		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}

	}

	public void setJobAttributes(String jobID,
			Map<String, String> updatedAttributes) throws IOException {

		out.writeInt(ZoniProtocol.OPCODE_SET_JOB_ATTRIBUTES);
		out.writeUTF(jobID);

		out.writeObject(updatedAttributes);

		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on setting job attributes: "
					+ message);
		}
	}

	public void cancelJob(String jobID) throws IOException {
		if (jobID == null) {
			return;
		}

		logger.debug("killing job " + jobID);
		out.writeInt(ZoniProtocol.OPCODE_CANCEL_JOB);
		out.writeUTF(jobID);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on cancelling job: " + message);
		}
	}

	public String[] getJobList() throws IOException {
		out.writeInt(ZoniProtocol.OPCODE_GET_JOB_LIST);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on getting job list: " + message);
		}

		int nrOfJobs = in.readInt();

		String[] jobs = new String[nrOfJobs];

		for (int i = 0; i < nrOfJobs; i++) {
			jobs[i] = in.readUTF();
		}
		return jobs;
	}

	@SuppressWarnings("unchecked")
	public Map getNodeInfo() throws IOException {
		logger.debug("getting node info");

		out.writeInt(ZoniProtocol.OPCODE_GET_NODE_INFO);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on getting node info: " + message);
		}

		try {
			return (Map<String, String>) in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public ZoniFileInfo getFileInfo(String sandboxPath, String jobID)
			throws IOException {
		logger.debug("getting file info");

		out.writeInt(ZoniProtocol.OPCODE_GET_FILE_INFO);
		out.writeUTF(jobID);
		out.writeUTF(sandboxPath);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on getting output files: "
					+ message);
		}

		try {

			return (ZoniFileInfo) in.readObject();

		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Returns a stream for reading standard out/err of the given job. This
	 * connection cannot be used after this call.
	 */
	public InputStream getOutput(String jobID, boolean stderr)
			throws IOException {
		logger.debug("getting stdout/stderr");

		if (stderr) {
			out.writeInt(ZoniProtocol.OPCODE_GET_STDERR);
		} else {
			out.writeInt(ZoniProtocol.OPCODE_GET_STDOUT);
		}

		out.writeUTF(jobID);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();
		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on getting stdout/stderr: "
					+ message);
		}

		return in;
	}

	public OutputStream getInput(String jobID)
			throws IOException {
		logger.debug("writing stdin");

		out.writeInt(ZoniProtocol.OPCODE_PUT_STDIN);
		out.writeUTF(jobID);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();
		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on putting stdin: " + message);
		}

		return out;
	}

	/**
	 * Writes an output file to the given stream.
	 * 
	 * @throws IOException
	 *             in case of trouble
	 */
	public void getOutputFile(OutputStream stream, String sandboxPath,
			String jobID) throws IOException {
		logger.debug("getting file");

		out.writeInt(ZoniProtocol.OPCODE_GET_FILE);

		out.writeUTF(jobID);
		out.writeUTF(sandboxPath);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on getting output file: "
					+ message);
		}

		long size = in.readLong();
		logger.debug("getting file " + sandboxPath + " of size " + size);

		byte[] buffer = new byte[1024];

		while (size > 0) {

			int tryRead = (int) Math.min(size, buffer.length);

			int read = in.read(buffer, 0, tryRead);

			if (read == -1) {
				throw new IOException("EOF on reading input file");
			}

			stream.write(buffer, 0, read);

			size = size - read;
		}

	}

	public void setNodeAttributes(Map<String, String> updatedAttributes)
			throws IOException {
		out.writeInt(ZoniProtocol.OPCODE_SET_NODE_ATTRIBUTES);

		out.writeObject(updatedAttributes);

		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		if (status != ZoniProtocol.STATUS_OK) {
			close();
			throw new IOException("exception on setting peer attributes: "
					+ message);
		}

	}

	public void kill(boolean recursive) throws IOException {
		logger.debug("killing peer (recursive = " + recursive + ")");

		out.writeInt(ZoniProtocol.OPCODE_KILL_NODE);
		out.writeBoolean(recursive);
		out.flush();

		int status = in.readInt();
		String message = in.readUTF();

		// kill always closes the connecton
		close();

		if (status != ZoniProtocol.STATUS_OK) {
			throw new IOException("exception on killing peer: " + message);
		}
	}

	public void close() {
		if (!socket.isClosed()) {
			try {
				out.writeInt(ZoniProtocol.OPCODE_CLOSE_CONNECTION);
				out.flush();
			} catch (IOException e) {
				logger.debug("IOException on closing connection", e);
			}
			try {
				socket.close();
			} catch (IOException e) {
				logger.debug("IOException on closing socket", e);
			}
		}
	}

	public boolean isClosed() {
		return socket.isClosed();
	}
}
