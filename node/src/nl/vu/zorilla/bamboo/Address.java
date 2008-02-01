package nl.vu.zorilla.bamboo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaError;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.util.Enums;
import nl.vu.zorilla.util.SizeOf;

import org.apache.log4j.Logger;

/**
 * Immutable class representing information about a particular node in the
 * system. Not all info may be available. A node info object is timestamped.
 */
final class Address {

    Logger logger = Logger.getLogger(Address.class);

    // nodeinfo types
    public enum Type {
        EMPTY, NORMAL, LOCATION, IP_ADDRESS;
    }

    // size of an Address object.
    // consists of node-ID + location + inet address + port + timestamp + type
    // + 1st 20 characters of version + 1st 20 characters of network name + 1st 20 characters of cluster
    public static final int SIZE = SizeOf.UUID + Location.SIZE
        + SizeOf.INET6_ADDRESS + SizeOf.INT + SizeOf.LONG + SizeOf.INT
        + + SizeOf.INT + (SizeOf.BYTE * 100) + SizeOf.INT + (SizeOf.BYTE * 100) + SizeOf.INT
        + (SizeOf.BYTE * 100);

    public static final Address UNKNOWN;

    static {
        UNKNOWN = new Address();
    }

    private final UUID nodeID;

    private final Location location;

    private final InetSocketAddress inetSocketAddress;

    private final long timestamp;

    private final Type type;

    // long representing build time of zorilla
    private final String zorillaVersion;

    private final String networkName;

    private final String cluster;

    /**
     * Returns the newest of two nodeinfo objects. Throws an error when no
     * complete NodeInfo object is given
     * 
     */
    public static Address newest(Address first, Address second)
        throws ZorillaError {

        if (first.type != second.type) {
            throw new ZorillaError("tried to select newest from two"
                + "addresses of a different type");
        }
        if (first.timestamp >= second.timestamp) {
            return first;
        }
        return second;
    }

    private static boolean allZeros(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != (byte) 0) {
                return false;
            }
        }
        return true;
    }

    // Empty NodeInfo object
    private Address() {
        this.nodeID = null;
        this.location = Location.nowhere;
        this.inetSocketAddress = null;
        this.timestamp = 0;
        this.type = Type.EMPTY;
        this.zorillaVersion = "";
        this.networkName = "";
        cluster = "";
    }

    /**
     * Creates a new nodeinfo object. The object will be timestamped with the
     * current time.
     */
    public Address(Node node, UUID nodeID, Location location,
        InetSocketAddress inetSocketAddress, Type type, String cluster) {
        this.nodeID = nodeID;
        this.location = location;
        this.inetSocketAddress = inetSocketAddress;
        this.type = type;
        this.zorillaVersion = node.getVersion();
        this.networkName = node.config().getNetworkName();
        this.cluster = cluster;

        timestamp = System.currentTimeMillis();
    }

    /**
     * Creates a new address object. The object will be timestamped with the
     * current time.
     */
    Address(Location location, Node node) {
        this.location = location;

        nodeID = null;
        inetSocketAddress = null;
        timestamp = System.currentTimeMillis();
        type = Type.LOCATION;
        zorillaVersion = node.getVersion();
        networkName = node.config().getNetworkName();
        cluster = "";
    }

    /**
     * Creates a new NodeInfo object for a node with only the ip address and
     * socket filled. The object will be timestamped with the current time.
     * 
     */
    Address(InetSocketAddress inetSocketAddress, Node node) {
        this.inetSocketAddress = inetSocketAddress;

        nodeID = null;
        location = Location.nowhere;
        timestamp = System.currentTimeMillis();
        type = Type.IP_ADDRESS;
        zorillaVersion = node.getVersion();
        networkName = node.config().getNetworkName();
        cluster = "";
    }

    Address(InetAddress address, int port, Node node) {
        this(new InetSocketAddress(address, port), node);
    }

    /**
     * Constructs a Address object from the contents of a byte buffer.
     * 
     * @param buffer
     *            the buffer to fill the info with
     */
    Address(ByteBuffer buffer) {
        int startPosition = buffer.position();

        byte[] inetAddressBytes = new byte[SizeOf.INET6_ADDRESS];

        long msbUUID = buffer.getLong();
        long lsbUUID = buffer.getLong();

        if (msbUUID == 0L && lsbUUID == 0L) {
            nodeID = null;
        } else {
            nodeID = new UUID(msbUUID, lsbUUID);
        }

        location = new Location(buffer);

        buffer.get(inetAddressBytes);
        int port = buffer.getInt();

        if (allZeros(inetAddressBytes)) {
            inetSocketAddress = null;
        } else {
            try {
                InetAddress inetAddress = InetAddress
                    .getByAddress(inetAddressBytes);
                inetSocketAddress = new InetSocketAddress(inetAddress, port);
            } catch (UnknownHostException e) {
                throw new ZorillaError(e);
            }
        }

        logger.debug("got inet address: " + inetSocketAddress);

        timestamp = buffer.getLong();

        try {
            type = Enums.getEnumConstant(Type.class, buffer.getInt());
        } catch (ZorillaException e) {
            throw new ZorillaError(e);
        }

        int versionLength = buffer.getInt();
        byte[] versionArray = new byte[100];
        buffer.get(versionArray);

        try {
            zorillaVersion = new String(versionArray, 0, versionLength, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZorillaError(e);
        }


        int networkNameLength = buffer.getInt();
        byte[] networkNameArray = new byte[100];
        buffer.get(networkNameArray);

        try {
            networkName = new String(networkNameArray, 0, networkNameLength,
                "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZorillaError(e);
        }

        int clusterLength = buffer.getInt();
        byte[] clusterArray = new byte[100];
        buffer.get(clusterArray);

        try {
            cluster = new String(clusterArray, 0, clusterLength, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZorillaError(e);
        }

        assert buffer.position() - startPosition == SIZE;
    }

    /**
     * Constructs a NodeInfo object from a buffer containing bytes.
     * 
     */
    public Address(DataInput in) throws IOException {
        byte[] inetAddressBytes = new byte[SizeOf.INET6_ADDRESS];

        long msbUUID = in.readLong();
        long lsbUUID = in.readLong();

        if (msbUUID == 0L && lsbUUID == 0L) {
            nodeID = null;
        } else {
            nodeID = new UUID(msbUUID, lsbUUID);
        }

        location = new Location(in);

        in.readArray(inetAddressBytes, 0, inetAddressBytes.length);
        int port = in.readInt();

        if (allZeros(inetAddressBytes)) {
            inetSocketAddress = null;
        } else {
            InetAddress inetAddress = InetAddress
                .getByAddress(inetAddressBytes);
            inetSocketAddress = new InetSocketAddress(inetAddress, port);
        }

        timestamp = in.readLong();

        try {
            type = Enums.getEnumConstant(Type.class, in.readInt());
        } catch (ZorillaException e) {
            throw new ZorillaError(e);
        }

        int versionLength = in.readInt();
        byte[] versionArray = new byte[100];
        in.readArray(versionArray);

        zorillaVersion = new String(versionArray, 0, versionLength,
            "UTF-8");

        int networkNameLength = in.readInt();
        byte[] networkNameArray = new byte[100];
        in.readArray(networkNameArray);

        networkName = new String(networkNameArray, 0, networkNameLength,
            "UTF-8");

        int clusterLength = in.readInt();
        byte[] clusterArray = new byte[100];
        in.readArray(clusterArray);

        cluster = new String(clusterArray, 0, clusterLength, "UTF-8");

    }

    public UUID nodeID() {
        return nodeID;
    }

    public Location location() {
        return location;
    }

    /**
     * Returns the last known socket address of this node.
     * 
     * @return the last known socket address of this node.
     */
    public InetSocketAddress socketAddress() {
        return inetSocketAddress;
    }

    public InetAddress inetAddress() {
        return inetSocketAddress.getAddress();
    }

    public int port() {
        return inetSocketAddress.getPort();
    }

    public Type type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }

    public String cluster() {
        return cluster;
    }

    /**
     * Write this nodeinfo object to a buffer.
     * 
     * @param buffer
     *            the buffer to write this object to.
     */
    void writeTo(ByteBuffer buffer) {
        int startPosition = buffer.position();

        if (nodeID == null) {
            buffer.putLong(0L);
            buffer.putLong(0L);
        } else {
            buffer.putLong(nodeID.getMostSignificantBits());
            buffer.putLong(nodeID.getLeastSignificantBits());
        }

        location.writeTo(buffer);

        if (inetSocketAddress == null) {
            byte[] zeros = new byte[SizeOf.INET6_ADDRESS];
            buffer.put(zeros);
            buffer.putInt(0);
        } else {
            byte[] inetAddressBytes = inetSocketAddress.getAddress()
                .getAddress();

            if (inetAddressBytes.length == SizeOf.INET4_ADDRESS) {
                // IPV4 Address, convert to ::ffff:w.x.y.z
                byte[] ipv6Bytes = new byte[SizeOf.INET6_ADDRESS];
                ipv6Bytes[0xA] = (byte) 0xFF;
                ipv6Bytes[0xB] = (byte) 0xFF;
                ipv6Bytes[0xC] = inetAddressBytes[0];
                ipv6Bytes[0xD] = inetAddressBytes[1];
                ipv6Bytes[0xE] = inetAddressBytes[2];
                ipv6Bytes[0xF] = inetAddressBytes[3];
                inetAddressBytes = ipv6Bytes;
            }

            buffer.put(inetAddressBytes);
            buffer.putInt(inetSocketAddress.getPort());
        }

        buffer.putLong(timestamp);

        buffer.putInt(type.ordinal());

        byte[] versionArray = new byte[100];
        byte[] versionBytes;

        try {
            versionBytes = zorillaVersion.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZorillaError(e);
        }

        if (versionBytes.length > 100) {
            throw new ZorillaError(
                "cluster names longer than 100 bytes not supported");
        }
        

        System.arraycopy(versionBytes, 0, versionArray, 0, versionBytes.length);

        buffer.putInt(versionBytes.length);
        buffer.put(versionArray);
        
        byte[] networkNameArray = new byte[100];
        byte[] networkNameBytes;

        if (networkName == null) {
            networkNameBytes = new byte[0];
        } else {
            try {
                networkNameBytes = networkName.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new ZorillaError(e);
            }

        }

        if (networkNameBytes.length > 100) {
            throw new ZorillaError(
                "network names longer than 100 bytes not supported");
        }

        System.arraycopy(networkNameBytes, 0, networkNameArray, 0,
            networkNameBytes.length);

        buffer.putInt(networkNameBytes.length);
        buffer.put(networkNameArray);

        byte[] clusterArray = new byte[100];
        byte[] clusterBytes;
        try {
            clusterBytes = cluster.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZorillaError(e);
        }

        if (clusterBytes.length > 100) {
            throw new ZorillaError(
                "cluster names longer than 100 bytes not supported");
        }

        System.arraycopy(clusterBytes, 0, clusterArray, 0, clusterBytes.length);

        buffer.putInt(clusterBytes.length);
        buffer.put(clusterArray);

        assert buffer.position() - startPosition == SIZE : "Address byte representation not specified size";

    }

    public void writeTo(DataOutput out) throws IOException {

        if (nodeID == null) {
            out.writeLong(0L);
            out.writeLong(0L);
        } else {
            out.writeLong(nodeID.getMostSignificantBits());
            out.writeLong(nodeID.getLeastSignificantBits());
        }

        location.writeTo(out);

        if (inetSocketAddress == null) {
            byte[] zeros = new byte[SizeOf.INET6_ADDRESS];
            out.writeArray(zeros, 0, zeros.length);
            out.writeInt(0);
        } else {
            byte[] inetAddressBytes = inetSocketAddress.getAddress()
                .getAddress();

            if (inetAddressBytes.length == SizeOf.INET4_ADDRESS) {
                // IPV4 Address, convert to ::ffff:w.x.y.z
                byte[] ipv6Bytes = new byte[SizeOf.INET6_ADDRESS];
                ipv6Bytes[0xA] = (byte) 0xFF;
                ipv6Bytes[0xB] = (byte) 0xFF;
                ipv6Bytes[0xC] = inetAddressBytes[0];
                ipv6Bytes[0xD] = inetAddressBytes[1];
                ipv6Bytes[0xE] = inetAddressBytes[2];
                ipv6Bytes[0xF] = inetAddressBytes[3];
                inetAddressBytes = ipv6Bytes;
            }
            out.writeArray(inetAddressBytes, 0, SizeOf.INET6_ADDRESS);
            out.writeInt(inetSocketAddress.getPort());
        }
        out.writeLong(timestamp);

        out.writeInt(type.ordinal());

        byte[] versionArray = new byte[100];
        byte[] versionBytes = zorillaVersion.getBytes("UTF-8");

        if (versionBytes.length > 100) {
            throw new ZorillaError(
                "cluster names longer than 100 bytes not supported");
        }

        System.arraycopy(versionBytes, 0, versionArray, 0, versionBytes.length);

        out.writeInt(versionBytes.length);
        out.writeArray(versionArray);

        byte[] networkNameArray = new byte[100];
        byte[] networkNameBytes;
        
        if (networkName == null) {
            networkNameBytes = new byte[0];
        } else {
            networkNameBytes = networkName.getBytes("UTF-8");
        }

        if (networkNameBytes.length > 100) {
            throw new ZorillaError(
                "network names longer than 100 bytes not supported");
        }

        System.arraycopy(networkNameBytes, 0, networkNameArray, 0,
            networkNameBytes.length);

        out.writeInt(networkNameBytes.length);
        out.writeArray(networkNameArray);

        byte[] clusterArray = new byte[100];
        byte[] clusterBytes = cluster.getBytes("UTF-8");

        if (clusterBytes.length > 100) {
            throw new ZorillaError(
                "cluster names longer than 100 bytes not supported");
        }

        System.arraycopy(clusterBytes, 0, clusterArray, 0, clusterBytes.length);

        out.writeInt(clusterBytes.length);
        out.writeArray(clusterArray);
    }

    public boolean unknown() {
        return (type == Type.EMPTY);
    }

    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof Address) {
            return equals((Address) object);
        }

        return false;
    }

    /**
     * Checks if a NodeInfo object is equal to another given identifier.
     * 
     * @param other
     *            the nodeinfo object to compare to.
     * @return if the given object is equal to this object.
     */
    boolean equals(Address other) {
        return other.nodeID.equals(nodeID)
            && other.location.equals(location)
            && (other.inetSocketAddress == null && inetSocketAddress == null || (other.inetSocketAddress
                .equals(this.inetSocketAddress)))
            && other.timestamp == timestamp && other.type == this.type;
    }

    public boolean sameNodeAs(Address other) {
        return other.nodeID.equals(nodeID);
    }

    public boolean sameLocationAs(Address other) {
        return other.location.equals(location);
    }

    public boolean checkVersion(String version) {
        if (version == null || this.zorillaVersion == null) {
            return false;
        }
        return this.zorillaVersion.equals(version);
    }

    public boolean checkNetworkName(String name) {
        if (!networkName.equals(name)) {
            logger.warn("network name \"" + networkName
                + "\" does not match \"" + name + "\"");
            return false;
        }
        return true;
    }

    public String toVerboseString() {
        return " nodeID:      " + nodeID + "\n address:     "
            + inetSocketAddress + "\n location:    " + location
            + "\n timestamp:   " + (new Date(timestamp)) + "\n type:        "
            + type;

    }

    public String toString() {
        String host;
        if (inetSocketAddress != null) {
            host = inetSocketAddress.getHostName() + ":"
                + inetSocketAddress.getPort();
        } else if (nodeID != null) {
            host = nodeID.toString().substring(0, 8);
        } else {
            host = "unknown";
        }

        switch (type) {
        case EMPTY:
            return "`UNKNOWN ADDRESS'";
        case NORMAL:
            return host;
        case LOCATION:
            return location.toString();
        case IP_ADDRESS:
            return host;
        default:
            throw new ZorillaError("unknown type");
        }
    }
}