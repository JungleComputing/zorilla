/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */
package bamboo.util;
import org.acplt.oncrpc.XdrAble;
import java.nio.ByteBuffer;
import org.acplt.oncrpc.XdrDecodingStream;

/**
 * Clone XdrAble objects by serializing and deserializing them.
 *
 * @author  Sean C. Rhea
 * @version $Id$
 */
public class XdrClone {

    /**
     * Clone XdrAble objects by serializing and deserializing them; the buffer
     * is cleared before serialization and not cleared after deserialization,
     * so the size of the object can be found by calling buf.position () after
     * a call to xdr_clone.
     */
    public static XdrAble xdr_clone (XdrAble value, ByteBuffer buf) {
        buf.clear();
        XdrByteBufferEncodingStream es = new XdrByteBufferEncodingStream(buf);
        try {
            value.xdrEncode(es);
            buf.flip();
            XdrByteBufferDecodingStream ds =
                new XdrByteBufferDecodingStream(buf);
            ds.beginDecoding();
            return (XdrAble) value.getClass().
                getConstructor(new Class[] {XdrDecodingStream.class}).
                newInstance(new Object[] {ds});
        }
        catch (Exception e) {
            assert false:e;
            return null; // in case asserts are turned off
        }
    }

 
}
