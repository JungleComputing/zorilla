package nl.vu.zorilla.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public final class ByteBufferOutputStream extends OutputStream {
    
    private final ByteBuffer buffer;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException {
        buffer.put((byte) (b & 0xF));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        buffer.put(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        buffer.put(b);
    }
}
