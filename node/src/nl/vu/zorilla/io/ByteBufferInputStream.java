package nl.vu.zorilla.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public final class ByteBufferInputStream extends InputStream {
    
    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        return buffer.get();
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len > available()) {
            len = available();
        }
        buffer.get(b, off, len);
        return len;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }
}
