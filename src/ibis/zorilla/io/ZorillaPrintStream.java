package ibis.zorilla.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;

public class ZorillaPrintStream extends OutputStream {

    private final OutputStream out;

    public ZorillaPrintStream(OutputStream out) {
        this.out = out;
    }

    public void printlog(String string) throws IOException {
        String log = new Date() + " | " + string;
        println(log);
    }

    public void println(String string) throws IOException {
        String line = string + "\n";
        try {
            write(line.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IOException("could not do UTF-8 encoding");
        }
    }

    public void print(Throwable e) throws IOException {
        StackTraceElement[] stack = e.getStackTrace();
        String stacktraceString = e.getMessage() + ":";
        for (StackTraceElement element : stack) {
            stacktraceString += "\n  " + element.toString();
        }
        println(stacktraceString);
        Throwable cause = e.getCause();
        if (cause != null) {
            println(" caused by: ");
            print(cause);
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }
}
