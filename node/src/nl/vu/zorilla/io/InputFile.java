package nl.vu.zorilla.io;

import java.io.File;

import nl.vu.zorilla.ZorillaException;

/**
 * Read-only input File in a virtual file system.
 */
public interface InputFile {
    
    long size() throws ZorillaException;

    //absolute path in "virtual" file system
    String path();

    /**
     * Copy this file to a "real" file in the given directory. 
     * May block to download file if needed.
     * @throws ZorillaException 
     */
    File copyTo(File dir) throws ZorillaException; 

    /**
     * Read data from the file at a specific offset.
     * Blocks until at leat one byte is returned
     * 
     * @return the number of bytes read, or -1 if the end of file has been reached
     * 
     * @throws IOException in case of trouble
     * reading data
     */
    int read(long fileOffset, byte[] data, int offset, int length) throws ZorillaException;
    
    void close() throws ZorillaException;
}
