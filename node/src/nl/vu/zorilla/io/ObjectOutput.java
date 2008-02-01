package nl.vu.zorilla.io;

import java.io.IOException;

public interface ObjectOutput extends ibis.io.DataOutput {
    
    public void writeString(String val) throws IOException;
    
    public void writeObject(Object object) throws IOException;

}
