package nl.vu.zorilla.io;

import java.io.IOException;

import ibis.io.DataInput;

public interface ObjectInput extends DataInput {
    
    public String readString() throws IOException;

    public Object readObject() throws IOException, ClassNotFoundException;

}
