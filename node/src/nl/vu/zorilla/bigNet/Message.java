package nl.vu.zorilla.bigNet;

import nl.vu.zorilla.io.ObjectInput;
import nl.vu.zorilla.io.ObjectOutput;

public interface Message extends ObjectInput, ObjectOutput {
    

    public Function getFunction();
    
    void setFunction(Function function);
    

}
