package nl.vu.zorilla.bamboo;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.gridlab.gat.URI;


public interface DataInput extends ibis.io.DataInput {

    String readString() throws IOException;

    String[] readStrings() throws IOException;

    void readStringMap(Map<String, String> map) throws IOException;

    Map<String, String> readStringMap() throws IOException;

    UUID readUUID() throws IOException;

    UUID[] readUUIDs() throws IOException;

    URI readURI() throws IOException;

    URI[] readURIs() throws IOException;
}