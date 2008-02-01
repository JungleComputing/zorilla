package nl.vu.zorilla.bamboo;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.gridlab.gat.URI;


public interface DataOutput extends ibis.io.DataOutput {

    void writeString(String string) throws IOException;

    void writeStrings(String[] strings) throws IOException;

    void writeStringMap(Map<String, String> stringMap) throws IOException;

    void writeUUID(UUID uuid) throws IOException;

    void writeUUIDs(UUID[] uuids) throws IOException;

    void writeURI(URI uri) throws IOException;

    void writeURIs(URI[] uris) throws IOException;
}