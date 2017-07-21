package serializer;

import java.io.Serializable;

public interface ISerializer extends Serializable {

    byte[] serialize(Object object);

    Object deserialize(byte[] data);
}
