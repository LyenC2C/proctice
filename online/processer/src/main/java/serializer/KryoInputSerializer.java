package serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoInputSerializer implements ISerializer {

    private static KryoPool kryoPool = new KryoPool(500);

    @Override
    public byte[] serialize(Object object) {

        if (object == null) return null;

        Output output = null;
        Kryo kryo = kryoPool.getResource();
        try {
            output = new Output(20, -1);
            kryo.writeClassAndObject(output, object);
            return output.getBuffer();
        } finally {
            if (output != null) {
                output.close();
            }
            kryoPool.returnResourceObject(kryo);
        }
    }

    @Override
    public Object deserialize(byte[] data) {

        if (data == null || data.length == 0)
            return null;

        Input input = null;
        Kryo kryo = kryoPool.getResource();
        try {
            input = new Input(data);
            return kryo.readClassAndObject(input);
        } finally {
            if (input != null) {
                input.close();
            }
            kryoPool.returnResourceObject(kryo);
        }
    }
}
