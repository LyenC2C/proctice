package serializer;

import com.esotericsoftware.kryo.Kryo;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoPool extends Pool<Kryo> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public KryoPool(int total) {

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(total);
        config.setMaxWaitMillis(10000);
        PooledObjectFactory factory = new PooledObjectFactory() {
            @Override
            public PooledObject makeObject() throws Exception {

                Kryo kryo = new Kryo();
                return new DefaultPooledObject<>(kryo);
            }

            @Override
            public void destroyObject(PooledObject p) throws Exception {
                p = null;
            }

            @Override
            public boolean validateObject(PooledObject p) {
                return true;
            }

            @Override
            public void activateObject(PooledObject p) throws Exception {
            }

            @Override
            public void passivateObject(PooledObject p) throws Exception {
            }
        };
        super.initPool(config, factory);
    }

    @Override
    public Kryo getResource() {
        if (logger.isDebugEnabled()) {
            logger.debug("get client,active {},remind {},wait {}", super.internalPool.getNumActive(), super.internalPool.getNumIdle(), super.internalPool.getNumWaiters());
        }
        return super.getResource();
    }
}
