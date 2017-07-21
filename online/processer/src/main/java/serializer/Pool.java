package serializer;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class Pool<T> implements Closeable {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected GenericObjectPool<T> internalPool;

    public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {

        if (this.internalPool != null) {
            try {
                close();
            } catch (Exception e) {
            }
        }

        this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
    }

    public T getResource() {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            throw new RuntimeException("Could not get a resource from the pool", e);
        }
    }

    public void returnResourceObject(final T resource) {
        if (resource == null) {
            return;
        }
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new RuntimeException("Could not return the resource to the pool", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new RuntimeException("Could not destroy the pool", e);
        }
    }
}
