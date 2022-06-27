package cn.ymatrix.objectpool;

import cn.ymatrix.data.Tuples;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;

import java.security.InvalidParameterException;

public class TuplesPool {
    private static final String TAG = StrUtil.logTagWrap(TuplesPool.class.getName());

    private static final Logger l = MxLogger.init(TuplesPool.class);

    private final GenericObjectPool<Tuples> objectPool;

    public TuplesPool(cn.ymatrix.objectpool.TuplesCreator creator, int size) throws NullPointerException {
        if (creator == null) {
            throw new NullPointerException("Creator is null in TuplesPool construction.");
        }
        if (size <= 0) {
            throw new InvalidParameterException("invalid tuples pool size: " + size);
        }
        GenericObjectPoolConfig<Tuples> conf = new GenericObjectPoolConfig<>();
        conf.setMaxTotal(size);
        int idle = size / 2;
        if (idle > 0) {
            conf.setMaxIdle(idle);
        } else {
            conf.setMaxIdle(size);
        }
        this.objectPool = new GenericObjectPool<>(new cn.ymatrix.objectpool.TuplesPoolFactory(creator), conf);
        l.info("Create TuplesPool with max total capacity {} and max idle {}", size, idle);
    }

    public Tuples tuplesBorrow() {
        try {
            return this.objectPool.borrowObject();
        } catch (Exception e) {
            l.error("{} Tuples pool borrow exception", TAG, e);
        }
        return null;
    }

    public void tuplesReturn(Tuples tuples) {
        if (tuples == null) {
            l.error("{} Append empty tuples into Tuples Object pool", TAG);
            return;
        }
        this.objectPool.returnObject(tuples);
    }

}
