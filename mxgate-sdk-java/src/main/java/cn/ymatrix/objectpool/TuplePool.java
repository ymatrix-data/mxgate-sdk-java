package cn.ymatrix.objectpool;

import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;

import java.security.InvalidParameterException;

public class TuplePool {
    private static final String TAG = StrUtil.logTagWrap(TuplePool.class.getName());
    private static final Logger l = MxLogger.init(TuplePool.class);
    private final GenericObjectPool<Tuple> objectPool;

    public TuplePool(cn.ymatrix.objectpool.TupleCreator creator, int size) throws NullPointerException {
        if (creator == null) {
            throw new NullPointerException("TupleCreator is null in TuplesPool's construction.");
        }
        if (size <= 0) {
            throw new InvalidParameterException("invalid tuple pool size: " + size);
        }
        GenericObjectPoolConfig<Tuple> conf = new GenericObjectPoolConfig<>();
        conf.setMaxTotal(size);
        int idle = size / 2;
        if (idle > 0) {
            conf.setMaxIdle(idle);
        } else {
            conf.setMaxIdle(size);
        }
        this.objectPool = new GenericObjectPool<>(new cn.ymatrix.objectpool.TuplePoolFactory(creator), conf);
        l.info("{} Create TuplePool with max total capacity {} and max idle {}", TAG, size, idle);
    }

    public Tuple tupleBorrow() {
        try {
            return this.objectPool.borrowObject();
        } catch (Exception e) {
            l.error("{} Tuple pool borrow exception", TAG, e);
        }
        return null;
    }

    public void tupleReturn(Tuple tuple) {
        if (tuple == null) {
            l.error("{} Append empty tuple into Tuple Object pool", TAG);
            return;
        }
        this.objectPool.returnObject(tuple);
    }

}
