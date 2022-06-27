package cn.ymatrix.objectpool;

import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;
import org.slf4j.Logger;

public class TuplePoolSoft {
    private static final String TAG = StrUtil.logTagWrap(TuplePool.class.getName());
    private static final Logger l = MxLogger.init(TuplePool.class);
    private final TuplePoolFactory tuplePoolFactory;
    private final SoftReferenceObjectPool<Tuple> objectPool;

    private static TupleCreator tupleCreator;

    private TuplePoolSoft(TupleCreator creator, int size) throws NullPointerException {
        if (creator == null) {
            throw new NullPointerException("TupleCreator is null in TuplesPool's construction.");
        }
        this.tuplePoolFactory = new TuplePoolFactory(creator);
        this.objectPool = new SoftReferenceObjectPool<>(this.tuplePoolFactory);
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
        try {
            this.objectPool.returnObject(tuple);
        } catch (Exception e) {
            l.error("{} Return Tuples back to soft tuple pool exception", TAG);
        }
    }

}
