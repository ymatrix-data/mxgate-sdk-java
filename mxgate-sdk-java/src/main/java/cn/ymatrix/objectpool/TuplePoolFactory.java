package cn.ymatrix.objectpool;

import cn.ymatrix.data.Tuple;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

class TuplePoolFactory extends BasePooledObjectFactory<Tuple> {
    private final TupleCreator creator;

    public TuplePoolFactory(TupleCreator creator) {
        this.creator = creator;
    }

    @Override
    public Tuple create() throws Exception {
        if (this.creator == null) {
            throw new NullPointerException("TupleCreator is null");
        }
        return this.creator.createTuple();
    }

    @Override
    public PooledObject<Tuple> wrap(Tuple tuple) {
        return new DefaultPooledObject<Tuple>(tuple);
    }
}
