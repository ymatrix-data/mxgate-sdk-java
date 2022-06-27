package cn.ymatrix.objectpool;

import cn.ymatrix.data.Tuples;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class TuplesPoolFactory extends BasePooledObjectFactory<Tuples> {

    private final TuplesCreator creator;

    public TuplesPoolFactory(TuplesCreator tuplesCreator) {
        this.creator = tuplesCreator;
    }

    @Override
    public Tuples create() throws Exception {
        if (this.creator == null) {
            throw new NullPointerException("Tuples creation on a null TuplesCreator.");
        }
        return creator.createTuples();
    }

    @Override
    public PooledObject<Tuples> wrap(Tuples tuples) {
        return new DefaultPooledObject<Tuples>(tuples);
    }
}
