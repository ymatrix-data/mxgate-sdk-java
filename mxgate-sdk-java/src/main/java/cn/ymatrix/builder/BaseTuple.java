package cn.ymatrix.builder;

import cn.ymatrix.data.Column;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.security.InvalidParameterException;

public abstract class BaseTuple implements Tuple {
    protected static final Logger l = MxLogger.init(BaseTuple.class);
    protected String delimiter;
    protected Column[] columns;
    protected final String schema;
    protected final String table;
    protected long serialNum;

    BaseTuple(String delimiter, String schema, String table) throws InvalidParameterException {
        if (StrUtil.isNullOrEmpty(delimiter)) {
            throw new InvalidParameterException("Delimiter is empty when try to create a lite Tuple.");
        }
        if (StrUtil.isNullOrEmpty(schema)) {
            throw new InvalidParameterException("Table schema is empty when try to create a lite Tuple.");
        }
        if (StrUtil.isNullOrEmpty(table)) {
            throw new InvalidParameterException("Table name is empty when try to create a lite Tuple.");
        }
        this.table = table;
        this.schema = schema;
        this.delimiter = delimiter;
    }

    @Override
    public String getTableName() {
        return StrUtil.connect(this.schema, ".", this.table);
    }

    @Override
    public byte[] toBytes() {
        return this.toString().getBytes();
    }

    @Override
    public String toString() {
        return this.toCSVLineStr();
    }

    @Override
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String getDelimiter() {
        return this.delimiter;
    }


    @Override
    public void setSerialNum(long sn) {
        this.serialNum = sn;
    }

    @Override
    public long getSerialNum() {
        return this.serialNum;
    }
}
