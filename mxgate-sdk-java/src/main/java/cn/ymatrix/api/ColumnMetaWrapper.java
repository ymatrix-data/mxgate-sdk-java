package cn.ymatrix.api;

public class ColumnMetaWrapper {

    public static ColumnMetaWrapper wrap(ColumnMeta columnMeta) throws NullPointerException {
        if (columnMeta == null) {
            throw new NullPointerException("Wrap on an empty columnMeta.");
        }
        return new ColumnMetaWrapper(columnMeta);
    }

    private ColumnMetaWrapper(ColumnMeta columnMeta) throws NullPointerException {
        this.type = columnMeta.getType();
        this.name = columnMeta.getName();
        this.num = columnMeta.getNum();
        this.isNotNull = columnMeta.getIsNotNull();
        this.isSetDefault = columnMeta.getIsSetDefault();
        this.isSerial = columnMeta.getIsSerial();
        this.isExcluded = columnMeta.getIsExcluded();
        this.isUpsertKey = columnMeta.getIsUpsertKey();
        this.isDeduplicateKey = columnMeta.getIsDeduplicateKey();
        this.defaultValue = columnMeta.getDefault();
    }

    private String type;

    private String name;

    private int num;

    private boolean isNotNull;

    private boolean isSetDefault;

    private boolean isSerial;

    private boolean isExcluded;

    private boolean isUpsertKey;

    private boolean isDeduplicateKey;

    private String defaultValue;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    public void setNotNull(boolean notNull) {
        isNotNull = notNull;
    }

    public boolean isSetDefault() {
        return isSetDefault;
    }

    public void setSetDefault(boolean setDefault) {
        isSetDefault = setDefault;
    }

    public boolean isSerial() {
        return isSerial;
    }

    public void setSerial(boolean serial) {
        isSerial = serial;
    }

    public boolean isExcluded() {
        return isExcluded;
    }

    public void setExcluded(boolean excluded) {
        isExcluded = excluded;
    }

    public boolean isUpsertKey() {
        return isUpsertKey;
    }

    public void setUpsertKey(boolean upsertKey) {
        isUpsertKey = upsertKey;
    }

    public boolean isDeduplicateKey() {
        return isDeduplicateKey;
    }

    public void setDeduplicateKey(boolean deduplicateKey) {
        isDeduplicateKey = deduplicateKey;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
}
