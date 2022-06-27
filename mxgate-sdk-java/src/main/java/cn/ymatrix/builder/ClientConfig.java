package cn.ymatrix.builder;


/**
 * The configuration of Client
 */
public class ClientConfig {
    private String schema;
    private String table;

    public ClientConfig(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
