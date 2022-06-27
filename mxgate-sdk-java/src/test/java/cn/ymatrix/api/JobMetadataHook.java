package cn.ymatrix.api;

import java.util.ArrayList;
import java.util.List;

public class JobMetadataHook {
    private int responseCode;

    private String delimiter;

    private String schema;

    private String table;
    private final List<MetadataHook> metadataHookList;
    private JobMetadataHook() {
        metadataHookList = new ArrayList<>();
    }

    private void addMetadataHook(MetadataHook hook) {
        metadataHookList.add(hook);
    }

    private void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    private void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    private void setSchema(String schema) {
        this.schema = schema;
    }

    private void setTable(String table) {
        this.table = table;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<MetadataHook> getMetadataHookList() {
        return metadataHookList;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public static class Builder {
        private final JobMetadataHook jobMetadataHook;

        public Builder() {
            jobMetadataHook = new JobMetadataHook();
        }

        public Builder setResponseCode(int responseCode) {
            this.jobMetadataHook.setResponseCode(responseCode);
            return this;
        }

        public Builder addJobMetadataHook(MetadataHook hook) {
            this.jobMetadataHook.addMetadataHook(hook);
            return this;
        }

        public JobMetadataHook build() {
            return this.jobMetadataHook;
        }

        public Builder setDelimiter(String delimiter) {
            this.jobMetadataHook.setDelimiter(delimiter);
            return this;
        }

        public Builder setSchemaTable(String schema, String table) {
            this.jobMetadataHook.setSchema(schema);
            this.jobMetadataHook.setTable(table);
            return this;
        }

    }
}
