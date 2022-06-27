package cn.ymatrix.api;

public class MetadataHook {
    private int num;

    private String type;

    private String name;

    private MetadataHook() {

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private void setNum(int num) {
        this.num = num;
    }

    private void setType(String type) {
        this.type = type;
    }

    private void setName(String name) {
        this.name = name;
    }

    public int getNum() {
        return num;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public static class Builder {
        private final MetadataHook metadataHook;

        public Builder() {
            this.metadataHook = new MetadataHook();
        }

        public MetadataHook Build() {
            return this.metadataHook;
        }

        public Builder setName(String name) {
            this.metadataHook.setName(name);
            return this;
        }

        public Builder setType(String type) {
            this.metadataHook.setType(type);
            return this;
        }

        public Builder setNum(int num) {
            this.metadataHook.setNum(num);
            return this;
        }
    }

}
