package cn.ymatrix.api;

import java.util.Map;

public class SendDataHook {
    private int responseCode;
    private String message;
    private int tupleCount;
    private int sleepMillis;

    private CompressDataDecoder decoder;

    private RuntimeException eTest;
    private Map<Long, String> errorLines;

    private SendDataHook() {
        responseCode = MxGrpcClient.GRPC_RESPONSE_CODE_OK;
    }

    private void setResponseCode(int c) {
        this.responseCode = c;
    }

    private void setMessage(String m) {
        message = m;
    }

    private void setTupleCount(int c) {
        tupleCount = c;
    }

    private void setErrorLines(Map<Long, String> e) {
        errorLines = e;
    }

    private void setSleepMillis(int t) {sleepMillis = t;}

    private void setException(String m) {eTest = new RuntimeException(m);}

    public int getResponseCode() {
        return responseCode;
    }

    public String getMessage() {
        return message;
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public int getSleepMillis() {
        return sleepMillis;
    }

    public RuntimeException getException() {
        return eTest;
    }

    public Map<Long, String> getErrorLines() {
        return errorLines;
    }

    public CompressDataDecoder getDecoder() {
        return decoder;
    }

    public void setDecoder(CompressDataDecoder decoder) {
        this.decoder = decoder;
    }

    public static SendDataHook.Builder newBuilder() {
        return new SendDataHook.Builder();
    }

    public static class Builder {
        private final SendDataHook sendDataHook;

        public Builder() {
            sendDataHook = new SendDataHook();
        }

        public SendDataHook build() {
            return this.sendDataHook;
        }

        public SendDataHook.Builder setResponseCode(int responseCode) {
            this.sendDataHook.setResponseCode(responseCode);
            return this;
        }

        public SendDataHook.Builder setMessage(String m) {
            this.sendDataHook.setMessage(m);
            return this;
        }

        public SendDataHook.Builder setTupleCount(int c) {
            this.sendDataHook.setTupleCount(c);
            return this;
        }

        public SendDataHook.Builder setErrorLines(Map<Long, String> e) {
            this.sendDataHook.setErrorLines(e);
            return this;
        }

        public SendDataHook.Builder setSleepMillis(int t) {
            this.sendDataHook.setSleepMillis(t);
            return this;
        }

        public SendDataHook.Builder setException(String m) {
            this.sendDataHook.setException(m);
            return this;
        }

        public SendDataHook.Builder setCompressDataDecoder(CompressDataDecoder decoder) {
            this.sendDataHook.setDecoder(decoder);
            return this;
        }
    }
}
