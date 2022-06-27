package cn.ymatrix.api;

import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import java.util.concurrent.ConcurrentHashMap;

public class MxGrpcClientManager {

    // Different table has different MxGrpcClient (table_name -> MxGrpcClient)
    private final ConcurrentHashMap<String, MxGrpcClient> clientMap;

    public static MxGrpcClientManager getInstance() {
        return MxGrpcClientManagerSingleton.instance;
    }

    private MxGrpcClientManager() {
        clientMap = new ConcurrentHashMap<>();
    }

    public MxGrpcClient prepareClient(String gRPCHost, String schema, String table, int timeoutMillis,
                                      CircuitBreakerConfig circuitBreakerConfig, CSVConstructor constructor) throws NullPointerException {
        String key = StrUtil.connect(gRPCHost, schema, table);
        if (this.clientMap.get(key) != null) {
            return this.clientMap.get(key);
        }
        MxGrpcClient client = MxGrpcClient.prepareMxGrpcClient(gRPCHost, schema, table, timeoutMillis, circuitBreakerConfig, constructor);
        this.clientMap.put(key, client);
        return client;
    }

    public MxGrpcClient getClientIfExist(String gRPCHost, String schema, String table) {
        String key = StrUtil.connect(gRPCHost, schema, table);
        if (this.clientMap.get(key) != null) {
            return this.clientMap.get(key);
        }
        return null;
    }

    private static class MxGrpcClientManagerSingleton {
        private static final MxGrpcClientManager instance = new MxGrpcClientManager();
    }
}
