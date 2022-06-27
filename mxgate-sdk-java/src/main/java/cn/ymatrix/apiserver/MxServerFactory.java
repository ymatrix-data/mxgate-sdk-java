package cn.ymatrix.apiserver;

import cn.ymatrix.builder.ServerConfig;

import java.security.InvalidParameterException;

public class MxServerFactory {
    /**
     * Get MxServer single instance.
     *
     * @return MxServer instance.
     */
    public static MxServer getMxServerInstance(ServerConfig serverConfig) throws InvalidParameterException, NullPointerException {
        return MxServerInstance.getInstance(serverConfig);
    }
}
