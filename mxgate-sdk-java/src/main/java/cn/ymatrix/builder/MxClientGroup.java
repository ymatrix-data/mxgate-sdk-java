package cn.ymatrix.builder;

import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiserver.MxServer;
import cn.ymatrix.cache.Cache;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

class MxClientGroup {
    private final List<MxClient> clientList;
    private final MxServer server;
    private final Cache cache;
    private final int groupNum;

    MxClientGroup(MxServer server, Cache cache, int groupNum) throws NullPointerException {
        if (server == null) {
            throw new NullPointerException("MxServer instance is null in MxClientGroup construction.");
        }
        if (cache == null) {
            throw new NullPointerException("Cache instance is null in MxClientGroup construction.");
        }
        if (groupNum <= 0) {
            throw new InvalidParameterException("GroupNumber must be positive.");
        }
        clientList = new ArrayList<>();
        this.server = server;
        this.cache = cache;
        this.groupNum = groupNum;
    }

    synchronized void addClient(MxClient client) throws NullPointerException {
        if (client == null) {
            throw new NullPointerException("Nullable MxClient should not be added into MxClient group list.");
        }
        this.clientList.add(client);
    }

    public List<MxClient> getClientList() {
        return clientList;
    }

    public MxServer getServer() {
        return server;
    }

    public Cache getCache() {
        return cache;
    }

    public int getGroupNum() {
        return groupNum;
    }
}
