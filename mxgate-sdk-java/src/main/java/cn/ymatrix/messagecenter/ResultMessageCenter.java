package cn.ymatrix.messagecenter;

import cn.ymatrix.apiclient.Result;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResultMessageCenter {
    private static final String TAG = StrUtil.logTagWrap(ResultMessageCenter.class.getName());
    private static final Logger l = MxLogger.init(ResultMessageCenter.class);
    private final Map<String, ResultMessageQueue<Result>> messageCenterMap;

    public static ResultMessageCenter getSingleInstance() {
        return center.instance;
    }

    private ResultMessageCenter() {
        this.messageCenterMap = new ConcurrentHashMap<>();
        l.info("{} Create ResultMessageCenter single instance.", TAG);
    }

    private static class center {
        private static final ResultMessageCenter instance = new ResultMessageCenter();
    }

    /**
     * Multiple clients may register the result message from this MessageCenter.
     * @param clientName, the key to identify each client.
     * @return ResultMessageQueue to register.
     */
    public synchronized ResultMessageQueue<Result> register(String clientName) throws NullPointerException {
        if (StrUtil.isNullOrEmpty(clientName)) {
            throw new NullPointerException("clientName is empty when try to register Result message from the MessageCenter.");
        }
        if (this.messageCenterMap.get(clientName) != null) {
            return this.messageCenterMap.get(clientName);
        }
        ResultMessageQueue<Result> queue = new ResultMessageQueue<>();
        this.messageCenterMap.put(clientName, queue);
        l.info("{} Register MessageQueue from MessageCenter for MxClient({}) .", TAG, clientName);
        return queue;
    }

    public synchronized void unRegister(String clientName) throws NullPointerException {
        if (StrUtil.isNullOrEmpty(clientName)) {
            throw new NullPointerException("clientName is empty when try to register Result message from the MessageCenter.");
        }
        this.messageCenterMap.remove(clientName);
        l.info("{} Unregister MessageQueue from MessageCenter for MxClient({}) .", TAG, clientName);
    }

    public ResultMessageQueue<Result> fetch(String clientName) {
        return this.messageCenterMap.get(clientName);
    }

}
