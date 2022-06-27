package cn.ymatrix.builder;

import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.apiclient.ResultStatus;
import cn.ymatrix.apiserver.MxServerFactory;
import cn.ymatrix.cache.CacheFactory;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.messagecenter.ResultMessageCenter;
import cn.ymatrix.messagecenter.ResultMessageQueue;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestResultMessageRegister {
    Logger l = MxLogger.init(TestResultMessageRegister.class);

    @Test
    public void TestResultMessageRegisterConstructor() {
        try {
            MxClientImpl client = TestMxClient.prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(TestMxClient.prepareServerConfig()), "TestResultMessageRegisterConstructor");

            String className = "cn.ymatrix.builder.MxClientImpl$ResultMessageRegister";
            Class<?> innerClazz = Class.forName(className);
            Constructor<?> constructor = innerClazz.getDeclaredConstructor(MxClientImpl.class);
            constructor.setAccessible(true);
            Object o = constructor.newInstance(client);
            Assert.assertEquals(className, o.getClass().getName());

            Method method = o.getClass().getDeclaredMethod("register", ResultMessageQueue.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);

            String clientName = "public" + "." + "test_table_register_constructor";
            ResultMessageQueue<Result> queue = ResultMessageCenter.getSingleInstance().register(clientName);
            Result result = new Result();
            result.setStatus(ResultStatus.SUCCESS);
            queue.add(result);

            final boolean[] onSuccessCallback = new boolean[1];
            CountDownLatch latch = new CountDownLatch(1);

            client.registerDataPostListener(new DataPostListener() {
                @Override
                public void onSuccess(Result result) {
                    onSuccessCallback[0] = true;
                    latch.countDown();
                }

                @Override
                public void onFailure(Result result) {
                    onSuccessCallback[0] = false;
                    latch.countDown();
                }
            });

            method.invoke(o, queue);
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(onSuccessCallback[0]);

            // UnRegister
            Method unRegister = o.getClass().getDeclaredMethod("unRegister");
            Assert.assertNotNull(unRegister);
            unRegister.setAccessible(true);
            unRegister.invoke(o);

        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
