package cn.ymatrix.httpclient;

import cn.ymatrix.exception.BrokenTuplesException;
import cn.ymatrix.logger.MxLogger;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.Assert;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MockHttpServer {
    public static class RootHandler implements HttpHandler {

        private static final Logger l = MxLogger.init(RootHandler.class);

        private int responseCode;

        private String responseBody;

        private Content content;

        private CompressDataDecoder decoder;

        public RootHandler(int code, String body) {
            this.responseCode = code;
            this.responseBody = body;
            this.content = new Content();
        }

        public RootHandler withProcessTime(int t) {
            this.content.setProcessTimeMillis(t);
            return this;
        }

        public RootHandler withProcessException(String m) {
            this.content.setE(m);
            return this;
        }

        public RootHandler withCompressDataDecoder(CompressDataDecoder decoder) {
            this.decoder = decoder;
            return this;
        }

        @Override
        public void handle(HttpExchange he) throws IOException {
            // parse request
            InputStreamReader isr = new InputStreamReader(he.getRequestBody(), StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String table = br.readLine();
            String data = br.readLine();

            if (this.decoder != null) {
                this.decoder.decode(data);
            }

            if (this.content.getProcessTimeMillis() > 0 && data != null && data.contains("slow")) {
                try {
                    Thread.sleep(this.content.getProcessTimeMillis());
                } catch (Exception e) {
                    l.error("Thread sleep exception ", e);
                }
            }

            if (this.content.getE() != null && data != null && data.contains("tuple broken")) {
                throw new BrokenTuplesException(this.content.getE().getMessage());
            }

            if (this.content.getE() != null && data != null && data.contains("exception")) {
                throw this.content.getE();
            }

            // send response
            he.sendResponseHeaders(this.responseCode, responseBody.length());
            OutputStream os = he.getResponseBody();
            os.write(responseBody.toString().getBytes());
            os.close();
        }
    }

    public static void startServerBlocking(final int port, final HttpHandler handler) {
        CountDownLatch latch = new CountDownLatch(1);
        Thread serverThread = new Thread() {
            @Override
            public void run() {
                startHttpServer(port, handler);
                latch.countDown();
            }
        };
        serverThread.start();

        try {
            boolean wait = latch.await(3, TimeUnit.SECONDS);
            Assert.assertTrue(wait);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void startHttpServer(int port, HttpHandler handler) {
        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", handler);
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Content {
        private int processTimeMillis;
        private RuntimeException e;

        public Content() {
        }

        public void setProcessTimeMillis(int m) {
            this.processTimeMillis = m;
        }

        public void setE(String m) {
            this.e = new RuntimeException(m);
        }

        public int getProcessTimeMillis() {
            return this.processTimeMillis;
        }

        public RuntimeException getE() {
            return this.e;
        }
    }
}
