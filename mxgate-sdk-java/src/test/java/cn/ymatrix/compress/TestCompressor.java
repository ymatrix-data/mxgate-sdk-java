package cn.ymatrix.compress;

import cn.ymatrix.logger.MxLogger;
import com.github.luben.zstd.Zstd;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestCompressor {
    private static final Logger l = MxLogger.init(TestCompressor.class);

    @Test
    public void TestCompress() {
        try {
            String srcStr = "\"2022-05-18 16:30:06\"|\"102020030\"|\"244\"|\"245\"|\"246\"|\"247\"|\"CAPAC243\"|\"lTxFCVLwcDTKbNbjau_c6243\"|\"lTxFCVLwcDTKbNbjau_c7243\"|\"lTxFCVLwcDTKbNbjau_c8243\"";
            Compressor compressor = CompressionFactory.getCompressor();
            byte[] compressedByte = compressor.compress(srcStr.getBytes(UTF_8));
            l.info("compressed byte length {}", compressedByte.length);
            byte[] afterEncode = Zstd.compress(srcStr.getBytes(UTF_8));
            Assert.assertEquals(compressedByte.length, afterEncode.length);
            for (int i = 0; i < compressedByte.length; i++) {
                Assert.assertEquals(compressedByte[i], afterEncode[i]);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestZstd() {
        try {
            String strToEncode = "\"2022-05-18 16:30:06\"|\"102020030\"|\"lTxFCVLwcDTKbNbjau_c6243\"|\"lTxFCVLwcDTKbNbjau_c7243\"|\"lTxFCVLwcDTKbNbjau_c8243\"";
            byte[] afterEncode = Zstd.compress(strToEncode.getBytes());
            l.info("{}", afterEncode);
            byte[] dest = new byte[strToEncode.getBytes().length];
            Zstd.decompress(dest, afterEncode);
            Assert.assertEquals(new String(dest), strToEncode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestDecompress() {
        try {
            String srcStr = "\"2022-05-18 16:30:06\"|\"102020030\"|\"244\"|\"245\"|\"246\"|\"247\"|\"CAPAC243\"|\"lTxFCVLwcDTKbNbjau_c6243\"|\"lTxFCVLwcDTKbNbjau_c7243\"|\"lTxFCVLwcDTKbNbjau_c8243\"";
            Compressor compressor = CompressionFactory.getCompressor();
            byte[] compressedByte = compressor.compress(srcStr.getBytes(UTF_8));
            l.info("compressed byte length {}", compressedByte.length);
            byte[] afterEncode = Zstd.compress(srcStr.getBytes(UTF_8));
            Assert.assertEquals(compressedByte.length, afterEncode.length);
            for (int i = 0; i < compressedByte.length; i++) {
                Assert.assertEquals(compressedByte[i], afterEncode[i]);
            }
            byte[] deCompressedBytes = compressor.decompress(compressedByte, srcStr.getBytes(UTF_8).length);
            l.info("decompressed string {}", new String(deCompressedBytes));
            Assert.assertEquals(new String(deCompressedBytes), srcStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
