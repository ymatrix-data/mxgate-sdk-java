package cn.ymatrix.compress;


import cn.ymatrix.logger.MxLogger;
import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;

public class CompressionFactory {

    public static Compressor getCompressor() {
        return new InnerCompressor();
    }

    private static class InnerCompressor implements Compressor {
        private static final Logger l = MxLogger.init(InnerCompressor.class);

        InnerCompressor() {
        }

        @Override
        public byte[] compress(byte[] src) {
            return this.compressZstd(src);
        }

        @Override
        public byte[] decompress(byte[] src, int length) {
            return decompressZstd(src, length);
        }

        private byte[] compressZstd(byte[] src) {
            return Zstd.compress(src);
        }

        private byte[] decompressZstd(byte[] src, int length) {
            byte[] dest = new byte[length];
            long code = Zstd.decompress(dest, src);
            if (Zstd.isError(code)) {
                throw new RuntimeException("Zstd decompress exception and the error code is " + code);
            }
            return dest;
        }
    }



}
