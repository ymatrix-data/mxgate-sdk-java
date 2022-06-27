package cn.ymatrix.compress;

public interface Compressor {
    byte[] compress(byte[] src);

    byte[] decompress(byte[] src, int length);
}
