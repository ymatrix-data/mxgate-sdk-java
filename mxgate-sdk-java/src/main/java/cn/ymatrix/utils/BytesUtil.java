package cn.ymatrix.utils;

public class BytesUtil {

    public static byte[] subByte(byte[] b, int off, int length) {
        byte[] ret = new byte[length];
        System.arraycopy(b, off, ret, 0, length);
        return ret;
    }


}
