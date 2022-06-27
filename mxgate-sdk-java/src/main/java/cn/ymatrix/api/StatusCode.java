package cn.ymatrix.api;

public enum StatusCode {
    NORMAL, // Success
    ERROR, // Request error
    PARTIALLY_TUPLES_FAIL, // Partially tuples insert fail.

    ALL_TUPLES_FAIL // All tuples insert fail.
}
