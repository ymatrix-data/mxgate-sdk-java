package cn.ymatrix.data;

import cn.ymatrix.utils.StrUtil;

public class ColumnsFactory {
    /**
     * Generate int point.
     */
    public static Column<Integer> columnInt(int i) {
        return new ColumnImpl<Integer>(i) {
            @Override
            public String toCSV() {
                Integer t = this.getValue();
                String strValue = Integer.toString(t);
                return StrUtil.quote(strValue);
            }
            @Override
            public String toString() {
                return String.valueOf(this.getValue());
            }
        };
    }

    /**
     * Generate String point.
     */
    public static Column<String> columnString(String s) {
        return new ColumnImpl<String>(s) {
            @Override
            public String toCSV() {
                String s = this.getValue();
                return StrUtil.quote(s);
            }

            @Override
            public String toString() {
                return this.getValue();
            }
        };
    }

    /**
     * Generate float point.
     */
    public static Column<Float> columnFloat(float f) {
        return new ColumnImpl<Float>(f) {
            @Override
            public String toCSV() {
                Float f = this.getValue();
                String strFloat = Float.toString(f);
                return StrUtil.quote(strFloat);
            }
            @Override
            public String toString() {
                return String.valueOf(this.getValue());
            }
        };
    }

    /**
     * Generate double type point.
     */
    public static Column<Double> columnDouble(double d) {
        return new ColumnImpl<Double>(d) {
            @Override
            public String toCSV() {
                Double d = this.getValue();
                String strDouble = Double.toString(d);
                return StrUtil.quote(strDouble);
            }
            @Override
            public String toString() {
                return String.valueOf(this.getValue());
            }
        };
    }

    /**
     * Generate raw point which can contain any object.
     */
    public static Column columnRaw(Object o) {
        return new ColumnImpl(o) {
            @Override
            public String toCSV() {
                Object obj = this.getValue();
                if (obj == null) {
                    return StrUtil.quote("");
                }
                return StrUtil.quote(String.valueOf(obj));
            }
            @Override
            public String toString() {
                if (this.getValue() == null) {
                    return ""; // Return an empty string.
                }
                return String.valueOf(this.getValue());
            }

        };
    }

}
