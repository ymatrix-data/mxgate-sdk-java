package Tools;

public class PrintColor {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static void RedPrint(String msg) {
        ColorPrint(ANSI_RED, msg);
    }

    public static void GreenPrint(String msg) {
        ColorPrint(ANSI_GREEN, msg);
    }

    public static void YellowPrint(String msg) {
        ColorPrint(ANSI_YELLOW, msg);
    }

    public static void BluePrint(String msg) {
        ColorPrint(ANSI_BLUE, msg);
    }

    private static void ColorPrint(String color, String msg) {
        System.out.println(color+msg+ANSI_RESET);
    }
}
