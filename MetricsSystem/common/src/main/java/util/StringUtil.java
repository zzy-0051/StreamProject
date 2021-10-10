package util;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class StringUtil {

    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isContains(String str1, String str2) {
        return str1.contains(str2);
    }

    public static boolean isNotContains(String str1, String str2) {
        return !isContains(str1, str2);
    }

}
