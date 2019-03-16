import java.util.Collection;

/**
 * @author ZJY
 * @ClassName: UtilCompare
 * @Description: UtilCompare
 * @date 2019/3/16 15:03
 */
public class UtilCompare {
    public static boolean isEmpty(String str) {
        return (str == null || "".equals(str.trim()));
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    @SuppressWarnings("rawtypes")
    public static boolean isNotEmpty(Collection list) {
        return list != null && list.size() > 0;
    }

    @SuppressWarnings("rawtypes")
    public static boolean isEmpty(Collection list) {
        return list == null || list.size() == 0;
    }
}
