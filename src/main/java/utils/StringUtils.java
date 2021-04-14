package utils;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StringUtils {
    public static List<Integer> convertIntegerListString(String integerList) {
        List<Integer> res = new ArrayList<>();
        for (String sInteger : integerList.split(",")) res.add(Integer.parseInt(sInteger));
        return res;
    }

    public static String getSHA256Hash(String inputString) {
        return Hashing.sha256()
                .hashString(inputString, StandardCharsets.UTF_8)
                .toString();
    }

}
