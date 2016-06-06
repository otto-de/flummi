package de.otto.elasticsearch.client.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.otto.sluggify.Sluggify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Strings.isNullOrEmpty;

public class StringUtils {

    private StringUtils() {}

    private static LoadingCache<String,String> sluggifyCategoryPathCache = CacheBuilder.newBuilder()
            .maximumSize(20000)
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String key) throws Exception {
                    return Sluggify.sluggifyPath(key, ">", "/");
                }
            });

    public static String sluggifyCategoryPath(final String categoriesPath) {
        try {
            return sluggifyCategoryPathCache.get(categoriesPath);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> splitAndTrim(String value, String regex) {
        List<String> result = new ArrayList<>();
        if (!isNullOrEmpty(value)) {
            String[] segements = value.split(regex);
            for (String segment : segements) {
                if (!isNullOrEmpty(segment)) {
                    result.add(segment.trim());
                }
            }
        }
        return result;
    }

    public static boolean isEmpty(final String stringToCheck) {
        return stringToCheck == null || stringToCheck.length() == 0;
    }

}
