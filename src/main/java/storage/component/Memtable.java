package storage.component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author nedis
 * @since 1.0
 */
public final class Memtable {

    private final Map<String, String> map = new HashMap<>();

    public void put(final String key, final String value) {
        if (value == null) {
            map.remove(key);
        } else {
            map.put(key, value);
        }
    }

    public String get(final String key) {
        return map.get(key);
    }

    public void remove(final String key) {
        map.remove(key);
    }

    public Set<Map.Entry<String, String>> entrySet(){
        return map.entrySet();
    }
}
