package org.bindiego.util;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class BindiegoFirebaseDataGen {

    private static final SecureRandom random = new SecureRandom();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    // Data pools for realistic values
    private static final String[] EVENT_NAMES = {
        "session_start", "user_engagement", "screen_view", "level_start",
        "level_complete", "level_fail", "ad_impression", "ad_click", "purchase"
    };
    private static final String[] PLATFORMS = {"IOS", "ANDROID"};
    private static final String[] COUNTRIES = {"United States", "India", "Brazil", "Germany", "United Kingdom", "Japan"};
    private static final String[] CITIES = {"New York", "Mumbai", "Sao Paulo", "Berlin", "London", "Tokyo"};
    private static final String[] AD_FORMATS = {"rewarded_video", "interstitial", "banner"};
    private static final String[] AD_SOURCES = {"admob", "meta_audience_network", "applovin"};
    private static final String[] ITEM_NAMES = {"gold_pack_small", "gem_bundle_medium", "starter_kit", "special_offer_epic"};
    private static final String[] SCREEN_NAMES = {"main_menu", "shop", "level_selector", "gameplay_hud", "settings"};

    /**
     * Generates a single, random Firebase event as a JSON formatted string.
     * @return A JSON string representing a single event record.
     */
    public String generateRandomEvent() {
        JSONObject record = new JSONObject();
        try {
            // 1. Generate core event time and name
            LocalDate eventDate = LocalDate.now().minusDays(random.nextInt(30));
            long eventTimestamp = generateTimestampMicros(eventDate);
            String eventName = pickRandom(EVENT_NAMES);

            record.put("event_date", eventDate.format(DATE_FORMATTER));
            record.put("event_timestamp", eventTimestamp);
            record.put("event_ts", JSONObject.NULL); // Often derived during ETL, can be set to null.
            record.put("event_name", eventName);

            // 2. Generate User Identifiers
            String userPseudoId = generateHexId(32).toUpperCase();
            String userId = generateHexId(32).toLowerCase();
            record.put("user_pseudo_id", userPseudoId);
            record.put("user_id", random.nextBoolean() ? userId : JSONObject.NULL);

            // 3. Generate Event Parameters based on event_name
            record.put("event_params", generateEventParams(eventName, userPseudoId));

            // 4. Populate other top-level fields
            record.put("user_properties", generateUserProperties());
            record.put("device", generateDevice());
            record.put("geo", generateGeo());
            record.put("traffic_source", generateTrafficSource());
            record.put("app_info", generateAppInfo());
            record.put("platform", pickRandom(PLATFORMS));
            record.put("stream_id", "1047766415");

            // 5. Handle ecommerce data for 'purchase' events
            if ("purchase".equals(eventName)) {
                JSONArray items = generateItems();
                record.put("items", items);
                record.put("ecommerce", generateEcommerce(items));
            } else {
                record.put("items", new JSONArray());
                record.put("ecommerce", new JSONObject());
            }

            // Add other nullable fields with random or fixed values
            record.put("event_previous_timestamp", eventTimestamp - random.nextInt(5_000_000));
            record.put("event_value_in_usd", JSONObject.NULL);
            record.put("event_bundle_sequence_id", random.nextInt(10000));
            record.put("user_first_touch_timestamp", eventTimestamp - random.nextInt(100_000_000));
            record.put("privacy_info", new JSONObject().put("analytics_storage", "Yes").put("ads_storage", "Yes"));
        } catch (JSONException e) {
            // Ignore exception
        }
        return record.toString();
    }
    
    // --- Helper methods to generate different parts of the schema ---

    private long generateTimestampMicros(LocalDate date) {
        long dayStartSeconds = date.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        long randomSecondsInDay = ThreadLocalRandom.current().nextLong(24 * 60 * 60);
        return (dayStartSeconds + randomSecondsInDay) * 1_000_000L + random.nextInt(1_000_000);
    }
    
    private String generateHexId(int length) {
        byte[] bytes = new byte[length / 2];
        random.nextBytes(bytes);
        StringBuilder sb = new StringBuilder(length);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private JSONArray generateEventParams(String eventName, String userPseudoId) {
        JSONArray params = new JSONArray();
        try {
            long sessionId = Math.abs(userPseudoId.hashCode());
            int sessionNumber = random.nextInt(100) + 1;

            params.put(createParam("ga_session_id", "int_value", sessionId));
            params.put(createParam("ga_session_number", "int_value", sessionNumber));
            params.put(createParam("engaged_session_event", "int_value", 1));

            switch (eventName) {
                case "level_start":
                    params.put(createParam("level_name", "string_value", "level_" + (random.nextInt(50) + 1)));
                    break;
                case "level_complete":
                    params.put(createParam("level_name", "string_value", "level_" + (random.nextInt(50) + 1)));
                    params.put(createParam("score", "int_value", random.nextInt(100000)));
                    break;
                case "level_fail":
                    params.put(createParam("level_name", "string_value", "level_" + (random.nextInt(50) + 1)));
                    break;
                case "ad_impression":
                case "ad_click":
                    params.put(createParam("ad_format", "string_value", pickRandom(AD_FORMATS)));
                    params.put(createParam("ad_source", "string_value", pickRandom(AD_SOURCES)));
                    params.put(createParam("ad_placement", "string_value", pickRandom(new String[]{"end_of_level", "store_boost"})));
                    break;
                case "screen_view":
                    params.put(createParam("firebase_screen_name", "string_value", pickRandom(SCREEN_NAMES)));
                    params.put(createParam("firebase_screen_class", "string_value", "UnityViewController"));
                    break;
                case "purchase":
                    params.put(createParam("currency", "string_value", "USD"));
                    params.put(createParam("value", "double_value", Math.round((random.nextDouble() * 100) * 100.0) / 100.0));
                    params.put(createParam("transaction_id", "string_value", "T" + generateHexId(10)));
                    break;
                case "user_engagement":
                    params.put(createParam("engagement_time_msec", "int_value", random.nextInt(30000) + 1000));
                    break;
            }
        } catch (Exception e) {
            // Ignore exception
        }
        return params;
    }

    private JSONObject createParam(String key, String valueType, Object value) {
        JSONObject param = new JSONObject();
        try {
            param.put("key", key);
            param.put("value", new JSONObject().put(valueType, value));
        } catch (JSONException e) {
            // Ignore exception
        }
        return param;
    }
    
    private JSONArray generateUserProperties() {
        JSONArray properties = new JSONArray();
        try {
            properties.put(createParam("player_level", "int_value", random.nextInt(100) + 1));
            properties.put(createParam("coins_balance", "int_value", random.nextInt(50000)));
            properties.put(createParam("is_spender", "string_value", String.valueOf(random.nextBoolean())));
        } catch (Exception e) {
            // Ignore exception
        }
        return properties;
    }

    private JSONObject generateDevice() {
        JSONObject device = new JSONObject();
        try {
            boolean isIOS = "IOS".equals(pickRandom(PLATFORMS));
            device.put("category", pickRandom(new String[]{"mobile", "tablet"}));
            device.put("mobile_brand_name", isIOS ? "Apple" : pickRandom(new String[]{"Samsung", "Google", "OnePlus"}));
            device.put("operating_system", isIOS ? "iOS" : "Android");
            device.put("operating_system_version", isIOS ? "16.5.1" : "13.0");
            device.put("language", pickRandom(new String[]{"en-us", "en-gb", "de-de", "ja-jp"}));
            device.put("is_limited_ad_tracking", String.valueOf(random.nextBoolean()));
        } catch (JSONException e) {
            // Ignore exception
        }
        return device;
    }

    private JSONObject generateGeo() {
        JSONObject geo = new JSONObject();
        try {
            int index = random.nextInt(COUNTRIES.length);
            geo.put("country", COUNTRIES[index]);
            geo.put("city", CITIES[index]);
            geo.put("continent", "Americas"); // Simplified for example
            geo.put("region", JSONObject.NULL);
        } catch (JSONException e) {
            // Ignore exception
        }
        return geo;
    }

    private JSONObject generateTrafficSource() {
        JSONObject ts = new JSONObject();
        try {
            ts.put("name", "(direct)");
            ts.put("medium", "(none)");
            ts.put("source", "(direct)");
        } catch (JSONException e) {
            // Ignore exception
        }
        return ts;
    }
    
    private JSONObject generateAppInfo() {
        JSONObject app = new JSONObject();
        try {
            app.put("id", "com.redhotlabs.bingo");
            app.put("version", "2.1.0");
            app.put("install_source", pickRandom(new String[]{"iTunes", "com.android.vending"}));
            app.put("firebase_app_id", "1:676333365279:ios:4916161d30139c63");
        } catch (JSONException e) {
            // Ignore exception
        }
        return app;
    }

    private JSONArray generateItems() {
        JSONArray items = new JSONArray();
        try {
            int itemCount = random.nextInt(3) + 1; // 1 to 3 items per purchase
            for (int i = 0; i < itemCount; i++) {
                JSONObject item = new JSONObject();
                double price = Math.round((random.nextDouble() * 20 + 0.99) * 100.0) / 100.0;
                int quantity = random.nextInt(2) + 1;
                item.put("item_id", generateHexId(8));
                item.put("item_name", pickRandom(ITEM_NAMES));
                item.put("item_brand", "in-game");
                item.put("item_category", "virtual_good");
                item.put("price", price);
                item.put("price_in_usd", price);
                item.put("quantity", quantity);
                item.put("item_revenue", price * quantity);
                items.put(item);
            }
        } catch (JSONException e) {
            // Ignore exception
        }
        return items;
    }
    
    private JSONObject generateEcommerce(JSONArray items) {
        JSONObject ecommerce = new JSONObject();
        try {
            double totalRevenue = 0;
            int totalQuantity = 0;
            for (int i = 0; i < items.length(); i++) {
                JSONObject item = items.getJSONObject(i);
                totalRevenue += item.getDouble("item_revenue");
                totalQuantity += item.getInt("quantity");
            }
            ecommerce.put("total_item_quantity", totalQuantity);
            ecommerce.put("purchase_revenue", totalRevenue);
            ecommerce.put("purchase_revenue_in_usd", totalRevenue);
            ecommerce.put("transaction_id", "T" + generateHexId(10));
        } catch (JSONException e) {
            // Ignore exception
        }
        return ecommerce;
    }

    private <T> T pickRandom(T[] array) {
        return array[random.nextInt(array.length)];
    }
}
