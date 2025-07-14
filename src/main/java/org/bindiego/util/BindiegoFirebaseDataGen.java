package org.bindiego.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.LongSerializationPolicy;

import java.lang.reflect.Type;
import java.math.BigDecimal;

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

    private final Gson gson;

    public BindiegoFirebaseDataGen() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        
        gsonBuilder.registerTypeAdapter(Double.class, new JsonSerializer<Double>() {
            @Override
            public JsonElement serialize(Double src, Type typeOfSrc, JsonSerializationContext context) {
                if (src == src.longValue()) {
                    return new JsonPrimitive(src.longValue());
                } else {
                    return new JsonPrimitive(new BigDecimal(src));
                }
            }
        });
/*
        gsonBuilder.registerTypeAdapter(Long.class, new JsonSerializer<Long>() {
            @Override
            public JsonElement serialize(Long src, Type typeOfSrc, JsonSerializationContext context) {
                return new JsonPrimitive(src);
            }
        });

        gsonBuilder.registerTypeAdapter(Integer.class, new JsonSerializer<Integer>() {
            @Override
            public JsonElement serialize(Integer src, Type typeOfSrc, JsonSerializationContext context) {
                return new JsonPrimitive(src);
            }
        });
*/  
        gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);
        
        this.gson = gsonBuilder.create();
    }

    /**
     * Generates a single, random Firebase event as a JSON formatted string.
     * @return A JSON string representing a single event record.
     */
    public String generateRandomEvent() {
        JsonObject record = new JsonObject();
        // 1. Generate core event time and name
        LocalDate eventDate = LocalDate.now().minusDays(random.nextInt(30));
        long eventTimestamp = generateTimestampMicros(eventDate);
        String eventName = pickRandom(EVENT_NAMES);

        record.addProperty("event_date", eventDate.format(DATE_FORMATTER));
        record.addProperty("event_timestamp", String.valueOf(eventTimestamp));
        record.add("event_ts", JsonNull.INSTANCE); // Often derived during ETL, can be set to null.
        record.addProperty("event_name", eventName);

        // 2. Generate User Identifiers
        String userPseudoId = generateHexId(32).toUpperCase();
        String userId = generateHexId(32).toLowerCase();
        record.addProperty("user_pseudo_id", userPseudoId);
        if (random.nextBoolean()) {
            record.addProperty("user_id", userId);
        } else {
            record.add("user_id", JsonNull.INSTANCE);
        }

        // 3. Generate Event Parameters based on event_name
        record.add("event_params", generateEventParams(eventName, userPseudoId));

        // 4. Populate other top-level fields
        record.add("user_properties", generateUserProperties());
        record.add("device", generateDevice());
        record.add("geo", generateGeo());
        record.add("traffic_source", generateTrafficSource());
        record.add("app_info", generateAppInfo());
        record.addProperty("platform", pickRandom(PLATFORMS));
        record.addProperty("stream_id", "1047766415");

        // 5. Handle ecommerce data for 'purchase' events
        if ("purchase".equals(eventName)) {
            JsonArray items = generateItems();
            record.add("items", items);
            record.add("ecommerce", generateEcommerce(items));
        } else {
            record.add("items", new JsonArray());
            record.add("ecommerce", new JsonObject());
        }

        // Add other nullable fields with random or fixed values
        record.addProperty("event_previous_timestamp", String.valueOf(eventTimestamp - random.nextInt(5_000_000)));
        record.add("event_value_in_usd", JsonNull.INSTANCE);
        record.addProperty("event_bundle_sequence_id", String.valueOf(random.nextInt(10000)));
        record.addProperty("user_first_touch_timestamp", String.valueOf(eventTimestamp - random.nextInt(100_000_000)));
        JsonObject privacyInfo = new JsonObject();
        privacyInfo.addProperty("analytics_storage", "Yes");
        privacyInfo.addProperty("ads_storage", "Yes");
        record.add("privacy_info", privacyInfo);
        return gson.toJson(record);
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

    private JsonArray generateEventParams(String eventName, String userPseudoId) {
        JsonArray params = new JsonArray();
        long sessionId = Math.abs(userPseudoId.hashCode());
        int sessionNumber = random.nextInt(100) + 1;

        params.add(createParam("ga_session_id", "int_value", sessionId));
        params.add(createParam("ga_session_number", "int_value", sessionNumber));
        params.add(createParam("engaged_session_event", "int_value", 1));

        switch (eventName) {
            case "level_start":
                params.add(createParam("level_name", "string_value", "level_" + (random.nextInt(50) + 1)));
                break;
            case "level_complete":
                params.add(createParam("level_name", "string_value", "level_" + (random.nextInt(50) + 1)));
                params.add(createParam("score", "int_value", random.nextInt(100000)));
                break;
            case "level_fail":
                params.add(createParam("level_name", "string_value", "level_" + (random.nextInt(50) + 1)));
                break;
            case "ad_impression":
            case "ad_click":
                params.add(createParam("ad_format", "string_value", pickRandom(AD_FORMATS)));
                params.add(createParam("ad_source", "string_value", pickRandom(AD_SOURCES)));
                params.add(createParam("ad_placement", "string_value", pickRandom(new String[]{"end_of_level", "store_boost"})));
                break;
            case "screen_view":
                params.add(createParam("firebase_screen_name", "string_value", pickRandom(SCREEN_NAMES)));
                params.add(createParam("firebase_screen_class", "string_value", "UnityViewController"));
                break;
            case "purchase":
                params.add(createParam("currency", "string_value", "USD"));
                params.add(createParam("value", "double_value", Math.round((random.nextDouble() * 100) * 100.0) / 100.0));
                params.add(createParam("transaction_id", "string_value", "T" + generateHexId(10)));
                break;
            case "user_engagement":
                params.add(createParam("engagement_time_msec", "int_value", random.nextInt(30000) + 1000));
                break;
        }
        return params;
    }

    private JsonObject createParam(String key, String valueType, Object value) {
        JsonObject param = new JsonObject();
        JsonObject valueObject = new JsonObject();
        if (value instanceof Double) {
            valueObject.addProperty(valueType, new BigDecimal((Double) value).toPlainString());
        } else if (value instanceof Float) {
            valueObject.addProperty(valueType, new BigDecimal((Float) value).toPlainString());
        } else {
            valueObject.addProperty(valueType, String.valueOf(value));
        }
        param.addProperty("key", key);
        param.add("value", valueObject);
        return param;
    }
    
    private JsonArray generateUserProperties() {
        JsonArray properties = new JsonArray();
        properties.add(createParam("player_level", "int_value", random.nextInt(100) + 1));
        properties.add(createParam("coins_balance", "int_value", random.nextInt(50000)));
        properties.add(createParam("is_spender", "string_value", String.valueOf(random.nextBoolean())));
        return properties;
    }

    private JsonObject generateDevice() {
        JsonObject device = new JsonObject();
        boolean isIOS = "IOS".equals(pickRandom(PLATFORMS));
        device.addProperty("category", pickRandom(new String[]{"mobile", "tablet"}));
        device.addProperty("mobile_brand_name", isIOS ? "Apple" : pickRandom(new String[]{"Samsung", "Google", "OnePlus"}));
        device.addProperty("operating_system", isIOS ? "iOS" : "Android");
        device.addProperty("operating_system_version", isIOS ? "16.5.1" : "13.0");
        device.addProperty("language", pickRandom(new String[]{"en-us", "en-gb", "de-de", "ja-jp"}));
        device.addProperty("is_limited_ad_tracking", String.valueOf(random.nextBoolean()));
        return device;
    }

    private JsonObject generateGeo() {
        JsonObject geo = new JsonObject();
        int index = random.nextInt(COUNTRIES.length);
        geo.addProperty("country", COUNTRIES[index]);
        geo.addProperty("city", CITIES[index]);
        geo.addProperty("continent", "Americas"); // Simplified for example
        geo.add("region", JsonNull.INSTANCE);
        return geo;
    }

    private JsonObject generateTrafficSource() {
        JsonObject ts = new JsonObject();
        ts.addProperty("name", "(direct)");
        ts.addProperty("medium", "(none)");
        ts.addProperty("source", "(direct)");
        return ts;
    }
    
    private JsonObject generateAppInfo() {
        JsonObject app = new JsonObject();
        app.addProperty("id", "com.redhotlabs.bingo");
        app.addProperty("version", "2.1.0");
        app.addProperty("install_source", pickRandom(new String[]{"iTunes", "com.android.vending"}));
        app.addProperty("firebase_app_id", "1:676333365279:ios:4916161d30139c63");
        return app;
    }

    private JsonArray generateItems() {
        JsonArray items = new JsonArray();
        int itemCount = random.nextInt(3) + 1; // 1 to 3 items per purchase
        for (int i = 0; i < itemCount; i++) {
            JsonObject item = new JsonObject();
            double price = Math.round((random.nextDouble() * 20 + 0.99) * 100.0) / 100.0;
            int quantity = random.nextInt(2) + 1;
            item.addProperty("item_id", generateHexId(8));
            item.addProperty("item_name", pickRandom(ITEM_NAMES));
            item.addProperty("item_brand", "in-game");
            item.addProperty("item_category", "virtual_good");
            item.addProperty("price", new BigDecimal(price).toPlainString());
            item.addProperty("price_in_usd", new BigDecimal(price).toPlainString());
            item.addProperty("quantity", String.valueOf(quantity));
            item.addProperty("item_revenue", new BigDecimal(price * quantity).toPlainString());
            items.add(item);
        }
        return items;
    }
    
    private JsonObject generateEcommerce(JsonArray items) {
        JsonObject ecommerce = new JsonObject();
        double totalRevenue = 0;
        int totalQuantity = 0;
        for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.get(i).getAsJsonObject();
            totalRevenue += Double.parseDouble(item.get("item_revenue").getAsString());
            totalQuantity += Integer.parseInt(item.get("quantity").getAsString());
        }
        ecommerce.addProperty("total_item_quantity", String.valueOf(totalQuantity));
        ecommerce.addProperty("purchase_revenue", new BigDecimal(totalRevenue).toPlainString());
        ecommerce.addProperty("purchase_revenue_in_usd", new BigDecimal(totalRevenue).toPlainString());
        ecommerce.addProperty("transaction_id", "T" + generateHexId(10));
        return ecommerce;
    }

    private <T> T pickRandom(T[] array) {
        return array[random.nextInt(array.length)];
    }
}
