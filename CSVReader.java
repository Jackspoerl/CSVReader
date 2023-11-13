import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class CSVReader {

    public static void main(String[] args) {
        String pathToCsv = "service-names-port-numbers.csv"; // replace with your CSV file path
        String line;

        Map<Integer, String> portMap = new HashMap<>(); // Create a new map for Redis
        TreeSet<Integer> sortedKeys = new TreeSet<>(); // Create a set to store sorted keys

        try (BufferedReader br = new BufferedReader(new FileReader(pathToCsv))) {
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    // Skip empty lines
                    continue;
                }

                // Use comma as separator
                String[] columns = line.split(",");

                // Check if the array has enough elements before accessing
                if (columns.length >= 4) {
                    // Check for duplicates
                    String keyString = columns[1];
                    String value = columns[3];

                    try {
                        // Convert key to an integer
                        int key = Integer.parseInt(keyString);

                        if (!portMap.containsKey(key)) {
                            // If not a duplicate key, add the key-value pair to the map
                            portMap.put(key, value);
                            sortedKeys.add(key); // Add key to the set for sorting
                        }
                    } catch (NumberFormatException e) {
                        // Handle the case when keyString is not a valid integer
                    }
                } else {
                    // Handle the case when the array doesn't have enough elements
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Jedis jedis = null;
        try {
            jedis = new Jedis("localhost");

            // Write the Map contents to Redis
            for (Map.Entry<Integer, String> entry : portMap.entrySet()) {
                jedis.set(entry.getKey().toString(), entry.getValue());
            }

            // Read and print the Redis database contents in order of keys
            for (Integer key : sortedKeys) {
                String value = jedis.get(key.toString());
                System.out.println("Port: " + key +" "+ value);
            }
        } catch (JedisConnectionException e) {
            System.out.println("Could not connect to Redis: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Issue: " + e.getMessage());
        } finally {
            if (jedis != null) {
                jedis.close(); // Always close the connection
            }
        }
    }
}

