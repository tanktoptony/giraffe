package com.topbloc.codechallenge.db;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DatabaseManager {
    private static final String jdbcPrefix = "jdbc:sqlite:";
    private static final String dbName = "challenge.db";
    private static String connectionString;
    private static Connection conn;

    static {
        File dbFile = new File(dbName);
        connectionString = jdbcPrefix + dbFile.getAbsolutePath();
    }

    public static void connect() {
        try {
            Connection connection = DriverManager.getConnection(connectionString);
            System.out.println("Connection to SQLite has been established.");
            conn = connection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    // Schema function to reset the database if needed - do not change
    public static void resetDatabase() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        File dbFile = new File(dbName);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        connectionString = jdbcPrefix + dbFile.getAbsolutePath();
        connect();
        applySchema();
        seedDatabase();
    }

    // Schema function to reset the database if needed - do not change
    private static void applySchema() {
        String itemsSql = "CREATE TABLE IF NOT EXISTS items (\n"
                + "id integer PRIMARY KEY,\n"
                + "name text NOT NULL UNIQUE\n"
                + ");";
        String inventorySql = "CREATE TABLE IF NOT EXISTS inventory (\n"
                + "id integer PRIMARY KEY,\n"
                + "item integer NOT NULL UNIQUE references items(id) ON DELETE CASCADE,\n"
                + "stock integer NOT NULL,\n"
                + "capacity integer NOT NULL\n"
                + ");";
        String distributorSql = "CREATE TABLE IF NOT EXISTS distributors (\n"
                + "id integer PRIMARY KEY,\n"
                + "name text NOT NULL UNIQUE\n"
                + ");";
        String distributorPricesSql = "CREATE TABLE IF NOT EXISTS distributor_prices (\n"
                + "id integer PRIMARY KEY,\n"
                + "distributor integer NOT NULL references distributors(id) ON DELETE CASCADE,\n"
                + "item integer NOT NULL references items(id) ON DELETE CASCADE,\n"
                + "cost float NOT NULL\n" +
                ");";

        try {
            System.out.println("Applying schema");
            conn.createStatement().execute(itemsSql);
            conn.createStatement().execute(inventorySql);
            conn.createStatement().execute(distributorSql);
            conn.createStatement().execute(distributorPricesSql);
            System.out.println("Schema applied");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // Schema function to reset the database if needed - do not change
    private static void seedDatabase() {
        String itemsSql = "INSERT INTO items (id, name) VALUES (1, 'Licorice'), (2, 'Good & Plenty'),\n"
            + "(3, 'Smarties'), (4, 'Tootsie Rolls'), (5, 'Necco Wafers'), (6, 'Wax Cola Bottles'), (7, 'Circus Peanuts'), (8, 'Candy Corn'),\n"
            + "(9, 'Twix'), (10, 'Snickers'), (11, 'M&Ms'), (12, 'Skittles'), (13, 'Starburst'), (14, 'Butterfinger'), (15, 'Peach Rings'), (16, 'Gummy Bears'), (17, 'Sour Patch Kids')";
        String inventorySql = "INSERT INTO inventory (item, stock, capacity) VALUES\n"
                + "(1, 22, 25), (2, 4, 20), (3, 15, 25), (4, 30, 50), (5, 14, 15), (6, 8, 10), (7, 10, 10), (8, 30, 40), (9, 17, 70), (10, 43, 65),\n" +
                "(11, 32, 55), (12, 25, 45), (13, 8, 45), (14, 10, 60), (15, 20, 30), (16, 15, 35), (17, 14, 60)";
        String distributorSql = "INSERT INTO distributors (id, name) VALUES (1, 'Candy Corp'), (2, 'The Sweet Suite'), (3, 'Dentists Hate Us')";
        String distributorPricesSql = "INSERT INTO distributor_prices (distributor, item, cost) VALUES \n" +
                "(1, 1, 0.81), (1, 2, 0.46), (1, 3, 0.89), (1, 4, 0.45), (2, 2, 0.18), (2, 3, 0.54), (2, 4, 0.67), (2, 5, 0.25), (2, 6, 0.35), (2, 7, 0.23), (2, 8, 0.41), (2, 9, 0.54),\n" +
                "(2, 10, 0.25), (2, 11, 0.52), (2, 12, 0.07), (2, 13, 0.77), (2, 14, 0.93), (2, 15, 0.11), (2, 16, 0.42), (3, 10, 0.47), (3, 11, 0.84), (3, 12, 0.15), (3, 13, 0.07), (3, 14, 0.97),\n" +
                "(3, 15, 0.39), (3, 16, 0.91), (3, 17, 0.85)";

        try {
            System.out.println("Seeding database");
            conn.createStatement().execute(itemsSql);
            conn.createStatement().execute(inventorySql);
            conn.createStatement().execute(distributorSql);
            conn.createStatement().execute(distributorPricesSql);
            System.out.println("Database seeded");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // Helper methods to convert ResultSet to JSON - change if desired, but should not be required
    private static JSONArray convertResultSetToJson(ResultSet rs) throws SQLException{
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<String> colNames = IntStream.range(0, columns)
                .mapToObj(i -> {
                    try {
                        return md.getColumnName(i + 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .collect(Collectors.toList());

        JSONArray jsonArray = new JSONArray();
        while (rs.next()) {
            jsonArray.add(convertRowToJson(rs, colNames));
        }
        return jsonArray;
    }

    private static JSONObject convertRowToJson(ResultSet rs, List<String> colNames) throws SQLException {
        JSONObject obj = new JSONObject();
        for (String colName : colNames) {
            obj.put(colName, rs.getObject(colName));
        }
        return obj;
    }

    // Controller functions - add your routes here. getItems is provided as an example
    public static JSONArray getItems() {
        String sql = "SELECT * FROM items";
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray getOutOfStockItems() {
        String sql = "SELECT inventory.id, items.name, inventory.stock, inventory.capacity " +
                    "FROM inventory " +
                    "JOIN items ON inventory.id = items.id " +
                    "WHERE inventory.stock = 0";
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray getOverStock() {
        String sql = "SELECT inventory.id, items.name, inventory.stock, inventory.capacity " +
                "FROM inventory " +
                "JOIN items ON inventory.id = items.id " +
                "WHERE inventory.stock > inventory.capacity";
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray getLowStock() {
        String sql = "SELECT inventory.id, items.name, inventory.stock, inventory.capacity " +
                "FROM inventory " +
                "JOIN items ON inventory.id = items.id " +
                "WHERE (CAST(inventory.stock AS FLOAT) / inventory.capacity) < .35";
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONObject getSpecificItem(int itemId) {
        String sql = "SELECT inventory.id, items.name, inventory.stock, inventory.capacity " +
                "FROM inventory " +
                "JOIN items ON inventory.id = items.id " +
                "WHERE inventory.id = ?";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setInt(1, itemId);
            ResultSet set = statement.executeQuery();

            if (set.next()) {
                JSONObject obj = new JSONObject();
                obj.put("id", set.getInt("id"));
                obj.put("name", set.getInt("name"));
                obj.put("stock", set.getInt("stock"));
                obj.put("capacity", set.getInt("capacity"));
                return obj;
            } else {
                return null;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray getDistributors() {
        String sql = "SELECT * FROM distributors";
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray getItemsByDistributor(int distributorId) {
        String sql = "SELECT items.id, items.name, distributor_prices.cost " +
                "FROM distributor_prices " +
                "JOIN items ON distributor_prices.item = items.id " +
                "WHERE distributor_prices.distributor = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, distributorId);
            ResultSet rs = statement.executeQuery();
            JSONArray json = convertResultSetToJson(rs);
            System.out.println("Found " + json.size() + " items for distributor ID " + distributorId);
            return json;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray getDistributorsByItemId(int itemId) {
        String sql = "SELECT distributors.id, distributors.name, distributor_prices.cost " +
                "FROM distributor_prices " +
                "JOIN distributors ON distributor_prices.distributor = distributors.id " +
                "WHERE distributor_prices.item = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, itemId);
            ResultSet rs = stmt.executeQuery();
            JSONArray json = convertResultSetToJson(rs);
            System.out.println("Found " + json.size() + " distributors for this item ID " + itemId);
            return json;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    // POST PUT DELETE
    public static boolean addItem(String name) {
        String sql = "INSERT INTO items (name) VALUES (?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, name);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public static boolean addToInventory(int itemId, int stock, int capacity) {
        String sql = "INSERT INTO inventory (item, stock, capacity) VALUES (?, ?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, itemId);
            stmt.setInt(2, stock);
            stmt.setInt(3, capacity);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("Error adding to inventory: " + e.getMessage());
            return false;
        }
    }

    public static boolean updateInventoryItem(int itemId, Integer stock, Integer capacity) {
        StringBuilder sql = new StringBuilder("UPDATE inventory SET ");
        boolean setStock = stock != null;
        boolean setCapacity = capacity != null;

        if (!setStock && !setCapacity) return false;

        if (setStock) sql.append("stock = ?");
        if (setCapacity) {
            if (setStock) sql.append(", ");
            sql.append("capacity = ?");
        }

        sql.append(" WHERE item = ?");

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            if (setStock) stmt.setInt(idx++, stock);
            if (setCapacity) stmt.setInt(idx++, capacity);
            stmt.setInt(idx, itemId);

            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            System.out.println("Error updating inventory: " + e.getMessage());
            return false;
        }
    }

    public static String addDistributor(String name) {
        String sql = "INSERT INTO distributors (name) VALUES (?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, name);
            int rows = stmt.executeUpdate();
            return rows > 0 ? "Distributor added successfully" : "Failed to add distributor";
        } catch (SQLException e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }

    public static String addItemToDistributorCatalog(int distributorId, int itemId, float cost) {
        String sql = "INSERT INTO distributor_prices (distributor, item, cost) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, distributorId);
            stmt.setInt(2, itemId);
            stmt.setFloat(3, cost);

            int rows = stmt.executeUpdate();
            return rows > 0 ? "Item added to catalog successfully" : "Failed to add item to catalog";
        } catch (SQLException e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }

    public static String updateDistributorCatalogPrice(int distributorId, int itemId, float newCost) {
        String sql = "UPDATE distributor_prices SET cost = ? WHERE distributor = ? AND item = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFloat(1, newCost);
            stmt.setInt(2, distributorId);
            stmt.setInt(3, itemId);

            int rows = stmt.executeUpdate();
            return rows > 0 ? "Price updated successfully" : "Item not found in catalog";
        } catch (SQLException e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }

    public static JSONObject getCheapestRestockOption(int itemId, int quantity) {
        String sql = "SELECT d.id AS distributor_id, d.name AS distributor_name, dp.cost " +
                "FROM distributor_prices dp " +
                "JOIN distributors d ON dp.distributor = d.id " +
                "WHERE dp.item = ? " +
                "ORDER BY dp.cost ASC LIMIT 1";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, itemId);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                JSONObject result = new JSONObject();
                result.put("distributor_id", rs.getInt("distributor_id"));
                result.put("distributor_name", rs.getString("distributor_name"));
                float unitCost = rs.getFloat("cost");
                result.put("unit_cost", unitCost);
                result.put("quantity", quantity);
                result.put("total_cost", unitCost * quantity);
                return result;
            } else {
                JSONObject result = new JSONObject();
                result.put("message", "No distributor found for given item");
                return result;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            JSONObject error = new JSONObject();
            error.put("error", e.getMessage());
            return error;
        }
    }

    public static JSONObject deleteItemFromInventory(int itemId) {
        String sql = "DELETE FROM inventory WHERE item = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, itemId);
            int rowsAffected = stmt.executeUpdate();

            JSONObject response = new JSONObject();
            if (rowsAffected > 0) {
                response.put("message", "Item removed from inventory.");
                response.put("item_id", itemId);
            } else {
                response.put("message", "Item not found in inventory.");
            }
            return response;
        } catch (SQLException e) {
            e.printStackTrace();
            JSONObject error = new JSONObject();
            error.put("error", e.getMessage());
            return error;
        }
    }

    public static JSONObject deleteDistributorById(int distributorId) {
        String sql = "DELETE FROM distributors WHERE id = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, distributorId);
            int rowsAffected = stmt.executeUpdate();

            JSONObject response = new JSONObject();
            if (rowsAffected > 0) {
                response.put("message", "Distributor deleted.");
                response.put("distributor_id", distributorId);
            } else {
                response.put("message", "Distributor not found.");
            }
            return response;
        } catch (SQLException e) {
            e.printStackTrace();
            JSONObject error = new JSONObject();
            error.put("error", e.getMessage());
            return error;
        }
    }

}
