package com.topbloc.codechallenge;

import com.topbloc.codechallenge.db.DatabaseManager;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import static spark.Spark.*;

public class Main {
    public static void main(String[] args) {
        DatabaseManager.connect();
        // Don't change this - required for GET and POST requests with the header 'content-type'
        options("/*",
                (req, res) -> {
                    res.header("Access-Control-Allow-Headers", "content-type");
                    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
                    return "OK";
                });

        // Don't change - if required you can reset your database by hitting this endpoint at localhost:4567/reset
        get("/reset", (req, res) -> {
            DatabaseManager.resetDatabase();
            return "OK";
        });



        //TODO: Add your routes here. a couple of examples are below
        get("/version", (req, res) -> "TopBloc Code Challenge v1.0");

        //GET Routes (Inventory)
        get("/items", (req, res) -> DatabaseManager.getItems());
        get("/out_of_stock", (req, res) -> DatabaseManager.getOutOfStockItems());
        get("/overstock", (req, res) -> DatabaseManager.getOverStock());
        get("/low_stock", (req, res) -> DatabaseManager.getLowStock());
        get("inventory/item/:id", (req, res) -> {
            res.type("application/json");
            try {
                int itemId = Integer.parseInt(req.params(":id"));
                JSONObject item = DatabaseManager.getSpecificItem(itemId);
                if (item != null) {
                    return item.toJSONString();
                } else {
                    res.status(404);
                    return "{\"error\":\"Item not found\"}";
                }
            } catch (NumberFormatException e) {
                res.status(400);
                return "{\"error\":\"Invalid item ID format\"}";
            }
        });

        // GET routes (Distributor)
        get("/distributor", (req, res) -> DatabaseManager.getDistributors());
        get("/distributor/:id/items", (req, res) -> {
            res.type("application/json");
            int distributorId;
            try {
                distributorId = Integer.parseInt(req.params(":id"));
            } catch (NumberFormatException e) {
                res.status(400);
                return "{\"error\":\"Invalid distributor ID\"}";
            }

            JSONArray items = DatabaseManager.getItemsByDistributor(distributorId);
            if (items == null || items.isEmpty()) {
                res.status(404);
                return "{\"error\": \"No items found for distributor\"}";
            }

            return items.toJSONString();
        });
        get("/item/:id/distributors", (req, res) -> {
            res.type("application/json");
            int itemId;
            try {
                itemId = Integer.parseInt(req.params(":id"));
            } catch (NumberFormatException e) {
                res.status(400);
                return "{\"error\": \"Invalid item ID\"}";
            }

            JSONArray distributors = DatabaseManager.getDistributorsByItemId(itemId);
            if (distributors == null || distributors.isEmpty()) {
                res.status(404);
                return "{\"error\": \"No distributors found for item\"}";
            }

            return distributors.toJSONString();
        });

        // POST/PUT/DELETE Routes
        post("/item", (req, res) -> {
            res.type("application/json");
            JSONObject body = (JSONObject) new org.json.simple.parser.JSONParser().parse(req.body());
            String name = (String) body.get("name");

            if (name == null || name.trim().isEmpty()) {
                res.status(400);
                return "{\"error\":\"Item name is required\"}";
            }

            boolean added = DatabaseManager.addItem(name);
            if (added) {
                res.status(201);
                return "{\"message\":\"Item added successfully\"}";
            } else {
                res.status(500);
                return "{\"error\":\"Could not add item\"}";
            }
        });

        post("/inventory", (req, res) -> {
            res.type("application/json");

            try {
                JSONObject body = (JSONObject) new JSONParser().parse(req.body());

                Long itemId = (Long) body.get("itemId");
                Long stock = (Long) body.get("stock");
                Long capacity = (Long) body.get("capacity");

                if (itemId == null || stock == null || capacity == null) {
                    res.status(400);
                    return "{\"error\": \"itemId, stock, and capacity are required.\"}";
                }

                boolean success = DatabaseManager.addToInventory(itemId.intValue(), stock.intValue(), capacity.intValue());

                if (success) {
                    res.status(201);
                    return "{\"message\": \"Inventory added successfully.\"}";
                } else {
                    res.status(500);
                    return "{\"error\": \"Failed to add to inventory. Item may already exist in inventory.\"}";
                }

            } catch (Exception e) {
                res.status(500);
                return "{\"error\": \"Invalid request body: " + e.getMessage() + "\"}";
            }
        });

        put("/inventory/:id", (req, res) -> {
            res.type("application/json");
            int itemId = Integer.parseInt(req.params(":id"));

            try {
                JSONObject body = (JSONObject) new JSONParser().parse(req.body());

                Integer stock = body.get("stock") != null ? ((Long) body.get("stock")).intValue() : null;
                Integer capacity = body.get("capacity") != null ? ((Long) body.get("capacity")).intValue() : null;

                boolean success = DatabaseManager.updateInventoryItem(itemId, stock, capacity);

                if (success) {
                    res.status(200);
                    return "{\"message\": \"Inventory updated successfully.\"}";
                } else {
                    res.status(404);
                    return "{\"error\": \"Inventory item not found or update failed.\"}";
                }
            } catch (Exception e) {
                res.status(500);
                return "{\"error\": \"Invalid request: " + e.getMessage() + "\"}";
            }
        });

        post("/distributors", (req, res) -> {
            res.type("application/json");

            JSONObject body = (JSONObject) new JSONParser().parse(req.body());
            String name = (String) body.get("name");

            if (name == null || name.isEmpty()) {
                res.status(400);
                return "{\"error\": \"Distributor name is required\"}";
            }

            String result = DatabaseManager.addDistributor(name);
            return "{\"message\": \"" + result + "\"}";
        });

        post("/distributor-catalog", (req, res) -> {
            res.type("application/json");

            JSONObject body = (JSONObject) new JSONParser().parse(req.body());

            int distributorId = ((Long) body.get("distributor_id")).intValue();
            int itemId = ((Long) body.get("item_id")).intValue();
            float cost = ((Double) body.get("cost")).floatValue();

            String result = DatabaseManager.addItemToDistributorCatalog(distributorId, itemId, cost);
            return "{\"message\": \"" + result + "\"}";
        });

        put("/distributor-catalog", (req, res) -> {
            res.type("application/json");

            JSONObject body = (JSONObject) new JSONParser().parse(req.body());

            int distributorId = ((Long) body.get("distributor_id")).intValue();
            int itemId = ((Long) body.get("item_id")).intValue();
            float newCost = ((Double) body.get("cost")).floatValue();

            String result = DatabaseManager.updateDistributorCatalogPrice(distributorId, itemId, newCost);
            return "{\"message\": \"" + result + "\"}";
        });

        get("/restock/cheapest", (req, res) -> {
            res.type("application/json");

            int itemId = Integer.parseInt(req.queryParams("item_id"));
            int quantity = Integer.parseInt(req.queryParams("quantity"));

            return DatabaseManager.getCheapestRestockOption(itemId, quantity).toJSONString();
        });

        delete("/inventory/:item_id", (req, res) -> {
            res.type("application/json");
            int itemId = Integer.parseInt(req.params(":item_id"));
            return DatabaseManager.deleteItemFromInventory(itemId).toJSONString();
        });

        delete("/distributors/:id", (req, res) -> {
            res.type("application/json");
            int id = Integer.parseInt(req.params(":id"));
            return DatabaseManager.deleteDistributorById(id).toJSONString();
        });

    }
}