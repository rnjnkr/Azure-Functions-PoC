package com.poc.azure.cosmosdbtrigger;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.CosmosDBTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

public class Function {
    @FunctionName("CosmosDBTrigger-Java")
    public void run(
           @CosmosDBTrigger(name = "dataItems", databaseName = "ToDoList", collectionName = "items",
                    connectionStringSetting = "CosmosDBSourceStorage", leaseCollectionName = "leases")
                    String[] input, final ExecutionContext context) {

        context.getLogger().info("Java HTTP trigger processed a request.");
        context.getLogger().info("Document count :" +  input.length);
        for (String string : input) {
            context.getLogger().info(string);
        }

        /*//Source database
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://azurefunctionspoc:677pp7s7bzzP6mc7V0QZeGS51IX8oczXwqS9o4n4GuG1FhmlHCN5xXj06veHR3klv818cEpGglCZDtZRSob9wQ==@azurefunctionspoc.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"));
        DB db = mongoClient.getDB("Employees");
        DBCollection collection = db.getCollection("employees");
        DBCursor cursor = collection.find();

        CosmosClient client = new CosmosClientBuilder()
                .endpoint("cosmodbsink.documents.azure.com")
                .key("GyK5aLqRZedrtjzr8itqCtYle618kQD8riC7EuFPj3uIkW3D6wG6lRq9Kk5jUHFrmQk5A1yV1ur0gC310khnOQ==")
                .connectionPolicy(new ConnectionPolicy())
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .build();
        CosmosDatabase database = client.getDatabase("EmployeesProcessed");
        CosmosContainer cosmosContainer = database.getContainer("employeesProcessed");

        //For sink database
        MongoClientURI mongoClientURI = new MongoClientURI("mongodb://cosmodbsink:GyK5aLqRZedrtjzr8itqCtYle618kQD8riC7EuFPj3uIkW3D6wG6lRq9Kk5jUHFrmQk5A1yV1ur0gC310khnOQ==@cosmodbsink.documents.azure.com:10255/?ssl=true&replicaSet=globaldb");
        MongoClient mongoClient1 = new MongoClient(mongoClientURI);
        MongoDatabase mongoDatabase = mongoClient1.getDatabase("EmployeeProcessed");
        MongoCollection<Document> collection1 = mongoDatabase.getCollection("employeeProcessed");

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Employee employee = null;
        while (cursor.hasNext()) {
            try {
                JSONObject item = new JSONObject(cursor.next().toString());
                item.put("profile_image", "No image available");
                item.remove("_id");
                Document document = Document.parse(item.toString());
                collection1.insertOne(document);
                *//*DBObject processedItem = (DBObject) JSON.parse(item.toString());
                System.out.println(processedItem.toString());
                collectionSink.insert(processedItem);*//*
//                cosmosContainer.createItem(item);
                employee = objectMapper.readValue(item.toString(), Employee.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }*/
    }
}
