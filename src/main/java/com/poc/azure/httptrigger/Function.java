package com.poc.azure.httptrigger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.poc.azure.dto.Employee;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    /**
     * This function listens at endpoint "/api/HttpTrigger-Java". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpTrigger-Java&code={your function key}
     * 2. curl "{your host}/api/HttpTrigger-Java?name=HTTP%20Query&code={your function key}"
     * Function Key is not needed when running locally, it is used to invoke function deployed to Azure.
     * More details: https://aka.ms/functions_authorization_keys
     * @return HttpResponseMessage
     */
    @FunctionName("HttpTrigger-Java")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
                    HttpRequestMessage<Optional<String>> request,
            @CosmosDBInput(name = "sourceDocuments", databaseName = "ToDoList", collectionName = "items",
                    connectionStringSetting = "CosmosDBSourceStorage", partitionKey = "/employeeId")
                    Optional<String> inputDocuments,
            @CosmosDBOutput(name = "processedDocuments", databaseName = "EmployeesProcessed", collectionName = "employeesProcessed",
                    connectionStringSetting = "CosmosDBSinkStorage", partitionKey = "/employeeId")
                    OutputBinding<List<Document>> outputData, final ExecutionContext context) {
//            @TimerTrigger(name = "keepAliveTrigger", schedule = "0 */2 * * * *") String timerInfo,
//            ExecutionContext context
           /*@CosmosDBTrigger(name = "dataItems", databaseName = "ToDoList", collectionName = "items",
                    connectionStringSetting = "CosmosDBSourceStorage", leaseCollectionName = "leases") String[] input, final ExecutionContext context*/
        context.getLogger().info("Java HTTP trigger processed a request.");
        JSONArray sourceDocuments = new JSONArray(inputDocuments.get());
        System.out.println("Document count :" +  sourceDocuments.length());
        System.out.println(inputDocuments.get().toString());
        List<Document> sinkDocuments = new ArrayList<>();

        for (Object obj : sourceDocuments) {
            JSONObject sinkDocument = (JSONObject) obj;
            sinkDocument.put("profile_image", "Not available");
            Document document = Document.parse(sinkDocument.toString());
            sinkDocuments.add(document);
        }
        outputData.setValue(sinkDocuments);
        /*MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://azurefunctionspoc:677pp7s7bzzP6mc7V0QZeGS51IX8oczXwqS9o4n4GuG1FhmlHCN5xXj06veHR3klv818cEpGglCZDtZRSob9wQ==@azurefunctionspoc.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"));
        DB db = mongoClient.getDB( "Employees" );
        DBCollection collection = db.getCollection("employees");
        DBCursor cursor = collection.find();*/

        /*CosmosClient client = new CosmosClientBuilder()
                .endpoint("cosmodbsink.documents.azure.com")
                .key("GyK5aLqRZedrtjzr8itqCtYle618kQD8riC7EuFPj3uIkW3D6wG6lRq9Kk5jUHFrmQk5A1yV1ur0gC310khnOQ==")
                .connectionPolicy(new ConnectionPolicy())
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .build();
        CosmosDatabase database = client.getDatabase("EmployeesProcessed");
        CosmosContainer cosmosContainer = database.getContainer("employeesProcessed");*/

        //For sink database
        /*MongoClient mongoClientSink = new MongoClient(new MongoClientURI("mongodb://cosmodbsink:GyK5aLqRZedrtjzr8itqCtYle618kQD8riC7EuFPj3uIkW3D6wG6lRq9Kk5jUHFrmQk5A1yV1ur0gC310khnOQ==@cosmodbsink.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"));
        DB dbSink = mongoClient.getDB( "EmployeesProcessed" );
        DBCollection collectionSink = dbSink.getCollection("employeesProcessed");*/
        /*MongoClientURI mongoClientURI = new MongoClientURI("mongodb://cosmodbsink:GyK5aLqRZedrtjzr8itqCtYle618kQD8riC7EuFPj3uIkW3D6wG6lRq9Kk5jUHFrmQk5A1yV1ur0gC310khnOQ==@cosmodbsink.documents.azure.com:10255/?ssl=true&replicaSet=globaldb");
        MongoClient mongoClient1 = new MongoClient(mongoClientURI);
        MongoDatabase mongoDatabase = mongoClient1.getDatabase("EmployeeProcessed");
        MongoCollection<Document> collection1 = mongoDatabase.getCollection("employeeProcessed");
        

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Employee employee = null;
        while(cursor.hasNext()) {
            try {
                JSONObject item = new JSONObject(cursor.next().toString());
                item.put("profile_image", "No image available");
                item.remove("_id");
                Document document = Document.parse(item.toString());
                collection1.insertOne(document);
                context.getLogger().info("Inserted document: " + document.toString());
                *//*DBObject processedItem = (DBObject) JSON.parse(item.toString());
                System.out.println(processedItem.toString());
                collectionSink.insert(processedItem);*//*
//                cosmosContainer.createItem(item);
                employee = objectMapper.readValue(item.toString(), Employee.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }*/
        
        return request.createResponseBuilder(HttpStatus.OK).body(sinkDocuments).build();
        // Parse query parameter
       /* String query = request.getQueryParameters().get("name");
        String name = request.getBody().orElse(query);

        if (name == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a name on the query string or in the request body").build();
        } else {
            return request.createResponseBuilder(HttpStatus.OK).body("Hello, " + name).build();
        }*/
    }
}
