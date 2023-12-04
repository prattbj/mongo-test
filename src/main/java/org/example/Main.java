package org.example;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.DeleteResult;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {

        String uri = "mongodb://localhost:27017";

        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .serverApi(serverApi)
                .build();

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase("Users");
            MongoCollection<Document> collection = database.getCollection("Accounts");

            // Send a ping to confirm a successful connection
            Bson command = new BsonDocument("ping", new BsonInt64(1));
            database.runCommand(command);
            System.out.println("Pinged your deployment. You successfully connected to MongoDB!");

            //Create document to insert
            Document toInsert = new Document()
                    .append("_id", new ObjectId())
                    .append("email", "nip20001@byui.edu")
                    .append("role", "admin")
                    .append("first_name", "Tyson")
                    .append("last_name", "Mergel")
                    .append("bio", "free text")
                    .append("technology", Arrays.asList("Python", "Java", "MangoDB"))
                    .append("Social", Arrays.asList("Facebook", "Java", "MangoDB"))
                    .append("messengers", Arrays.asList("Type", "discord", "Link"));

            insert(collection, toInsert);

            // Create things to update
            Document query = new Document().append("first_name",  "Tyson");
            Bson updates = Updates.combine(
                    Updates.set("last_name", "Nipges-Mergel"),
                    Updates.addToSet("Bio", "Here to have fun"),
                    Updates.currentTimestamp("lastUpdated"));
            UpdateOptions options = new UpdateOptions().upsert(true);

            update(collection, query, updates, options);

            //Create document to delete
            Document toDelete = new Document("first_name", "Tyson");

            delete(collection, toDelete);


        } catch(MongoException me)
        {
            System.err.println("Unable to connect to database: " + me);
        }
    }
    // Insert a document into a collection
    public static void insert(MongoCollection<Document> collection, Document toInsert)
    {
        try {
            // Inserts a sample document describing a movie into the collection
            InsertOneResult result = collection.insertOne(toInsert);

            // Prints the ID of the inserted document
            System.out.println("Success! Inserted document id: " + result.getInsertedId());

            // Prints a message if any exceptions occur during the operation
        } catch (MongoException me) {
            System.err.println("Unable to insert due to an error: " + me);
        }
    }

    //Update a document
    public static void update(MongoCollection<Document> collection, Document query, Bson updates, UpdateOptions options)
    {
        try {
            UpdateResult result = collection.updateOne(query, updates, options);
            System.out.println("Modified document count: " + result.getModifiedCount());
            System.out.println("Upserted id: " + result.getUpsertedId()); // only contains a value when an upsert is performed
        } catch (MongoException me) {
            System.err.println("Unable to update due to an error: " + me);
        }
    }

    //Delete a document
    public static void delete(MongoCollection<Document> collection, Document toDelete)
    {
        //Note that no exception will be thrown if there isn't anything to delete.
        try {

            DeleteResult result = collection.deleteOne(toDelete);
            System.out.println("Deleted document count: " + result.getDeletedCount());

        } catch (MongoException me){
            System.err.println("Unable to delete document: " + me);
        }
    }
}
