package org.example;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
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

/* Project: 
 *  Authors: Benjamin Prat, Mckennah Palmer, Tyson Nipges-Mergel, Nicolas Almeida, Dane Selch
 *  Class: CSE 490 Sepcial topics
 *  Teacher: Brother Clements
 *  Description: This is our Demo, used for the purpose of showing our ability to connect to a
 *      mongodb local server, and then follow the CRUD method to create and a database on said
 *      server.
 */

import java.util.Arrays;

public class Main {
    /* Function: main
     *  Inpits: N/A
     *  Descripion: main is the runner function. It will begin by connecting to the 
     *      local Mongodb server. upon failing it will output an error message. Upon 
     *      success, the function will procede to create the needed objects to call and
     *      run all the; insert(), read(), update(), and delete() functions. 
     */
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

            // Read
            try {
                // Prints document for user to read
                read(collection, query);
            }catch (MongoException meToo)
            {
                //Error message for user
                System.err.println("Unable to connect: " + meToo);
            }

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
    
    /* Function: insert
     *  Input: MongoCollection<Document>, Document
     *  Description:  Upon accessing a collection, This function will add the desired
     *      document into the mongoDB collection. On completion, it will then print out
     *      the results of the insertone() function to the System. On failure an error
     *      message will instead be sent to the system. 
     */
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

    /* Function: read
     *  Input: MongoCollection<Document>, Document
     *  Description: Upon being called, This function will search a
     *      mongodb collection for a specific document. If found, the 
     *      document will be printed to the System. In case the document
     *      is not found, an error message will be printed instead.
     */
    public static void read(MongoCollection<Document> collection, Document query)
    {
        try {
            // To make sure it is the document I want to pull for this test
            Bson filter = Filters.and(Filters.gt("qty", 10), Filters.lt("qty", 5));
            // Prints out document for the user to read
            System.out.println("The document: " + collection.find(filter));
            //System.out.println("The document: " + collection.find(query));
        } catch (MongoException meToo) {
            // Gives a warning message for user if print fails
            System.err.println("Unable to print for user to read: " + meToo);
        }
    }

    /* Function: update
     *  Input: MongoCollection<Document>, Document, Bson, UpdateOptions
     *  Description: This funciton will find a given document inside a mongodb collection
     *      once found it will take and add the data in the Bson object to the document.
     *      On a fail to, will send an error message to the system.
     */ 
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

    /* Function: delete
     *  Input: MongoCollection<Document>, Document
     *   Description: This function will Seach a collection for the desired document
     *      once found, it will remove the document with a message containting
     *      the amount of deleted entries.  In the case of failure it will have
     *      an error message 
     */
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
