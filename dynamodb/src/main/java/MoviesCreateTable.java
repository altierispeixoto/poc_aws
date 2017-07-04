/**
 *
 * Created by altieris on 20/06/17.
 */
import java.util.ArrayList;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;

public class MoviesCreateTable {

    public static void main(String[] args) throws Exception {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        DynamoDB dynamoDBClient = new DynamoDB(client);

        String tableName = "kinesisEventsSource";

        try {

            System.out.println("Attempting to create table; please wait...");


            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

            attributeDefinitions.add(new AttributeDefinition("serial", ScalarAttributeType.S));
            attributeDefinitions.add(new AttributeDefinition("date", ScalarAttributeType.N));



            ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
            keySchema.add(new KeySchemaElement()
                    .withAttributeName("serial")
                    .withKeyType(KeyType.HASH));  //Partition key


            keySchema.add(new KeySchemaElement()
                    .withAttributeName("date")
                    .withKeyType(KeyType.RANGE));  //Sort key



            StreamSpecification streamSpecification = new StreamSpecification();
            streamSpecification.setStreamEnabled(true);
            streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE);


            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(10L)
                            .withWriteCapacityUnits(10L))
                    .withStreamSpecification(streamSpecification);

            dynamoDBClient.createTable(createTableRequest).waitForActive();


            DescribeTableResult describeTableResult = client.describeTable(tableName);

            String myStreamArn = describeTableResult.getTable().getLatestStreamArn();
            StreamSpecification myStreamSpec =
                    describeTableResult.getTable().getStreamSpecification();

            System.out.println("Current stream ARN for " + tableName + ": "+ myStreamArn);
            System.out.println("Stream enabled: "+ myStreamSpec.getStreamEnabled());
            System.out.println("Update view type: "+ myStreamSpec.getStreamViewType());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }
}