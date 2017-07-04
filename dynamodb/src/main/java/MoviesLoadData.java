/**
 *
 * Created by altieris on 20/06/17.
 */
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;

public class MoviesLoadData {


    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();


    public static void main(String[] args) throws Exception {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("kinesisEventsSource");

        while (true) {

            String serial = "222333555";
            long date = System.currentTimeMillis();

            ByteBuffer bf = ByteBuffer.wrap("aqui vai o proto".getBytes(charset));

            try {
                Item item = new Item()
                        .withPrimaryKey("serial", serial, "date", date)
                        .withBinary("proto",bf);

                table.putItem(item);

                System.out.println("PutItem succeeded: " + serial + " " + date);

            }
            catch (Exception e) {
                System.err.println("Unable to add movie: " + serial + " " + date);
                System.err.println(e.getMessage());
                break;
            }
        }
    }

}