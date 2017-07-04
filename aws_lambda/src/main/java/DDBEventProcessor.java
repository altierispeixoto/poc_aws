import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class DDBEventProcessor implements
        RequestHandler<DynamodbEvent, String> {



    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        KinesisProducer kinesis = new KinesisProducer();

        for (DynamodbStreamRecord record : ddbEvent.getRecords()){
            System.out.println(record.getEventID());
            System.out.println(record.getEventName());
            System.out.println(record.getDynamodb().toString());

            ByteBuffer data = null;
            try {

                data = ByteBuffer.wrap(record.getDynamodb().toString().getBytes("UTF-8"));

            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            kinesis.addUserRecord("kinesisProtoStream", UUID.randomUUID().toString() , data);


        }
        return "Successfully processed " + ddbEvent.getRecords().size() + " records.";
    }

}