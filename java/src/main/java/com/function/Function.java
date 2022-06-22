package com.function;

import java.util.*;
import com.google.gson.Gson;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.util.Optional;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    /**
     * This function listens at endpoint "/api/publish". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/publish
     * 2. curl "{your host}/api/HttpExample?message=HTTP%20Query"
     */
    @FunctionName("publish")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,        
            @KafkaOutput(
                name = "publish",
                topic= "topic",
                brokerList = "%BrokerList%",
                username = "$ConnectionString",
                password = "EventHubConnectionString",
                authenticationMode = BrokerAuthenticationMode.PLAIN,
                sslCaLocation = "confluent_cloud_cacert.pem",
                protocol = BrokerProtocol.SASLSSL
                ) OutputBinding<KafkaEntity[]> output,                
            final ExecutionContext context) {

        context.getLogger().info("Java HTTP trigger processed a request.");        

        // Parse query parameter
        String message = request.getQueryParameters().get("message");
        message = request.getBody().orElse(message);        
        context.getLogger().info("Message:" + message);
        
        KafkaEntity[] kafkaEvents = new KafkaEntity[1];
        KafkaHeaders[] headers1 = new KafkaHeaders[1];
        headers1[0] = new KafkaHeaders("test-header-key", "test-header-value");        
        KafkaEntity kafkaEvent1 = new KafkaEntity(message, headers1);
        kafkaEvents[0] = kafkaEvent1;                
        
        //output.setValue(message);
        output.setValue(kafkaEvents);

        return request.createResponseBuilder(HttpStatus.OK).body(message).build();
    }


    @FunctionName("KafkaTriggerMany")
    public void runMany(
            @KafkaTrigger(
                name = "kafkaTriggerMany",
                topic = "topic",  
                brokerList="%BrokerList%",
                consumerGroup="$Default", 
                username = "$ConnectionString", 
                password = "EventHubConnectionString",
                authenticationMode = BrokerAuthenticationMode.PLAIN,
                protocol = BrokerProtocol.SASLSSL,
                sslCaLocation = "confluent_cloud_cacert.pem", // Enable this line for windows.
                cardinality = Cardinality.MANY,
                dataType = "string"
             ) List<String> kafkaEvents,
             @KafkaOutput(
                name = "KafkaOutput",
                topic = "replicatedTopic",  
                brokerList="%BrokerList%",
                username = "$ConnectionString", 
                password = "ReplicatedEventHubConnectionString",
                authenticationMode = BrokerAuthenticationMode.PLAIN,
                sslCaLocation = "confluent_cloud_cacert.pem", // Enable this line for windows.  
                protocol = BrokerProtocol.SASLSSL
            )  OutputBinding<KafkaEntity[]> output,             
            final ExecutionContext context) {

            int numEvents = kafkaEvents.size();
            context.getLogger().info("replicating " + numEvents + " messages");
            
            // Create an array for the replicated events
            KafkaEntity[] replicatedEvents = new KafkaEntity[numEvents];

            Gson gson = new Gson();            
            for (int i = 0; i < kafkaEvents.size(); i++) {                
                KafkaEntity kevent = gson.fromJson(kafkaEvents.get(i),KafkaEntity.class);
                context.getLogger().info("Java Kafka trigger function called for message: " + kevent.Value);
                context.getLogger().info("Headers for the message:");
                            
                // Decode the header value before setting it in the replicated event header
                for (int h = 0; h < kevent.Headers.length; h++){
                    String decodedValue = new String(Base64.getDecoder().decode(kevent.Headers[h].Value));
                    context.getLogger().info("Key:" + kevent.Headers[h].Key + " Value:" + decodedValue); 
                    kevent.Headers[h].Value = decodedValue;
                }

                // Add the replicated event to the array for the output binding
                replicatedEvents[i] = kevent;
            }               
          
            output.setValue(replicatedEvents);
    }

}


