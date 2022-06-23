using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Threading.Tasks;

namespace KafkaReplicationFunctions
{
    public class ReplicateEvents
    {
        [FunctionName("ReplicateEvents")]
        public static async Task Run(
            [KafkaTrigger("BrokerList",
                          "topic",
                          Username = "$ConnectionString",
                          Password = "%KafkaPassword%",
                          Protocol = BrokerProtocol.SaslSsl,
                          AuthenticationMode = BrokerAuthenticationMode.Plain,
                          SslCaLocation = "confluent_cloud_cacert.pem",
                          ConsumerGroup = "$Default")] KafkaEventData<string>[] events,
            [Kafka("ReplicatedBrokerList", 
                   "replicatedTopic", 
                   Username ="$ConnectionString", 
                   Password = "%ReplicatedKafkaPassword%", 
                    Protocol = BrokerProtocol.SaslSsl,
                   AuthenticationMode = BrokerAuthenticationMode.Plain)] IAsyncCollector<KafkaEventData<string>> replicatedEvents,
            ILogger log)
        {
            log.LogInformation($"{events.Length} received");
            

            foreach (KafkaEventData<string> eventData in events)
            {
                log.LogInformation($"C# Kafka trigger function processed a message: {eventData.Value}");

                // Copy the value
                var kafkaEvent = new KafkaEventData<string>()
                {                    
                    Value = eventData.Value
                };

                // Copy the headers
                foreach (var h in eventData.Headers)
                {
                    kafkaEvent.Headers.Add(h.Key, h.Value);
                }

                // Add replication metadata
                DateTime enqueuedUtc = eventData.Timestamp.ToUniversalTime();
                kafkaEvent.Headers.Add("repl-enqueued-time", Encoding.UTF8.GetBytes(enqueuedUtc.ToString("yyyy-MM-dd hh:mm:ss.fff")));
                kafkaEvent.Headers.Add("repl-offset", Encoding.UTF8.GetBytes(eventData.Offset.ToString()));
                kafkaEvent.Headers.Add("repl-partition", Encoding.UTF8.GetBytes(eventData.Partition.ToString()));
                kafkaEvent.Headers.Add("repl-topic", Encoding.UTF8.GetBytes(eventData.Topic));
                
                await replicatedEvents.AddAsync(kafkaEvent);
            }
            
        }
    }
}
