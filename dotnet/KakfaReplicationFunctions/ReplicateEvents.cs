using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace KakfaReplicationFunctions
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

                var kafkaEvent = new KafkaEventData<string>()
                {
                    Value = eventData.Value
                };

                await replicatedEvents.AddAsync(kafkaEvent);
            }
            
        }
    }
}
