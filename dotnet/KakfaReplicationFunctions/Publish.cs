using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.Kafka;

namespace KakfaReplicationFunctions
{
    public static class Publish
    {
        [FunctionName("Publish")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [Kafka("BrokerList",
                   "replicatedTopic",
                   Username ="$ConnectionString",
                   Password = "%ReplicatedKafkaPassword%",  
                   Protocol = BrokerProtocol.SaslSsl,
                   SslCaLocation = "confluent_cloud_cacert.pem",
                   AuthenticationMode = BrokerAuthenticationMode.Plain)] IAsyncCollector<KafkaEventData<string>> replicatedEvents,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            try
            {                
                var kafkaEvent = new KafkaEventData<string>()
                {
                    Value = $"Created on: {DateTime.UtcNow.ToLongDateString()}",
                };

                await replicatedEvents.AddAsync(kafkaEvent);
            }
            catch (Exception ex) 
            {
                throw new Exception("Exception occured. Make sure topic exist and validate credentials.", ex);
            }
            
            return new OkResult();
        }
    }
}
