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

namespace KafkaReplicationFunctions
{
    public static class Publish
    {
        [FunctionName("Publish")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [Kafka("BrokerList",
                   "topic",
                   Username ="$ConnectionString",
                   Password = "%KafkaPassword%",  
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
                    // Test value
                    Value = $"Created on: {DateTime.UtcNow.ToLongDateString()} {DateTime.UtcNow.ToLongTimeString()}",
                };

                // Add some headers
                kafkaEvent.Headers.Add("test-header1", System.Text.Encoding.UTF8.GetBytes("dotnet-test1"));
                kafkaEvent.Headers.Add("test-header2", System.Text.Encoding.UTF8.GetBytes("dotnet-test2"));

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
