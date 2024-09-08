using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Carlinx.CLXGetGovIL
{
    public class CLXGetGovIL
    {
        private readonly ILogger _logger;

        public CLXGetGovIL(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CLXGetGovIL>();
        }

        [Function("CLXGetGovIL")]
        public async Task RunAsync([TimerTrigger("0 30 9 * * *")] TimerInfo myTimer, FunctionContext context)
        {
            using var httpClient = new HttpClient();

        // Define a list of web services with URL and name
            var webServices = new List<WebServiceInfo>
            {
                new WebServiceInfo { UID = "d00812f4-58c5-4ce8-b16c-ac13ae52f9d8", Name = "GOVIL01" },
                new WebServiceInfo { UID = "142afde2-6228-49f9-8a29-9b6c3a0cbe40", Name = "GOVIL02" },
                new WebServiceInfo { UID = "5e87a7a1-2f6f-41c1-8aec-7216d52a6cf6", Name = "GOVIL03" },
                new WebServiceInfo { UID = "053cea08-09bc-40ec-8f7a-156f0677aff3", Name = "GOVIL04A" },
                new WebServiceInfo { UID = "03adc637-b6fe-402b-9937-7c3d3afc9140", Name = "GOVIL04B" },
                new WebServiceInfo { UID = "0866573c-40cd-4ca8-91d2-9dd2d7a492e5", Name = "GOVIL05" },
                new WebServiceInfo { UID = "56063a99-8a3e-4ff4-912e-5966c0279bad", Name = "GOVIL06" },
                new WebServiceInfo { UID = "bb2355dc-9ec7-4f06-9c3f-3344672171da", Name = "GOVIL07" },
                new WebServiceInfo { UID = "2c33523f-87aa-44ec-a736-edbb0a82975e", Name = "GOVIL08A" },
                new WebServiceInfo { UID = "36bf1404-0be4-49d2-82dc-2f1ead4a8b93", Name = "GOVIL08B" },
                new WebServiceInfo { UID = "851ecab1-0622-4dbe-a6c7-f950cf82abf9", Name = "GOVIL09A" },
                new WebServiceInfo { UID = "4e6b9724-4c1e-43f0-909a-154d4cc4e046", Name = "GOVIL09B" },
                new WebServiceInfo { UID = "c8b9f9c8-4612-4068-934f-d4acd2e3c06e", Name = "GOVIL10" },
                new WebServiceInfo { UID = "39f455bf-6db0-4926-859d-017f34eacbcb", Name = "GOVIL12" },
                new WebServiceInfo { UID = "eb74ad8c-ffcd-43bb-949c-2244fc8a8651", Name = "GOVIL13" }   
            };

        // Fetch data from each web service in batches
            string webServiceUrl= "https://data.gov.il/api/3/action/datastore_search?resource_id=";

            string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=govildailybatch;AccountKey=7GhCXGfDdMpYEp8wKj3GJAWrgFCnUoFlD4su2y7yQGXCls9yKy/LJP4zhL/+nfAYXSj6YTiUVdHp+AStjayF/Q==;EndpointSuffix=core.windows.net";
            //                                     //DefaultEndpointsProtocol=https;AccountName=govildailybatch;AccountKey=MTeIT7yFF5TlDz3Zvg0LfbKR6jEI8Z6JmGzMa3p6QvhNVQ3oJ1NB4eQsykjAiZOiEKUPVCY9OgGd+AStPmuxxA==;EndpointSuffix=core.windows.net";

            foreach (var serviceInfo in webServices)
            {
                var batches = await FetchDataInBatches(httpClient, webServiceUrl + serviceInfo.UID, 50000, serviceInfo, storageAccountConnectionString, context);

                // Upload each batch to Azure Blob Storage with date and service folder structure
            }

             _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }
            
        }

        static async Task<bool> FetchDataInBatches(HttpClient httpClient, string url, int batchSize, WebServiceInfo serviceInfo, string storageAccountConnectionString, FunctionContext context)
        {
            //var batches = new List<string>();
            int skip = 0;
            int totalRecords = int.MaxValue;

            var logger = context.GetLogger("FetchDataInBatches");

            while (skip < totalRecords)
            {
                //skip += batchSize;

                logger.LogInformation($"Getting {batchSize} records from {serviceInfo.Name}. Current offset is {skip}");
                // Fetch a batch of data from the web service
                var batch = await FetchBatchFromWebService(httpClient, url, skip, batchSize);

                totalRecords = JObject.Parse(batch)["result"].Value<int>("total");

                // if (string.IsNullOrEmpty(batch))
                //     break; // No more data available
                
                //batches.Add(batch);
                var currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
                var blobName = serviceInfo.Name + "/" + currentDate +"/" + "batch_" + Guid.NewGuid().ToString() +".json";
                await UploadDataToBlobStorage(batch, storageAccountConnectionString, "datagov", blobName);

                logger.LogInformation($"Created blob name: {blobName}");

                skip += batchSize;
            }

            return true;
        }

        static async Task<string> FetchBatchFromWebService(HttpClient httpClient, string url, int skip, int batchSize)
        {
            // Fetch a batch of data from the web service
            var response = await httpClient.GetAsync($"{url}&offset={skip}&limit={batchSize}");
            response.EnsureSuccessStatusCode();

            // Read the content
            return await response.Content.ReadAsStringAsync();
        }

        static async Task UploadDataToBlobStorage(string data, string connectionString, string containerName, string blobName)
        {
            // Create a BlobServiceClient object which will be used to create a container client
            var blobServiceClient = new BlobServiceClient(connectionString);

            // Create a container client object
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            // Create the container if it does not exist
            await containerClient.CreateIfNotExistsAsync();

            // Get a reference to a blob
            var blobClient = containerClient.GetBlobClient(blobName);

            // Convert data string to a byte array
            byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(data);

            // Upload data to the blob
            using var stream = new MemoryStream(byteArray);
            await blobClient.UploadAsync(stream, true);
        }

        class WebServiceInfo
        {
            public string UID { get; set; }
            public string Name { get; set; }
        }
    }
 }