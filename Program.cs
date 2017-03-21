using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace CSharpLambdaFunction
{
    public class LambdaHandler
    {
        public static void Main()
        {
            Task.Run(async () =>
            {
                Stream result;
                using (var stream = new FileStream($"{Directory.GetCurrentDirectory()}\\testData.json", FileMode.Open))
                {
                    result = await new LambdaHandler().LogHandler(stream);
                }
                using (var reader = new StreamReader(result))
                {
                    var records = reader.ReadToEnd();

                    Console.WriteLine(records);
                }
            }).GetAwaiter().GetResult();
        }

        public async Task<Stream> LogHandler(Stream inputStream)
        {
            LogEntry entry;
            FirehoseInput input;
            using (var reader = new StreamReader(inputStream))
            {
                input = Deserialize<FirehoseInput>(reader);
            }

            foreach (var record in input.records)
            {
                record.result = "Ok";
                using (var memStream = new MemoryStream(Convert.FromBase64String(record.data)))
                {
                    using (var reader = new StreamReader(memStream))
                    {
                        entry = Deserialize<LogEntry>(reader);
                    }
                }

                var reqTask = SendToS3AndGetLink(entry.Request);
                var resTask = SendToS3AndGetLink(entry.Response);

                entry.Request = await reqTask;
                entry.Response = await resTask;

                var data = ConvertToStream(entry);

                record.data = Convert.ToBase64String(data.ToArray());
            }

            return ConvertToStream(input);
        }

        private T Deserialize<T>(TextReader reader)
        {
            var serializer = new Newtonsoft.Json.JsonSerializer();

            return (T)serializer.Deserialize(reader, typeof(T));
        }

        private MemoryStream ConvertToStream<T>(T logEntry)
        {
            var serializer = new Newtonsoft.Json.JsonSerializer();

            var memStream = new MemoryStream();
            var writer = new StreamWriter(memStream);
            serializer.Serialize(writer, logEntry);

            writer.Flush();

            memStream.Position = 0;

            return memStream;
        }

        private Stream ConvertToStream(string value)
        {
            var memStream = new MemoryStream();
            var writer = new StreamWriter(memStream);

            writer.Write(value);
            writer.Flush();

            return memStream;
        }

        private async Task<string> SendToS3AndGetLink(string value)
        {
            var s3 = new AmazonS3Client(Amazon.RegionEndpoint.USWest2); //define your own region here

            var putRequest = new PutObjectRequest();

            var key = Guid.NewGuid().ToString().Replace("-", string.Empty);
            putRequest.BucketName = GetBucketName();
            putRequest.Key = key;
            putRequest.InputStream = ConvertToStream(value);
            putRequest.ContentType = "application/json";

            var response = await s3.PutObjectAsync(putRequest);

            var urlRequest = new GetPreSignedUrlRequest();
            urlRequest.BucketName = GetBucketName();
            urlRequest.Expires = DateTime.UtcNow.AddYears(2);
            urlRequest.Key = key;

            return s3.GetPreSignedURL(urlRequest);
        }

        private string GetBucketName()
        {
            return "cloudncode-logs";
        }
    }

    public class FirehoseInput
    {
        public string invocationId { get; set; }
        public string deliveryStreamArn { get; set; }
        public string region { get; set; }
        public List<FirehoseRecord> records { get; set; }
    }
    public class FirehoseRecord
    {
        public string recordId { get; set; }
        public string result { get; set; }
        public string data { get; set; }
    }

    public class LogEntry
    {
        public string RequestId { get; set; }
        public string SessionId { get; set; }
        public string Timestamp { get; set; }
        public string UserId { get; set; }
        public string ServerIp { get; set; }
        public string Message { get; set; }
        public string Level { get; set; }
        public string Category { get; set; }
        public string Request { get; set; }
        public string Response { get; set; }
    }
}