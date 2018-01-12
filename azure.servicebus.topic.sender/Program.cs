using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;

namespace azure.servicebus.topic.sender
{
    class Program
    {
        static void Main(string[] args)
        {
            var m = new MessageCreator();
            m.Send().Wait();
            Console.WriteLine("All finished");
            Console.ReadLine();
        }
    }

    public class MessageCreator
    {
        public async Task Send(int count = 10)
        {
            var sbkey = "Endpoint=sb://jpdallstatesb.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=nA/t52ONyg7mWAG7VU93Fe2sxPxC5bPZj2TqiS/XoHE=";
            var sender = new MessageSender(sbkey, "claims");
            var r = new Random();
            var types = new List<string>() { "Accident", "Property", "Theft", "Injury" };

            for (var i = 0; i < count; i++)
            {
                var thing = new { Id = Guid.NewGuid(), ClaimType = types[r.Next(0, types.Count - 1)], CustomerId = Math.Round(DateTime.Now.Millisecond * Math.PI, 0) };
                var data = JsonConvert.SerializeObject(thing);
                var msg = new Microsoft.Azure.ServiceBus.Message(System.Text.Encoding.UTF8.GetBytes(data));
                msg.UserProperties.Add("ClaimType", thing.ClaimType);
                msg.Label = thing.ClaimType;
                await sender.SendAsync(msg);
                Console.WriteLine($"Sent message {thing.Id}");
            }
        }
    }
}
