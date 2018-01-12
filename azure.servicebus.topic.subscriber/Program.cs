using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using System;
using System.Threading.Tasks;

namespace azure.servicebus.topic.subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            var s = new SubManager();
            s.Go().Wait();
            Console.ReadLine();
        }
    }

    public class SubManager
    {
        public async Task Go()
        {
            var sb = "Endpoint=sb://jpdallstatesb.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=c6+wfnt4YZAHBmmHkXzeQiasRv4a7bl/sqHU4zjhCbo=";
            var topic = "claims";
            var allMessages = new SubscriptionClient(sb, topic, "all", ReceiveMode.PeekLock);
            var accidentMessages = new SubscriptionClient(sb, topic, "accident", ReceiveMode.PeekLock);

            await accidentMessages.RemoveRuleAsync("accident-filter");
            await accidentMessages.AddRuleAsync(new RuleDescription()
            {
                Name = "accident-filter",
                Filter = new CorrelationFilter() { Label = "Accident" }
            });

            allMessages.RegisterMessageHandler(async (msg, cancellationToken) =>
            {
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine($"all subscription recieved message {msg.MessageId} {msg.UserProperties["ClaimType"]}");
                Console.WriteLine(System.Text.Encoding.UTF8.GetString(msg.Body));
                await allMessages.CompleteAsync(msg.SystemProperties.LockToken);
            }, new MessageHandlerOptions(e => { Console.WriteLine(e.Exception); return Task.CompletedTask; }) { AutoComplete = false });

            accidentMessages.RegisterMessageHandler(async (msg, cancellationToken) =>
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"accident subscription recieved message {msg.MessageId} {msg.UserProperties["ClaimType"]}");
                Console.WriteLine(System.Text.Encoding.UTF8.GetString(msg.Body));
                await accidentMessages.CompleteAsync(msg.SystemProperties.LockToken);
                Console.ForegroundColor = ConsoleColor.White;
            }, new MessageHandlerOptions(e => { Console.WriteLine(e.Exception); return Task.CompletedTask; }) { AutoComplete = false });
        }
    }

    public class AllSubscriber : TopicSubscriber
    {
        public AllSubscriber(string serviceBus, string topic) : base(serviceBus, topic, "all") { }
    }

    public class AccidentSubscriber : TopicSubscriber
    {
        public AccidentSubscriber(string serviceBus, string topic) : base(serviceBus, topic, "accident") { }

    }

    public class TopicSubscriber
    {
        private string _sb;
        private string _topic;
        private string _sub;

        public TopicSubscriber(string serviceBus, string topic, string subscription)
        {
            _sb = serviceBus;
            _sub = subscription;
            _topic = topic;
        }

        public void Go()
        {
            var receiver = new SubscriptionClient(_sb, _topic, _sub);

        }
    }
}
