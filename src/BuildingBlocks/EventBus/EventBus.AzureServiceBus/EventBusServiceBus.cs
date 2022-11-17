using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;
using System.Text;

namespace EventBus.AzureServiceBus
{
    public class EventBusServiceBus : BaseEventBus
    {
        private ITopicClient _topicClient;

        private ManagementClient _managementClient;

        public EventBusServiceBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider) : base(eventBusConfig, serviceProvider)
        {
            _managementClient = new ManagementClient(eventBusConfig.EventBusConnectionString);
            _topicClient = CreateTopicClient();
        }

        private ITopicClient CreateTopicClient()
        {
            if (_topicClient == null || _topicClient.IsClosedOrClosing)
                _topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, RetryPolicy.Default);
            // Ensure that topic already exists
            if (!_managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
                _managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();
            return _topicClient;
        }

        public override void Publish(IntegrationEvent @event)
        {
            var eventName = @event.GetType().Name; //example: OrderCreatedIntegrationEvent

            eventName = ProcessEventName(eventName); // example: orderCreated

            var eventStr = JsonConvert.SerializeObject(@event);
            var bodyArr = Encoding.UTF8.GetBytes(eventStr);

            Message message = new Message()
            {
                MessageId = Guid.NewGuid().ToString(),
                Body = null,
                Label = eventName
            };
            _topicClient.SendAsync(message).GetAwaiter().GetResult();
        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);
            if (!eventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
            {
            }
            else
            {
            }
        }

        public override void UnSubscibe<T, TH>()
        {
            throw new NotImplementedException();
        }
    }
}