using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;

namespace EventBus.AzureServiceBus
{
    public class EventBusServiceBus : BaseEventBus
    {
        private ITopicClient _topicClient;

        private ManagementClient _managementClient;
        private ILogger _logger;

        public EventBusServiceBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider) : base(eventBusConfig, serviceProvider)
        {
            _managementClient = new ManagementClient(eventBusConfig.EventBusConnectionString);
            _topicClient = CreateTopicClient();
            _logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
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
            string eventName = @event.GetType().Name; //example: OrderCreatedIntegrationEvent

            eventName = ProcessEventName(eventName); // example: orderCreated

            string eventStr = JsonConvert.SerializeObject(@event);
            byte[] bodyArr = Encoding.UTF8.GetBytes(eventStr);

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
            string eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);
            if (!eventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptionClient = CreateSubscriptionClientIfNotExists(eventName);
                RegisterSubscriptionClientMessageHandler(subscriptionClient);
            }
            _logger.LogInformation("Subscribing to event {EventName} with {EventHandler}", eventName, typeof(TH).Name);
            eventBusSubscriptionManager.AddSubscription<T, TH>();
        }

        public override void UnSubscibe<T, TH>()
        {
            string eventName = typeof(T).Name;
            try
            {
                var subscriptionClient = CreateSubscriptionClient(eventName);
                subscriptionClient
                    .RemoveRuleAsync(eventName)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (MessagingEntityNotFoundException)
            {
                _logger.LogWarning("The messasging entity {eventName} Could not be found.", eventName);
            }
            _logger.LogInformation("Unsubscribing from event {EventName}", eventName);
            eventBusSubscriptionManager.RemoveSubscription<T, TH>();
        }

        private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
        {
            subscriptionClient.RegisterMessageHandler(
                async (message, token) =>
                {
                    string eventName = $"{message.Label}";
                    string messageData = Encoding.UTF8.GetString(message.Body);

                    //Complete the message so that it is not received again.
                    if (await ProcessEvent(ProcessEventName(eventName), messageData))
                    {
                        await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                    }
                },
                new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false });
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            var ex = exceptionReceivedEventArgs.Exception;
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            _logger.LogError(ex, "ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Message, context);
            return Task.CompletedTask;
        }

        private ISubscriptionClient CreateSubscriptionClientIfNotExists(String eventName)
        {
            SubscriptionClient subClient = CreateSubscriptionClient(eventName);
            bool exists = _managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
            if (!exists)
            {
                _managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
                RemoveDefaultRule(subClient);
            }
            CreateRuleIfNotExists(ProcessEventName(eventName), subClient);
            return subClient;
        }

        private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
        {
            bool ruleExists;
            try
            {
                RuleDescription rule = _managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetResult();
                ruleExists = rule != null;
            }
            catch (MessagingEntityNotFoundException)
            {
                //Azure Managment Client doesn't RuleExists method
                ruleExists = false;
            }

            if (!ruleExists)
            {
                subscriptionClient.AddRuleAsync(new RuleDescription
                {
                    Filter = new CorrelationFilter { Label = eventName },
                    Name = eventName
                }).GetAwaiter().GetResult();
            }
        }

        private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
        {
            try
            {
                subscriptionClient
                    .RemoveRuleAsync(RuleDescription.DefaultRuleName)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (MessagingEntityAlreadyExistsException)
            {
                _logger.LogWarning("The messaging entity {DefaultTuleName} Could not be found.", RuleDescription.DefaultRuleName);
            }
        }

        private SubscriptionClient CreateSubscriptionClient(string eventName)
        {
            return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
        }

        public override void Dispose()
        {
            base.Dispose();
            _topicClient.CloseAsync().GetAwaiter().GetResult();
            _managementClient.CloseAsync().GetAwaiter().GetResult();
            _topicClient = null;
            _managementClient = null;
        }
    }
}