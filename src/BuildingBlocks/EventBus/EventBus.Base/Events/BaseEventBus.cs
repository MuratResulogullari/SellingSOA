using EventBus.Base.Abstraction;
using EventBus.Base.SubManagers;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace EventBus.Base.Events
{
    public abstract class BaseEventBus : IEventBus
    {
        public readonly IServiceProvider serviceProvider;
        public readonly IEventBusSubscriptionManager eventBusSubscriptionManager;
        public EventBusConfig EventBusConfig { get; set; }

        public BaseEventBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider)
        {
            EventBusConfig = eventBusConfig;
            this.serviceProvider = serviceProvider;
            eventBusSubscriptionManager = new InMemoryEventBusSubscriptionManager(ProcessEventName);
        }

        public virtual string ProcessEventName(string eventName)
        {
            if (EventBusConfig.DeleteEventPrefix)
                eventName = eventName.TrimStart(EventBusConfig.EventNamePrefix.ToArray());
            if (EventBusConfig.DeleteEventSuffix)
                eventName = eventName.TrimEnd(EventBusConfig.EventNameSuffix.ToArray());
            return eventName;
        }

        public virtual string GetSubName(string eventName)
        {
            return $"{EventBusConfig.SubscriberClientAppName}.{ProcessEventName(eventName)}";
        }

        public virtual void Dispose()
        {
            EventBusConfig = null;
            eventBusSubscriptionManager.Clear();
        }

        public async Task<bool> ProcessEvent(string eventName, string message)
        {
            var processed = false;
            eventName = ProcessEventName(eventName);
            if (eventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptions = eventBusSubscriptionManager.GetHandlersForEvent(eventName);
                using (var scope = serviceProvider.CreateScope())
                {
                    foreach (var subscription in subscriptions)
                    {
                        var handler = serviceProvider.GetService(subscription.HandlerType);
                        if (handler == null) continue;

                        var eventType = eventBusSubscriptionManager.GetEventTypeByName($"{EventBusConfig.EventNamePrefix}{eventName}{EventBusConfig.EventNameSuffix}");
                        var integrationEvent = JsonConvert.DeserializeObject(message, eventType);

                        var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                        await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { integrationEvent });
                    }
                    processed = true;
                }
            }
            return processed;
        }

        public abstract void Publish(IntegrationEvent @event);

        public abstract void Subscribe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;

        public abstract void UnSubscibe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;
    }
}