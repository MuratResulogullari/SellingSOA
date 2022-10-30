using EventBus.Base.Events;
using EventBus.Base.SubManagers;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.Base.Abstraction
{
    public abstract class BaseEventBus : IEventBus
    {
        public readonly IServiceProvider serviceProvider;
        public readonly IEventBusSubscriptionManager eventBusSubscriptionManager;
        private EventBusConfig _eventBusConfig;
        public BaseEventBus(IServiceProvider serviceProvider,EventBusConfig eventBusConfig)
        {
            this.serviceProvider = serviceProvider;
            _eventBusConfig= eventBusConfig;
            eventBusSubscriptionManager = new InMemoryEventBusSubscriptionManager(ProcessEventName);
        }
        public virtual string ProcessEventName(string eventName)
        {
            if (_eventBusConfig.DeleteEventPrefix)
                eventName = eventName.TrimStart(_eventBusConfig.EventNamePrefix.ToArray());
            if (_eventBusConfig.DeleteEventSuffix)
                eventName= eventName.TrimEnd(_eventBusConfig.EventNameSuffix.ToArray());
            return eventName;
        }
        public virtual string GetSubName(string eventName)
        {
            return $"{_eventBusConfig.SubscriberClientAppName}.{ProcessEventName(eventName)}";
        }
        public virtual void Dispose()
        {
            _eventBusConfig = null;
        }
        public async Task<bool> ProcessEvent(string eventName,string message)
        {
            var processed = false;
            eventName = ProcessEventName(eventName);
            if (eventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptions =eventBusSubscriptionManager.GetHandlersForEvent(eventName);
                using (var scope =serviceProvider.CreateScope())
                {
                    foreach (var subscription in subscriptions)
                    {
                        var handler = serviceProvider.GetService(subscription.HandlerType);
                        if (handler == null) continue;

                        var eventType = eventBusSubscriptionManager.GetEventTypeByName($"{_eventBusConfig.EventNamePrefix}{eventName}{_eventBusConfig.EventNameSuffix}");
                        var integrationEvent = JsonConvert.DeserializeObject(message, eventType);
                        
                        var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                        await (Task)concreteType.GetMethod("Handle").Invoke(handler,new object[] { integrationEvent });
                    }
                    processed = true;
                }
            }
            return processed;
        }
        public abstract void Publish(IntegrationEvent @event);

        public abstract void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;

        public abstract void UnSubscibe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;
    }
}
