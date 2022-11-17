using EventBus.Base.Events;

namespace EventBus.Base.Abstraction
{
    public interface IEventBus
    {
        void Publish(IntegrationEvent @event);

        void Subscribe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;

        void UnSubscibe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;
    }
}