RabbitMQ Message Bus for node-cqrs
==================================

## Usage

```bash
npm install bitbucket:cqrs/node-cqrs bitbucket:cqrs/node-cqrs-rabbitmq --save
```

A configured instance of RabbitMqBus must be passed in as "bus" option to the EventStore constructor:

```javascript
const EventStore = require('node-cqrs').EventStore;
const InMemoryEventStorage = require('node-cqrs').InMemoryEventStorage;
const RabbitMqBus = require('node-cqrs-rabbitmq');

const storage = new InMemoryEventStorage();
const bus = new RabbitMqBus({
	connectionString: 'amqp://username:password@localhost?heartbeat=30',
	queue: 'my.events',
	durable: false,
	appId: 'my-app-id'
});

const eventStore = new EventStore({ storage, bus });

eventStore.commit([
	{ aggreateId: 1, aggregateVersion: 1, type: 'somethingHappened', payload: {} }
]);
```


The same, using DI container: 

```javascript
const EventStore = require('node-cqrs').EventStore;
const InMemoryEventStorage = require('node-cqrs').InMemoryEventStorage;
const Container = require('node-cqrs').Container;
const RabbitMqBus = require('node-cqrs-rabbitmq');

const c = new Container();

c.register(InMemoryEventStorage, 'storage');
c.register(() => new RabbitMqBus({
	connectionString: 'amqp://username:password@localhost?heartbeat=30',
	queue: 'my.events',
	durable: false,
	appId: 'my-app-id'
}), 'bus');

c.register(EventStore, 'eventStore');

c.eventStore.commit([
	{ aggreateId: 1, aggregateVersion: 1, type: 'somethingHappened', payload: {} }
]);
```

### Constructor Options

-	connectionString
-	queue
-	queuePrefix
-	durable
-	prefetch
-	appId
