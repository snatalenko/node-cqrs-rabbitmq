'use strict';

const amqp = require('amqplib');
const uuid = require('uuid');
const createLogger = require('debug');
const reconnect = require('./reconnect');

// http://www.squaremobius.net/amqp.node/channel_api.html#channel_publish
const DEFAULT_ROUTE = '';
const DEFAULT_MESSAGE_PROPS = {
	persistent: true,
	contentType: 'application/json',
	contentEncoding: 'utf8'
};

const _connectionPromise = Symbol('connection');
const _subChannelPromise = Symbol('sub channel');
const _subChannelPrefetch = Symbol('sub channel prefetch');
const _pubChannelPromise = Symbol('pub channel');
const _queueName = Symbol('queue');
const _handlers = Symbol('consumers');
const _queueOptions = Symbol('options');


/**
 * Creates human-readable message descriptor for debug messages
 *
 * @param {{fields:object, properties:object}} message
 * @returns {string}
 */
function descriptor(message) {
	if (!message) throw new TypeError('message argument required');

	return [
		message.fields && message.fields.exchange,
		message.properties && message.properties.messageId || message.fields && message.fields.deliveryTag,
		message.fields && message.fields.redelivered ? 'redelivered' : undefined
	].filter(v => !!v).join('-');
}


/**
 * Publishes a message to the given MQ channel/exchange
 *
 * @param {object} channel
 * @param {{type:string}} message
 * @param {string} appId
 * @param {{debug: function, info: function}} logger
 */
function publish(channel, message, appId, logger) {
	if (!channel) throw new TypeError('channel argument required');
	if (!message) throw new TypeError('message argument required');
	if (!message.type) throw new TypeError('message.type argument required');
	if (appId && typeof appId !== 'string') throw new TypeError('appId argument, when provided, must be a String');
	if (typeof logger !== 'object' || !logger) throw new TypeError('logger argument must be an Object');


	return new Promise(function (resolve, reject) {

		const exchange = message.type;
		const content = new Buffer(JSON.stringify(message), DEFAULT_MESSAGE_PROPS.contentEncoding);
		const properties = Object.assign({}, DEFAULT_MESSAGE_PROPS, {
			timestamp: Date.now(),
			appId,
			type: message.type,
			messageId: message.id ? message.id.toString() :
				message._id ? message._id.toString() : undefined,
			correlationId: message.sagaId ? message.sagaId.toString() :
				message.aggregateId ? message.aggregateId.toString() : undefined
		});

		const writeResult = channel.publish(exchange, DEFAULT_ROUTE, content, properties, function (err, ok) {
			if (err) reject(err);
			else resolve(ok);
		});

		if (!writeResult)
			throw new Error('Queue write did not succeed');

	}).then(r => {
		logger.debug(`'${message.type}' acknowledged by the queue`);
		return r;
	}, err => {
		logger.info(`'${message.type}' could NOT be acknowledged by the queue: ${err && err.message || err}`);
		throw err;
	});
}


/**
 * Decodes the received message content in accordance with the content-type
 * @param  {Buffer} content     Received message content
 * @param  {String} contentType Received message content type (e.g. application/json)
 * @return {Promise}            Promise resolving to decoded payload
 */
function decodePayload(content, contentType) {
	return new Promise(function (resolve, reject) {
		if (!content || !content.length) {
			return resolve(undefined);
		} else if (contentType === 'application/json') {
			return resolve(JSON.parse(content.toString()));
		} else {
			throw new TypeError(`Unexpected content-type: ${contentType}`);
		}
	});
}


/**
 * Creates queue
 *
 * @template TChannel
 * @param {TChannel} channel
 * @param {string} queueName
 * @param {{durable:boolean, exclusive:boolean, deadLetterExchange:string}} options
 * @param {{debug: function, info: function}} logger
 * @returns {PromiseLike<{channel:TChannel, queueName:string}>}
 */
function assertQueue(channel, queueName, options, logger) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof queueName !== 'string' || !queueName.length) throw new TypeError('queueName argument must be a non-empty String');
	if (!options) throw new TypeError('options argument required');
	if (typeof logger !== 'object' || !logger) throw new TypeError('logger argument must be an Object');

	logger.debug(`asserting queue '${queueName}'...`);

	return Promise.all([
		channel.assertQueue(queueName, options),
		options.deadLetterExchange ? assertDeadLetterExchange(channel, options.deadLetterExchange, logger) : undefined
	]).then(r => {
		if (r[0]) {
			logger.debug(`queue '${r[0].queue}' asserted, ${r[0].messageCount} messages, ${r[0].consumerCount} consumers`);
			queueName = r[0].queue;
		}
		return { channel, queueName };
	});
}

/**
 * Asserts exchange and binds a ginen queue to it
 *
 * @template TChannel
 * @param {TChannel} channel
 * @param {string} queueName
 * @param {string} exchangeName
 * @param {{debug: function, info: function}} logger
 * @returns {PromiseLike<TChannel>}
 */
function assertExchange(channel, queueName, exchangeName, logger) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof queueName !== 'string' || !queueName.length) throw new TypeError('queueName argument must be a non-empty String');
	if (typeof exchangeName !== 'string' || !exchangeName.length) throw new TypeError('exchangeName argument must be a non-empty String');
	if (typeof logger !== 'object' || !logger) throw new TypeError('logger argument must be an Object');

	logger.debug(`asserting exchange '${exchangeName}' monitored by '${queueName}'...`);

	return Promise.all([
		channel.assertExchange(exchangeName, 'fanout'),
		channel.bindQueue(queueName, exchangeName /*, pattern */),
	]).then(() => channel);
}

/**
 * Asserts dead letter exchange with a durable queue for failed messages
 *
 * @param {TChannel} channel
 * @param {string} deadLetterExchange
 * @param {{debug: function, info: function}} logger
 * @returns {PromiseLike<TChannel>}
 */
function assertDeadLetterExchange(channel, deadLetterExchange, logger) {
	if (!channel) throw new TypeError('channel argument required');
	if (typeof deadLetterExchange !== 'string' || !deadLetterExchange.length)
		throw new TypeError('deadLetterExchange argument must be a non-empty String');
	if (typeof logger !== 'object' || !logger) throw new TypeError('logger argument must be an Object');

	return Promise.resolve(channel)
		.then(channel => assertQueue(channel, deadLetterExchange, { durable: true }, logger))
		.then(({channel}) => assertExchange(channel, deadLetterExchange, deadLetterExchange, logger));
}

/**
 * Subscribes the channel to the given queue
 *
 * @param {string} queueName
 * @param {any} handler
 * @param {{debug: function, info: function}} logger
 * @returns {function(object):PromiseLike<object>}
 */
function assertConsumer(channel, queueName, handler, logger) {

	logger.debug(`subscribing to queue '${queueName}'...`);

	return channel.consume(queueName, handler).then(response => {
		logger.info(`consumer set up as ${response.consumerTag}`);
		return channel;
	});
}


module.exports = class RabbitMqBus {

	get queueName() {
		return this[_queueName];
	}

	/**
	 * Creates an instance of RabbitMqBus
	 *
	 * @param {{connectionString:string, appId:string, queue:string, queuePrefix:string, durable: boolean, prefetch:number}} options
	 */
	constructor(options) {
		if (!options) throw new TypeError('options argument required');
		if (typeof options.connectionString !== 'string' || !options.connectionString.length)
			throw new TypeError('options.connectionString argument must be a non-empty String');
		if (options.durable && !options.queue)
			throw new TypeError('options.queue argument is required, when options.durable is true');

		this._appId = options.appId || undefined;
		this._logger = {
			debug: createLogger('cqrs:debug:rabbitmq' + (this._appId ? ':' + this._appId : '')),
			info: createLogger('cqrs:info:rabbitmq' + (this._appId ? ':' + this._appId : ''))
		};
		this._handle = this._handle.bind(this);

		this[_queueName] = options.queue || options.queuePrefix && (options.queuePrefix + uuid.v4().replace(/-/g, '')) || undefined;
		this[_queueOptions] = {
			// will survive broker restarts
			durable: 'durable' in options ? options.durable : !!options.queue,
			// scoped to connection
			exclusive: !options.queue,
			// an exchange to which messages discarded from the queue will be resent
			deadLetterExchange: this.queueName ? this.queueName + '.failed' : undefined
		};

		this[_subChannelPrefetch] = options.prefetch || undefined;

		this._createConnection(options.connectionString);
		this._createPubChannel();
		this._createSubChannel();
	}

	_createConnection(connectionString) {
		if (typeof connectionString !== 'string' || !connectionString.length) throw new TypeError('connectionString argument must be a non-empty String');

		this._logger.debug(`connecting to ${reconnect.mask(connectionString)}...`);
		this[_connectionPromise] = reconnect(() => amqp.connect(connectionString), null, null, this._logger.info);
		this[_connectionPromise].then(cn => {
			this._logger.info(`connected to ${reconnect.mask(connectionString)}`);
		});
		return this[_connectionPromise];
	}

	_createPubChannel() {
		this._logger.debug('establishing publish channel...');
		return this[_pubChannelPromise] = this[_connectionPromise]
			.then(cn => cn.createConfirmChannel())
			.then(ch => {
				this._logger.debug('publish channel established');
				ch.on('error', this._onPubChannelError.bind(this, ch));
				return ch;
			});
	}

	_onPubChannelError(ch, err) {
		this._logger.info('publish channel error:');
		this._logger.info(err);

		this._createPubChannel();

		if (ch.unconfirmed.length) {
			this._logger.info('%d awaiting acknowledgement callback(s) will timeout', ch.unconfirmed.length);
			ch.unconfirmed.forEach(cb => cb(err));
		}
	}

	_createSubChannel() {
		this._logger.debug('establishing subscribe channel...');
		return this[_subChannelPromise] = this[_connectionPromise]
			.then(cn => cn.createChannel())
			.then(ch => {
				this._logger.debug('subscribe channel established');
				if (this[_subChannelPrefetch])
					ch.prefetch(this[_subChannelPrefetch]);
				return ch;
			});
	}

	on(messageType, handler) {
		if (typeof messageType !== 'string' || !messageType.length) throw new TypeError('messageType argument must be a non-empty String');
		if (typeof handler !== 'function') throw new TypeError('handler argument must be a Function');


		let subscribeSequence = this[_subChannelPromise];

		if (!this[_handlers]) {
			this[_handlers] = {};

			subscribeSequence = subscribeSequence
				.then(channel => assertQueue(channel, this.queueName, this[_queueOptions], this._logger))
				.then(({channel, queueName}) => {
					this[_queueName] = queueName;
					return channel;
				})
				.then(channel => assertConsumer(channel, this.queueName, this._handle, this._logger));
		}

		if (!(messageType in this[_handlers])) {
			this[_handlers][messageType] = [handler];

			subscribeSequence = subscribeSequence
				.then(channel => assertExchange(channel, this.queueName, messageType, this._logger));
		}
		else {
			this[_handlers][messageType].push(handler);
		}

		return subscribeSequence.catch(err => {
			this._logger.info(err);
			throw err;
		});
	}

	_handle(message) {
		if (!message) return;

		const msgId = descriptor(message);
		const handlers = this[_handlers][message.properties.type];
		if (!handlers || handlers.length === 0) {
			this._logger.info(`'${msgId}' received, no handlers configured`);
			return;
		}

		this._logger.debug(`'${msgId}' received, passing to ${handlers.length === 1 ? '1 handler' : handlers.length + ' handlers'}...`);

		return decodePayload(message.content, message.properties.contentType)
			.then(payload => Promise.all(handlers.map(h => h(payload))))
			.then(results => {
				this._logger.debug(`'${msgId}' processed, will be acknowledged`);
				return this[_subChannelPromise].then(channel => channel.ack(message));
			}, err => {
				this._logger.info(`'${msgId}' processing failed, will be rejected: ${err && err.message || err || 'No reason specified'}`);
				this._logger.info(err);
				// second argument indicates whether the message will be re-routed to another channel
				return this[_subChannelPromise].then(channel => channel.reject(message, false));
			})
			.catch(this._logger.info);
	}


	/**
	 * Removes subscriptions
	 *
	 * @param {string} [messageType] Optional message type which subscriptions must be destroyed. If not provided, all subscriptions will be destroyed
	 * @returns {void}
	 */
	off(messageType) {
		if (messageType) {
			this._logger.debug(`unsubscribing from '${messageType}'`);
			delete this[_handlers][messageType];
		}
		else {
			this._logger.debug('unsubscribing from all messages');
			this[_handlers] = {};
		}
	}

	/**
	 * Sends a command to MQ
	 *
	 * @param {{type:string}} command
	 * @returns {PromiseLike<void>}
	 */
	send(command) {
		if (!command) throw new TypeError('command argument required');
		if (!command.type) throw new TypeError('command.type argument required');

		this._logger.debug(`sending ${command.type}...`);

		return this[_pubChannelPromise].then(ch => publish(ch, command, this._appId, this._logger));
	}

	/**
	 * Publishes an event to MQ
	 *
	 * @param {{type:string}} event
	 * @returns {PromiseLike<void>}
	 */
	publish(event) {
		if (!event) throw new TypeError('event argument required');
		if (!event.type) throw new TypeError('event.type argument required');

		this._logger.debug(`publishing '${event.type}'...`);

		return this[_pubChannelPromise].then(ch => publish(ch, event, this._appId, this._logger));
	}
};
