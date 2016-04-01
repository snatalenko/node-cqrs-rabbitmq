'use strict';

const debug = require('debug');
// const chai = require('chai');
// const expect = chai.expect;

debug.enable('*');

const RabbitMqBus = require('../src/infrastructure/RabbitMqBus');

const services1 = new RabbitMqBus({
	connectionString: 'amqp://ss:123@localhost?heartbeat=30',
	queue: 'api.services',
	durable: true,
	appId: 'services1'
});

const services2 = new RabbitMqBus({
	connectionString: 'amqp://ss:123@localhost?heartbeat=30',
	queue: 'api.services',
	durable: true,
	appId: 'services2'
});

const sagas1 = new RabbitMqBus({
	connectionString: 'amqp://ss:123@localhost?heartbeat=30',
	queue: 'api.sagas',
	durable: false,
	appId: 'sagas1'
});

const sagas2 = new RabbitMqBus({
	connectionString: 'amqp://ss:123@localhost?heartbeat=30',
	queue: 'api.sagas',
	durable: false,
	appId: 'sagas2'
});

const projections1 = new RabbitMqBus({
	connectionString: 'amqp://ss:123@localhost?heartbeat=30',
	queuePrefix: 'api.projections.',
	appId: 'projections1'
});

const projections2 = new RabbitMqBus({
	connectionString: 'amqp://ss:123@localhost?heartbeat=30',
	queuePrefix: 'api.projections.',
	appId: 'projections2'
});

let failed = false;

function commandHandler(command, logger) {

	logger('got message, processing...');

	return new Promise(function (resolve, reject) {
		setTimeout(() => {
			if (!failed) {
				logger('processing failed');
				reject(new Error('test'));
				failed = true;
			}
			else {
				resolve({
					ok: true
				});
			}
		}, Math.random() * 10000);
	});
}

services1.on('doSomething', msg => commandHandler(msg, debug('services1')));
services2.on('doSomething', msg => commandHandler(msg, debug('services2')));
projections1.on('somethingHappened', msg => debug('projections1')('got message'));
projections2.on('somethingHappened', msg => debug('projections2')('got message'));
sagas1.on('somethingHappened', msg => debug('sagas1')('got message'));
sagas2.on('somethingHappened', msg => debug('sagas2')('got message'));

setTimeout(() => {
	for (let i = 0; i < 5; i++) {
		sagas1.send('doSomething', {
			i: i
		});
	}
}, 1000);

setInterval(() => {}, 1000);
