'use strict';

require('debug').enable('*');

const RabbitMqBus = require('../..');
const {connectionString} = require('./credentials');
const {expect} = require('chai');

describe('RabbitMqBus', function () {

	this.timeout(60000);
	this.slow(0);

	let services1CommandsRejected = [];
	let services2CommandsHandled = [];
	let sagaEventsHandled = [];
	let services1;
	let services2;
	let sagas1;

	before(() => {
		services1 = new RabbitMqBus({ connectionString, queue: 'services', durable: true, prefetch: 1, appId: 'services1' });
		services2 = new RabbitMqBus({ connectionString, queue: 'services', durable: true, prefetch: 1, appId: 'services2' });
		sagas1 = new RabbitMqBus({ connectionString, queue: 'sagas', durable: false, appId: 'sagas1' });


		return Promise.all([
			sagas1.channel,
			services1.channel,
			services2.channel
		]);
	});

	after(() => {
		services1.off();
		services2.off();
		sagas1.off();
	});

	it('works', done => {

		services1CommandsRejected = [];
		services1.on('doSomething', command => new Promise((rs, rj) => {
			services1CommandsRejected.push(command);
			// services1.publish({ type: 'somethingDone', by: 'service1' });
			setTimeout(rj, 100);
		}));

		services2CommandsHandled = [];
		services2.on('doSomething', command => new Promise((rs, rj) => {
			services2CommandsHandled.push(command);
			services2.publish({ type: 'somethingDone', by: 'service2' });
			setTimeout(rs, 100);
		}));

		sagaEventsHandled = [];
		sagas1.on('processStarted', event => {
			sagaEventsHandled.push(event);
			sagas1.send({ type: 'doSomething', id: 'A' });
			sagas1.send({ type: 'doSomething', id: 'B' });
			sagas1.send({ type: 'doSomething', id: 'C' });
			sagas1.send({ type: 'doSomething', id: 'D' });
		});

		let timeout = 0;
		sagas1.on('somethingDone', event => {
			sagaEventsHandled.push(event);

			if (timeout)
				clearTimeout(timeout);

			timeout = setTimeout(f => {
				expect(services1CommandsRejected).to.have.length(2);
				expect(services2CommandsHandled).to.have.length(2);
				expect(sagaEventsHandled).to.have.length(3);
				expect(sagaEventsHandled).to.have.deep.property('[0].type', 'processStarted');
				expect(sagaEventsHandled).to.have.deep.property('[2].type', 'somethingDone');
				done();
			}, 500);
		});

		services1.publish({ type: 'processStarted' });

	});
});