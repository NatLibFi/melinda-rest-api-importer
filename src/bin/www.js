#!/usr/bin/env node

/**
 * Module dependencies.
 */

import app from '../app';
import http from 'http';
import {Utils} from '@natlibfi/melinda-commons';
import amqplib from 'amqplib';
import {logError} from '../utils';
import {consumeQueue} from '../services/fromQueueService'
import {toRecordLoadApi} from '../services/toRecordLoadApiService';

import {EventEmitter} from 'events';
class QueueEmitter extends EventEmitter {}
export const EMITTER = new QueueEmitter();

import {
  AMQP_URL,
  HTTP_PORT,
  NAME_QUEUE_BULK,
  NAME_QUEUE_PRIORITY,
  EMITTER_CONSUM_ANNOUNSE,
  EMITTER_JOB_CONSUME,
  PURGE_QUEUE_ON_LOAD
} from '../config'

const {createLogger} = Utils;
const logger = createLogger();
const loader = toRecordLoadApi();

process
  .on('SIGINT', err => handleExit(err, 1))
  .on('SIGTERM', err => handleExit(err, 1));
//.on('unhandledRejection', err => handleExit(err, 1))
//.on('uncaughtException', err => handleExit(err, 1));

/**
 * Get port from config and store in Express.
 */
const port = HTTP_PORT;
app.set('port', port);
/**
 * Create HTTP server.
 */

const server = http.createServer(app);

/**
 * Listen on provided port, on all network interfaces.
 */

server.listen(port);
server.on('error', onError);
server.on('listening', onListening);

setEmitterListeners();
async function setEmitterListeners() {
  await operateRabbitQueues(true, PURGE_QUEUE_ON_LOAD, true);

  await new Promise((res) => {
    EMITTER
      .on('SHUTDOWN', () => res())
      .on(EMITTER_JOB_CONSUME, (type) => {
        logger.log('debug', `Emitter job - Consume: ${type}`);
        consumeQueue(type, EMITTER);
      })
      .on(EMITTER_CONSUM_ANNOUNSE, async (data) => {
        logger.log('debug', `Emitter job - Announce: ${data.type}`);
        if (data.type === NAME_QUEUE_PRIORITY) {
          EMITTER.emit(EMITTER_CONSUM_ANNOUNSE + '.' + data.QUEUEID, data);
        }
        //logger.log('info', JSON.stringify(data));
        await loader(data);
        consumeQueues();
      });
      consumeQueues();
  });
}

async function consumeQueues() {
  const queues = await operateRabbitQueues(false, false, true);
  if (queues.PRIORITY > 0) {
    EMITTER.emit(EMITTER_JOB_CONSUME, NAME_QUEUE_PRIORITY);
  } else if (queues.BULK > 0) {
    EMITTER.emit(EMITTER_JOB_CONSUME, NAME_QUEUE_BULK);
  } else {
    setTimeout(consumeQueues, 3000);
  }
}

/**
 * Event listener for HTTP server "error" event.
 */

function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }

  var bind = typeof port === 'string'
    ? 'Pipe ' + port
    : 'Port ' + port;

  // handle specific listen errors with friendly messages
  switch (error.code) {
    case 'EACCES':
      logger.error(bind + ' requires elevated privileges');
      process.exit(1);
      break;
    case 'EADDRINUSE':
      logger.error(bind + ' is already in use');
      process.exit(1);
      break;
    default:
      throw error;
  }
}

/**
 * Event listener for HTTP server "listening" event.
 */

function onListening() {
  const addr = server.address();
  const bind = typeof addr === 'string'
    ? 'pipe ' + addr
    : 'port ' + addr.port;
  logger.log('info', 'Listening on ' + bind);
}

async function operateRabbitQueues(initQueues, purge, checkQueues) {
  let connection;
  let channel;
  let prioQueueCount;
  let bulkQueueCount;

  try {
    connection = await amqplib.connect(AMQP_URL);
    channel = await connection.createChannel();

    if (initQueues) {
      await channel.assertQueue(NAME_QUEUE_PRIORITY, {durable: true, autoDelete: false});
      await channel.assertQueue(NAME_QUEUE_BULK, {durable: true, autoDelete: false});
      logger.log('info', 'Rabbitmq queues has been initiated');
    }

    if (purge) {
      await channel.purgeQueue(NAME_QUEUE_PRIORITY);
      await channel.purgeQueue(NAME_QUEUE_BULK);
      logger.log('info', 'Rabbitmq queues have been purged');
    }

    if (checkQueues) {
      const infoChannelPrio = await channel.checkQueue(NAME_QUEUE_PRIORITY);
      prioQueueCount = infoChannelPrio.messageCount
      logger.log('debug', `${NAME_QUEUE_PRIORITY} queue: ${prioQueueCount} records`);
      const infoChannel = await channel.checkQueue(NAME_QUEUE_BULK);
      bulkQueueCount = infoChannel.messageCount;
      logger.log('debug', `${NAME_QUEUE_BULK} queue: ${bulkQueueCount} records`);
    }
  } catch (err) {
    logError(err);
  } finally {
    if (channel) {
      await channel.close();
    }

    if (connection) {
      await connection.close();
    }

    if (checkQueues) {
      return {PRIORITY: prioQueueCount, BULK: bulkQueueCount}
    }
  }
}

async function handleExit(err, arg) {
  logError(err);
  EMITTER.emit('SHUTDOWN');
  process.exit(arg);
}
