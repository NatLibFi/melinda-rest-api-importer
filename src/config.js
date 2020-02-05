import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils; // eslint-disable-line no-unused-vars

// Record-load-api to save data
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY');

// Amqp variables to priority
export const AMQP_URL = readEnvironmentVariable('AMQP_URL', {defaultValue: 'amqp://127.0.0.1:5672/'});

// Mongo variables to bulk
export const MONGO_URI = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://127.0.0.1:27017/db'});

// Operation variables
export const POLL_WAIT_TIME = readEnvironmentVariable('POLL_WAIT_TIME', {defaultValue: 1000});
export const OPERATION = readEnvironmentVariable('OPERATION');
