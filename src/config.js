import {readEnvironmentVariable} from '@natlibfi/melinda-backend-commons/';

// Record-load-api to save data
export const recordLoadUrl = readEnvironmentVariable('RECORD_LOAD_URL');
export const recordLoadFixPath = readEnvironmentVariable('RECORD_LOAD_FIX_PATH', {defaultValue: '/fix'});
export const recordLoadLoadPath = readEnvironmentVariable('RECORD_LOAD_LOAD_PATH', {defaultValue: ''});
export const recordLoadApiKey = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const recordLoadLibrary = readEnvironmentVariable('RECORD_LOAD_LIBRARY');
export const fixPrio = readEnvironmentVariable('FIX_PRIO', {defaultValue: 'API'});
export const fixBulk = readEnvironmentVariable('FIX_BULK', {defaultValue: 'INSB'});

// Amqp variables to priority
export const amqpUrl = readEnvironmentVariable('AMQP_URL', {defaultValue: 'amqp://127.0.0.1:5672/'});

// Mongo variables to bulk
export const mongoUri = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://127.0.0.1:27017/db'});

// Operation variables
export const pollWaitTime = parseInt(readEnvironmentVariable('POLL_WAIT_TIME', {defaultValue: 1000}), 10);
export const error503WaitTime = parseInt(readEnvironmentVariable('ERROR_503_WAIT_TIME', {defaultValue: 10000}), 10);

export const operation = readEnvironmentVariable('OPERATION');

// Reporting variables
// keepLoadProcessReports: ALL/NONE/NON_PROCESSED/NON_HANDLED
export const keepLoadProcessReports = readEnvironmentVariable('KEEP_LOAD_PROCESS_RESULTS', {defaultValue: 'NON_HANDLED'});
