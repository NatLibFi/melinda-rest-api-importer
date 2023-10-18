import {readEnvironmentVariable} from '@natlibfi/melinda-backend-commons/';

// Record-load-api to save data
export const recordLoadUrl = readEnvironmentVariable('RECORD_LOAD_URL');
export const recordLoadApiKey = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const recordLoadLibrary = readEnvironmentVariable('RECORD_LOAD_LIBRARY');
export const fixPrio = readEnvironmentVariable('FIX_PRIO', {defaultValue: 'API'});
export const fixBulk = readEnvironmentVariable('FIX_BULK', {defaultValue: 'INSB'});

// Amqp variables to priority
export const amqpUrl = readEnvironmentVariable('AMQP_URL', {defaultValue: 'amqp://127.0.0.1:5672/'});

// Mongo variables to bulk
export const mongoUri = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://127.0.0.1:27017/db'});

// Operation variables
export const pollWaitTime = readEnvironmentVariable('POLL_WAIT_TIME', {defaultValue: 1000});
export const error503WaitTime = readEnvironmentVariable('ERROR_503_WAIT_TIME', {defaultValue: 10000});
export const operation = readEnvironmentVariable('OPERATION');

// Reporting variables
// keepLoadProcessReports: ALL/NONE/NON_PROCESSED/NON_HANDLED
export const keepLoadProcessReports = readEnvironmentVariable('KEEP_LOAD_PROCESS_RESULTS', {defaultValue: 'NON_HANDLED'});
