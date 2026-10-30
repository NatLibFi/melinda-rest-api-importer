import {readEnvironmentVariable} from '@natlibfi/melinda-commons';

// Record-load-api to save data
export const recordLoadUrl: string = readEnvironmentVariable('RECORD_LOAD_URL');
export const recordLoadFixPath: string = readEnvironmentVariable('RECORD_LOAD_FIX_PATH', {defaultValue: '/fix'});
export const recordLoadLoadPath: string = readEnvironmentVariable('RECORD_LOAD_LOAD_PATH', {defaultValue: ''});
export const recordLoadApiKey: string = readEnvironmentVariable('RECORD_LOAD_API_KEY');
export const recordLoadLibrary: string = readEnvironmentVariable('RECORD_LOAD_LIBRARY');
export const fixPrio: string = readEnvironmentVariable('FIX_PRIO', {defaultValue: 'API'});
export const fixBulk: string = readEnvironmentVariable('FIX_BULK', {defaultValue: 'INSB'});

// Amqp variables to priority
export const amqpUrl: string = readEnvironmentVariable('AMQP_URL', {defaultValue: 'amqp://127.0.0.1:5672/'});

// Mongo variables to bulk
export const mongoUri: string = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://127.0.0.1:27017/db'});

// Operation variables
export const pollWaitTime: number = parseInt(readEnvironmentVariable('POLL_WAIT_TIME', {defaultValue: 1000}), 10);
export const error503WaitTime: number = parseInt(readEnvironmentVariable('ERROR_503_WAIT_TIME', {defaultValue: 10000}), 10);

export const operation: string = readEnvironmentVariable('OPERATION');

// Reporting variables
// keepLoadProcessReports: ALL/NONE/NON_PROCESSSED/NON_HANDLED
export const keepLoadProcessReports: string = readEnvironmentVariable('KEEP_LOAD_PROCESS_RESULTS', {defaultValue: 'NON_HANDLED'});
