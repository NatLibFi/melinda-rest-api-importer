# Service for queuing import records from melinda-rest-api

## Usage

While service is in use it polls Mongo for jobs is state `'IMPORTER.IN_QUEUE'` for `OPERATION`, prioritizing prio jobs. When a job is found, the record(s) belonging to it are fetched from it's AMQP queue (`OPERATION.correalationId`) in suitable chunks of records and the record file is forwarded to record-load-api for actual importing to Melinda. When all loadprocesses for a job are done, the job is transitioned to state `DONE` in Mongo. 

### Environment variables
| Name                | Mandatory | Description                                                                                                        |
|---------------------|-----------|--------------------------------------------------------------------------------------------------------------------|
| AMQP_URL            | Yes       | A serialized object of AMQP connection config                                                                      |
| OPERATION           | Yes       | A string state of passing operations. Enum: `'CREATE'` or `'UPDATE'`                                               |
| RECORD_LOAD_API_KEY | Yes       | A string key authorized to use the API                                                                             |
| RECORD_LOAD_LIBRARY | Yes       | A string                                                                                                           |
| RECORD_LOAD_URL     | Yes       | A serialized URL address of Melinda-record-load-api                                                                |
| MONGO_URI           | No        | A serialized URL address of Melinda-rest-api's import queue database. Defaults to `'mongodb://localhost:27017/db'` |
| OFFLINE_PERIOD      | No        | Starting hour and length of offline period. e.g `'11,1'`                                                           |
| POLL_WAIT_TIME      | No        | A number value presenting time in ms between polling                                                               |
| LOG_LEVEL           | No        | Log information level                                                                                              |
| ERROR_503_WAIT_TIME | No        | A number value presenting time in ms for waiting before trying again when receiving 503 error from aleph-record-load-api |
| KEEP_LOAD_PROCESS_RESULTS | No  | A string telling in which cases load process details are saved in Mongo. Defaults to `NON_HANDLED`. Options: `ALL`, `NONE`, `NON_PROCESSED`, `NON_HANDLED`. |


### Mongo
Db: `'rest-api'`
Tables: `'prio'` for prio jobs, `'bulk'` for bulk jobs

Queue-item schema example for a prio job queueItem:
```json
{
"correlationId":"FOO",
"cataloger":"xxx0000",
"oCatalogerIn": "xxx0000",
"operation":"UPDATE",
"operationSettings": {
  "noop": true,
  "unique": false,
  "prio": true,
  },
"recordLoadParams": {
  "pActiveLibrary": "XXX00",
  "pInputFile": "filename.seq",
  "pRejectFile": "filename.rej",
  "pLogFile": "filename.syslog",
  "pOldNew": "NEW"
  },
"queueItemState":"DONE",
"creationTime":"2020-01-01T00:00:00.000Z",
"modificationTime":"2020-01-01T00:00:01.000Z",
"handledIds": [ "000000001"],
"rejectedIds": [],
"errorStatus": "",
"errorMessage": "",
"noopValidationMessages": [],
"loadProcessReports": []
}
```

Queue-item schema examle for a bulk job queueItem:
```json
{
"correlationId":"FOO",
"cataloger":"xxx0000",
"oCatalogerIn": "xxx0000",
"operation":"UPDATE",
"operationSettings": {
  "prio": false,
 },
"contentType": "application/json",
"recordLoadParams": {
  "pActiveLibrary": "XXX00",
  "pInputFile": "filename.seq",
  "pRejectFile": "filename.rej",
  "pLogFile": "filename.syslog",
  "pOldNew": "NEW"
},
"queueItemState":"DONE",
"creationTime":"2020-01-01T00:00:00.000Z",
"modificationTime":"2020-01-01T00:00:01.000Z",
"handledIds": [ "000000001","000000002"],
"rejectedIds": ["000999999"],
"errorStatus": "",
"errorMessage": "",
"loadProcessReports": [{
  "status": 200,
  "processId": 9999,
  "processedAll": false,
  "recordAmount": 3,
  "processedAmount": 2,
  "handledAmount": 1,
  "rejectedAmount": 1,
  "rejectMessages": ["Cannot overwrite a deleted record. Record 000999999 is written to rej file"]
  }]
}
```

## License and copyright

Copyright (c) 2020-2021 **University Of Helsinki (The National Library Of Finland)**

This project's source code is licensed under the terms of **GNU Affero General Public License Version 3** or any later version.