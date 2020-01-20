# Service for queuing import records from melinda-rest-api

## Usage
While service is in operation, if AMQP does not have anything in `'QUEUE'` it checks mongo if there is job with same `'OPERATION'`.

### Environment variables
| Name                 | Mandatory | Description                                                                                                        |
|----------------------|-----------|--------------------------------------------------------------------------------------------------------------------|
| AMQP_URL             | Yes       | A serialized object of AMQP connection config                                                                      |
| OPERATION            | Yes       | A string state of passing operations. Enum: `'create'` or `'update'`                                               |
| QUEUE                | Yes       | A string name of AMQP queue to be polled. Enum: Melinda-recorc-import-commons `PRIO_IMPORT_QUEUES`                 |
| RECORD_LOAD_API_KEY  | Yes       | A string key authorized to use the API                                                                             |
| RECORD_LOAD_LIBRARY  | Yes       | A string                                                                                                           |
| RECORD_LOAD_URL      | Yes       | A serialized URL address of Melinda-record-load-api                                                                |
| DEFAULT_CATALOGER_ID | No        | Default cataloger to be set to the record ? Obsolite! Defaults to: `'API'`                                         |
| MONGO_URI            | No        | A serialized URL address of Melinda-rest-api's import queue database. Defaults to `'mongodb://localhost:27017/db'` |
| OFFLINE_PERIOD       | No        | Starting hour and length of offline period. Format is `'START_HOUR,LENGTH_IN_HOURS'`,                              |
| POLL_WAIT_TIME       | No        | A number value presenting time in ms between polling                                                               |
| PURGE_QUEUE_ON_LOAD  | No        | A numeric presentation of boolean option to purge AMQP queue set in `'QUEUE'` when process is started e.g. `1`     |

### Mongo
Db: `'rest-api'`
Table: `'queue-items'`
Queue-item schema:
```json
{
	"correlationId":"FOO",
	"cataloger":"xxx0000",
	"operation":"update",
	"contentType":"application/json",
	"queueItemState":"PENDING_QUEUING",
	"creationTime":"2020-01-01T00:00:00.000Z",
	"modificationTime":"2020-01-01T00:00:01.000Z"
}
```

## License and copyright

Copyright (c) 2020-2020 **University Of Helsinki (The National Library Of Finland)**

This project's source code is licensed under the terms of **GNU Affero General Public License Version 3** or any later version.