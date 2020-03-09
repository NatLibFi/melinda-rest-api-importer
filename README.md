# Service for queuing import records from melinda-rest-api

## Usage
While service is in operation, if AMQP does not have anything in `'QUEUE'` it checks mongo if there is job with same `'OPERATION'`.

### Environment variables
| Name                | Mandatory | Description                                                                                                        |
|---------------------|-----------|--------------------------------------------------------------------------------------------------------------------|
| AMQP_URL            | Yes       | A serialized object of AMQP connection config                                                                      |
| OPERATION           | Yes       | A string state of passing operations. Enum: `'CREATE'` or `'UPDATE'` (Also marks what queue will be polled)        |
| RECORD_LOAD_API_KEY | Yes       | A string key authorized to use the API                                                                             |
| RECORD_LOAD_LIBRARY | Yes       | A string                                                                                                           |
| RECORD_LOAD_URL     | Yes       | A serialized URL address of Melinda-record-load-api                                                                |
| MONGO_URI           | No        | A serialized URL address of Melinda-rest-api's import queue database. Defaults to `'mongodb://localhost:27017/db'` |
| OFFLINE_PERIOD      | No        | Starting hour and length of offline period. e.g `'11,1'`                                                           |
| POLL_WAIT_TIME      | No        | A number value presenting time in ms between polling                                                               |



### Mongo
Db: `'rest-api'`
Table: `'queue-items'`
Queue-item schema:
```json
{
	"correlationId":"FOO",
	"cataloger":"xxx0000",
	"operation":"UPDATE",
	"contentType":"application/json",
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
  "handledIds": [ "000000001","000000002"]
}
```

## License and copyright

Copyright (c) 2020-2020 **University Of Helsinki (The National Library Of Finland)**

This project's source code is licensed under the terms of **GNU Affero General Public License Version 3** or any later version.