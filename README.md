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

P_manage_18 confs
| P_manage_18 args      | camelCase           | Prio update | Prio create | Bulk update | Bulk create | Finto             | NOTE                           |
|-----------------------|---------------------|-------------|-------------|-------------|-------------|-------------------|--------------------------------|
| "p_active_library"    | "pActiveLibrary"    | `params`    | `params`    | `params`    | `params`    | `params`          |                                |
| "p_input_file"        | "pInputFile"        | `generated` | `generated` | `generated` | `generated` | `generated`       |                                |
| "p_reject_file"       | "pRejectFile"       | `generated` | `generated` | `generated` | `generated` | `generated`       |                                |
| "p_log_file"          | "pLogFile"          | `generated` | `generated` | `generated` | `generated` | `generated`       |                                |
| "p_old_new"           | "pOldNew"           | OLD         | NEW         | OLD         | NEW         | NEW / OLD         |                                |
| "p_fix_type"          | "pFixType"          | API         | API         | INSB        | INSB        | INSA              | Alpeh fix routine code         |
| "p_check_references"  | "pCheckReferences"  |             |             |             |             |                   |                                |
| "p_update_f"          | "pUpdateF"          | FULL        | FULL        | FULL        | FULL        | FULL              | Indexing                       |
| "p_update_type"       | "pUpdateType"       | REP         | REP         | REP         | REP         | REP / APP / MERGE | REP or APP (REPlace or APPend) |
| "p_update_mode"       | "pUpdateMode"       | M           | M           | M           | M           | M                 | M (Multi-user)                 |
| "p_char_conv"         | "pCharConv"         |             |             |             |             |                   |                                |
| "p_merge_type"        | "pMergeType"        |             |             |             |             | '' / REPLACE      |                                |
| "p_cataloger_in"      | "pCatalogerIn"      | `params`    | `params`    | `params`    | `params`    | `params`          |                                |
| "p_cataloger_level_x" | "pCatalogerLevelX"  |             |             |             |             | 30                |                                |
| "p_z07_priority_year" | "pZ07PriorityYear"  | 1998        | 1990        | 2099        | 2099        | 2095              |                                |
| "p_redirection_field" | "pRedirectionField" |             |             |             |             |                   |                                |

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
        "library": "XXX00",
        "inputFile": "filename.seq",
        "method": "NEW",
        "fixRoutine": "INSB",
        "space": "",
        "indexing": "FULL",
        "updateAction": "APP",
        "mode": "M",
        "charConversion": "",
        "mergeRoutine": "",
        "cataloger": "XXX0000",
        "catalogerLevel": "",
        "indexingPriority": "2099"
      },
	"queueItemState":"PENDING_QUEUING",
	"creationTime":"2020-01-01T00:00:00.000Z",
	"modificationTime":"2020-01-01T00:00:01.000Z"
}
```

## License and copyright

Copyright (c) 2020-2020 **University Of Helsinki (The National Library Of Finland)**

This project's source code is licensed under the terms of **GNU Affero General Public License Version 3** or any later version.