#!/bin/sh
curl -s -w '\nHTTP_CODE:%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"source_code": "class MyStrategy: pass"}' http://localhost:3000/api/strategies/validate