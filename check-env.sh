#!/bin/bash
kubectl get deployment dev-worker -n backtesting-dev -o json | python3 -c "
import json, sys
d = json.load(sys.stdin)
for e in d['spec']['template']['spec']['containers'][0].get('env', []):
    print(f\"{e['name']}={e.get('value','')}\")
"
