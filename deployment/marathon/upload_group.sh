#!/bin/bash

# Upload artifacts to marathon
# MARATHON_MASTER="http://10.2.54.230:8081"
# MARATHON_MASTER="http://10.2.73.30:8080"
# MARATHON_MASTER="http://localhost:8083"
 MARATHON_MASTER="http://10.2.95.5:8081"
 #MARATHON_MASTER="http://marathon.qs-test-v2.hli.io"

 curl -include -XPUT "$MARATHON_MASTER/v2/groups?force=true" -d @$1
