#! /bin/bash

if [[ ${#} -ne 1 ]]; then
	echo "Usage: ./run.sh <SCHEDULE_FILE>"
	exit 1
fi
SCHEDULE="${1}"

/usr/lib/jvm/default-java/bin/java -cp .:gen-java/:"lib/*" LocalSincroniaProcessor ${SCHEDULE}