#!/usr/bin/env bash
TMPDIR=$(mktemp -d)
echo "Using temporary data directory: $TMPDIR"

mkdir -p $TMPDIR/data
docker run \
    --volume=$TMPDIR/data:/data \
    --publish=7474:7474 --publish=7687:7687 \
    neo4j

# reachable via http://localhost:7474 for http and localhost:7687 for bolt
# default login after container spinup is neo4j / neo4j and will
# requir changing in webinterface

# Modify this according to your needs:
# add --volume=$HOME/neo4j/data:/data \ to enable persistence of data 
# pass --env=NEO4J_AUTH=none \ to disable authentification.
