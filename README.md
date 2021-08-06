# Prerequisite

1. Install latest NodeJS >= 16, you may use [nvm](https://github.com/nvm-sh/nvm) to install it.
2. Execute `npm install`.
3. Install `TiUP` in current node.

# How it works

This scripts has two parts:

1. Crawler: periodically fetch some cluster info from the main cluster and save it to a json file.
2. Restorer: move the learner out of the old cluster and promote them to voters in another cluster.

# Usage

1. Crawler: 
```
node index.js --help
Options:
      --help      Show help                           [boolean]
      --version   Show version number                 [boolean]
  -t, --topo      path of topology file     [string] [required]
  -z, --zone      zone that need info backup[string] [required]
  -s, --save      path to save cluster info backup
                                            [string] [required]
  -i, --interval  path to save cluster info backup (s)
                                          [number] [default: 1]
```

2. Restorer:
```
node recover.js --help
Options:
      --help     Show help                            [boolean]
      --version  Show version number                  [boolean]
  -c, --config   path of config file        [string] [required]
```

config template:

```yaml
cluster-version: v5.1.0
# new cluster name
cluster-name: iosmanthus-backup

# the main cluster's topology file
old-topology: config/old.yaml
# the new cluster's topology file
new-topology: config/new.yaml
# the topology that contains the learners from the main cluster, change port to avoid conflict.
join-topology: config/join.yaml
# recover info file path
recover-info-file: config/recover-info.json
# recover zone label
recover-zone: backup
# path for tikv-ctl on each learner node
tikv-ctl-path: /root/tikv-ctl
# path for pd-recover in current node
pd-recover-path: bin/pd-recover
```