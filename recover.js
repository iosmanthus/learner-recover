const shell = require('shelljs');
const yaml = require('js-yaml');
const fs = require('fs');
const yargs = require('yargs/yargs')
const { hideBin } = require('yargs/helpers')
const axios = require('axios');
const winston = require('winston');

const logger = module.exports = winston.createLogger({
    transports: [new winston.transports.Console()],
    format: winston.format.combine(
        winston.format.colorize({ all: true }),
        winston.format.simple()
    )
});

const argv = yargs(hideBin(process.argv))
    .option('config', {
        alias: 'c',
        type: 'string',
        require: true,
        description: 'path of config file'
    })
    .argv

class BackupCluster {
    constructor({ topology, zoneName, recoverInfo }) {
        this.nodes = topology['tikv_servers']
            .map(({
                host,
                data_dir,
                port,
                config: { 'server.labels': { zone } } }
            ) => { return { host, port, data_dir, zone }; })
            .filter(({ zone }) => zone === zoneName)
            .map(({ host, port, data_dir }) => {
                if (!port) {
                    port = 20160;
                }
                return { host, port, data_dir };
            })
        this.user = topology['global']['user'];
        this.sshPort = topology['global']['ssh_port'];
        this.recoverInfo = recoverInfo;
    }

    stop() {
        for (const { host, port } of this.nodes) {
            shell.exec(`ssh -p ${this.sshPort} ${this.user}@${host} systemctl stop tikv-${port}.service`,
                { fatal: true });
        }
    }

    unsafeRecover({ tikvCtlPath }) {
        const failStores = this.recoverInfo['storeIds'].reduce((acc, s, index) => {
            if (index == 0) {
                return `${s}`
            }
            return `${acc},${s}`
        })
        for (const { host, data_dir } of this.nodes) {
            shell.exec(`ssh -p ${this.sshPort} ${this.user}@${host} \
                ${tikvCtlPath} --data-dir ${data_dir} unsafe-recover remove-fail-stores -s ${failStores} --all-regions --promote-learner`,
                { fatal: true });
        }
    }

    rebuildPDServer({ pdRecoverPath, version, clusterName, topoFile }) {
        shell.exec(`tiup cluster deploy -y ${clusterName} ${version} ${topoFile}`, { fatal: true });
        shell.exec(`tiup cluster start -y ${clusterName}`, { fatal: true });

        const topo = yaml.load(fs.readFileSync(topoFile));
        const pdServers = topo['pd_servers'].map(({ host, client_port }) => {
            if (!client_port) {
                client_port = 2379;
            }
            return { host, client_port }
        })

        const anyone = pdServers[0];
        this.pdServer = anyone;
        this.clusterName = clusterName;

        console.log(anyone);
        shell.exec(`${pdRecoverPath} -endpoints http://${anyone.host}:${anyone.client_port} \
            -cluster-id ${this.recoverInfo.clusterId} -alloc-id ${this.recoverInfo.allocId}`, { fatal: true })

        shell.exec(`tiup cluster restart -y ${clusterName}`, { fatal: true })
    }

    async joinLearners({ topoFile }) {
        while (true) {
            try {
                await axios.get(`http://${this.pdServer.host}:${this.pdServer.client_port}/pd/api/v1/config/replicate`);
            } catch (_) {
                continue;
            }
            break;
        }
        shell.exec(`tiup cluster scale-out -y ${this.clusterName} ${topoFile}`, { fatal: true })
    }
}

async function recover() {
    const config = yaml.load(fs.readFileSync(argv.config));
    const topology = yaml.load(fs.readFileSync(config['old-topology']));
    const recoverInfo = JSON.parse(fs.readFileSync(config['recover-info-file']));
    const cluster = new BackupCluster({ topology, zoneName: config['recover-zone'], recoverInfo });
    logger.info('Building backup cluster info');
    console.log(cluster);

    logger.warn('Stopping TiKV learners');
    cluster.stop();
    logger.warn('Unsafe recovering TiKV learners, this may take several minutes');
    cluster.unsafeRecover({ tikvCtlPath: config['tikv-ctl-path'] });
    logger.warn('Rebuilding PD server');
    cluster.rebuildPDServer({
        pdRecoverPath: config['pd-recover-path'],
        version: config['cluster-version'],
        clusterName: config['cluster-name'],
        topoFile: config['new-topology']
    });
    logger.warn('Joining TiKV learners');
    await cluster.joinLearners({
        topoFile: config['join-topology']
    });
}

recover().then(() => { })