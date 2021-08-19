#!/usr/bin/env node
const axios = require('axios');
const yaml = require('js-yaml');
const fs = require('fs');
const yargs = require('yargs/yargs')
const { PrometheusDriver } = require('prometheus-query');
const { hideBin } = require('yargs/helpers');
const { isMatch } = require('./common');

const winston = require('winston');

const logger = winston.createLogger({
    transports: [new winston.transports.Console()],
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.colorize({ all: true }),
        winston.format.printf(info => `${info.timestamp} [${info.level}]: ${info.message}`),
    )
});

const argv = yargs(hideBin(process.argv))
    .option('config', {
        alias: 'c',
        type: 'string',
        require: true,
        description: 'config file path',
    })
    .argv




function labelsObject(labels) {
    let object = {};
    for (const { key, value } of labels) {
        object[key] = value;
    }
    return object
}
class RecoverInfoCollecter {
    constructor({ topology, masterLabels, backupLabels, timeout }) {
        const pdServer = topology['pd_servers'].map(
            ({ host, client_port }) => {
                if (!client_port) {
                    client_port = 2379;
                }
                return { host, client_port }
            })[0];

        const promServer = topology['monitoring_servers'].map(({ host, port }) => {
            if (!port) {
                port = 9090;
            }
            return { host, port }
        })[0];

        this.backupNodes = topology['tikv_servers']
            .filter(
                ({
                    config: { 'server.labels': labels } }
                ) => isMatch(backupLabels, labels)
            )
            .map(({ host, port }) => {
                if (!port) {
                    port = 20180;
                }
                return `${host}:${port}`;
            });

        this.pdAddr = `${pdServer.host}:${pdServer.client_port}`;
        this.masterLabels = masterLabels;

        this.promDriver = new PrometheusDriver({
            endpoint: `http://${promServer.host}:${promServer.port}`,
            baseURL: "/api/v1",
            timeout,
        });
        this.timeout = timeout;
    }

    async collectStoreIds() {
        try {
            const { data } = await axios.get(`http://${this.pdAddr}/pd/api/v1/stores`, { timeout: this.timeout });
            const { stores } = data;
            return stores
                .filter(({ store: { labels } }) =>
                    isMatch(this.masterLabels, labelsObject(labels))
                )
                .map(({ store: { id } }) => id);

        } catch (e) {
            return undefined;
        }
    }

    async collectAllocId() {
        try {
            const q = `pd_cluster_id`;
            const metric = await this.promDriver.instantQuery(q);
            const series = metric.result;
            if (series.length == 0) {
                return undefined;
            }
            const id = series[series.length - 1].value.value;
            return parseInt(id) + (2 ** 32);
        } catch (e) {
            return undefined;
        }
    }

    async collectClusterId() {
        try {
            const q = `pd_cluster_metadata{instance="${this.pdAddr}"}`;
            const metric = await this.promDriver.instantQuery(q);
            const series = metric.result;
            if (series.length == 0) {
                return undefined;
            }
            const type = series[series.length - 1].metric.labels.type;
            const clusterId = type.slice('cluster'.length)
            return clusterId;
        } catch (e) {
            return undefined;
        }
    }

    async collectRPO() {
        try {
            const q = `sum(tikv_resolved_ts_min_resolved_ts_gap_seconds) by (instance)`;
            const metric = await this.promDriver.instantQuery(q);
            const series = metric.result;
            if (series.length == 0) {
                return undefined;
            }
            const rpos = series.map(({ metric: { labels: { instance } }, value: { value } }) => {
                return { instance, value };
            }).filter(({ instance }) => this.backupNodes.includes(instance));
            return rpos;
        } catch (e) {
            return undefined;
        }
    }

    async collect() {
        const rpos = await this.collectRPO();
        const storeIds = await this.collectStoreIds();
        const clusterId = await this.collectClusterId();
        const allocId = await this.collectAllocId();
        return {
            rpos,
            storeIds,
            clusterId,
            allocId
        };
    }
}

async function fetch(config) {
    if (fs.existsSync(config['save'])) {
        info = JSON.parse(fs.readFileSync(config['save']));
    } else {
        info = {};
    }
    const topology = yaml.load(fs.readFileSync(config['topo']));

    const collector = new RecoverInfoCollecter({
        topology,
        masterLabels: config['master-labels'],
        backupLabels: config['backup-labels'],
        timeout: 1000,
    });

    try {
        const newInfo = await collector.collect()
        for (const [k, v] of Object.entries(newInfo)) {
            if (v) {
                if (k == 'rpos' && v.length == 0) {
                    continue;
                }
                info[k] = v;
            }
        }
        fs.writeFileSync(config['save'], JSON.stringify(info));
        logger.info(`saved recover info to ${config['save']}`);
    } catch (err) {
        logger.error(`fail to fetch backup info ${err.toString()}`)
    }
}


function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function main() {
    const config = yaml.load(fs.readFileSync(argv['config']));

    const interval = config['interval'] ? parseInt(config['interval']) * 1000 : 1000;
    const times = config['repeat'] ? parseInt(config['repeat']) : Number.MAX_SAFE_INTEGER;
    for (let i = 0; i < times; i++) {
        await fetch(config);
        await sleep(interval);
    }
}

main().then(() => { })
