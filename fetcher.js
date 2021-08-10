#!/usr/bin/env node
const axios = require('axios');
const yaml = require('js-yaml');
const fs = require('fs');
const yargs = require('yargs/yargs')
const { PrometheusDriver } = require('prometheus-query');
const { hideBin } = require('yargs/helpers');

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
    .option('topo', {
        alias: 't',
        type: 'string',
        require: true,
        description: 'path of topology file'
    })
    .option('master-zone', {
        alias: 'm',
        type: 'string',
        require: true,
        description: 'zone that need info backup'
    })
    .option('backup-zone', {
        alias: 'b',
        type: 'string',
        require: true,
        description: 'zone that need calculate RPO'
    })
    .option('save', {
        alias: 's',
        type: 'string',
        require: true,
        description: 'path to save cluster info backup'
    })
    .option('interval', {
        alias: 'i',
        type: 'number',
        default: 1,
        description: 'path to save cluster info backup (s)'
    })
    .argv


class RecoverInfoCollecter {
    constructor({ topology, masterZone, backupZone }) {
        const pdServer = topology['pd_servers'].map(({ host, client_port }) => {
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
                    config: { 'server.labels': { zone } } }
                ) => zone === backupZone
            )
            .map(({ host, port }) => {
                if (!port) {
                    port = 20180;
                }
                return `${host}:${port}`;
            });
        this.pdAddr = `${pdServer.host}:${pdServer.client_port}`;
        this.masterZone = masterZone;
        this.promDriver = new PrometheusDriver({
            endpoint: `http://${promServer.host}:${promServer.port}`,
            baseURL: "/api/v1"
        });
    }

    async collectStoreIds() {
        const { data } = await axios.get(`http://${this.pdAddr}/pd/api/v1/stores`);
        const { stores } = data;
        return stores
            .filter(({ store: { labels } }) => labels.map(({ value }) => value)
                .includes(this.masterZone))
            .map(({ store: { id } }) => id);
    }

    async collectAllocId() {
        const q = `pd_cluster_id{instance="${this.pdAddr}"}`;
        const metric = await this.promDriver.instantQuery(q);
        const series = metric.result;
        if (series.length == 0) {
            throw new Error('empty alloc id');
        }
        const id = series[series.length - 1].value.value;
        return parseInt(id) + (2 ** 32);
    }

    async collectClusterId() {
        const q = `pd_cluster_metadata{instance="${this.pdAddr}"}`;
        const metric = await this.promDriver.instantQuery(q);
        const series = metric.result;
        if (series.length == 0) {
            throw new Error('empty cluster id')
        }
        const type = series[series.length - 1].metric.labels.type;
        const clusterId = type.slice('cluster'.length)
        return clusterId;
    }

    async collectRPO() {
        const q = `sum(tikv_resolved_ts_min_resolved_ts_gap_seconds) by (instance)`;
        const metric = await this.promDriver.instantQuery(q);
        const series = metric.result;
        if (series.length == 0) {
            throw new Error('empty cluster id')
        }
        const rpos = series.map(({ metric: { labels: { instance } }, value: { value } }) => {
            return { instance, value };
        }).filter(({ instance }) => this.backupNodes.includes(instance));
        return rpos;
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

async function main(info) {
    const topology = yaml.load(fs.readFileSync(argv['topo']));

    const collector = new RecoverInfoCollecter({
        topology,
        masterZone: argv['master-zone'],
        backupZone: argv['backup-zone'],
    });

    try {
        const newInfo = await collector.collect()
        if (newInfo.rpos && newInfo.rpos.length > 0) {
            info = newInfo;
        } else {
            info.storeIds = newInfo.storeIds;
            info.allocId = newInfo.allocId;
            info.clusterId = newInfo.clusterId;
        }
        fs.writeFileSync(argv['save'], JSON.stringify(info));
        logger.info(`saved recover info to ${argv['save']}`);
    } catch (err) {
        logger.error(`fail to fetch backup info ${err.toString()}`)
    }
}

setInterval(async () => { let info; await main(info); }, argv['interval'] * 1000);
