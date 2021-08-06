const axios = require('axios');
const yaml = require('js-yaml');
const fs = require('fs');
const yargs = require('yargs/yargs')
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
    .option('topo', {
        alias: 't',
        type: 'string',
        require: true,
        description: 'path of topology file'
    })
    .option('zone', {
        alias: 'z',
        type: 'string',
        require: true,
        description: 'zone that need info backup'
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
    constructor({ topology, zone }) {
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
        this.pdAddr = `http://${pdServer.host}:${pdServer.client_port}`;
        this.promAddr = `http://${promServer.host}:${promServer.port}`;
        this.zone = zone;
    }
    async collectStoreIds() {
        const { data } = await axios.get(this.pdAddr + '/pd/api/v1/stores');
        const { stores } = data;
        return stores
            .filter(({ store: { labels } }) => labels.map(({ value }) => value)
                .includes(this.zone))
            .map(({ store: { id } }) => id);
    }

    async collectAllocId() {
        const { data } = await axios.get(this.promAddr + '/api/v1/query' + '?query=pd_cluster_id');
        const { data: { result: [{ value: [_, id] }] } } = data;
        return parseInt(id) + 4294967296;
    }

    async collectClusterId() {
        const { data } = await axios.get(this.promAddr + '/api/v1/query' + '?query=pd_cluster_metadata');
        const { data: { result: [{ metric: { type } }] } } = data;
        return type.slice('cluster'.length);
    }

    async collect() {
        const storeIds = await this.collectStoreIds();
        const clusterId = await this.collectClusterId();
        const allocId = await this.collectAllocId();
        return {
            storeIds,
            clusterId,
            allocId
        };
    }
}

async function main() {
    console.log(argv['topo']);
    const topology = yaml.load(fs.readFileSync(argv['topo']));

    const collector = new RecoverInfoCollecter({
        topology,
        zone: argv['zone'],
    });

    try {
        const info = await collector.collect()
        fs.writeFileSync(argv['save'], JSON.stringify(info));
        console.log(`saved recover info to ${argv['save']}`);
    } catch (err) {
        console.error(err.toString());
    }
}

setInterval(async () => { await main(); }, argv['interval'] * 1000);