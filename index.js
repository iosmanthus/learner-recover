const axios = require('axios');
const fs = require('fs');

const zone = 'master';
const pdUrl = 'http://172.16.4.188:2379';
const promUrl = 'http://172.16.4.193:9090';
const to = './recover-info.json';

class RecoverInfoCollecter {
    constructor({ pdAddr, promAddr, zone }) {
        this.pdAddr = pdAddr;
        this.promAddr = promAddr;
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
    const collector = new RecoverInfoCollecter({
        pdAddr: pdUrl,
        promAddr: promUrl,
        zone: zone
    });
    try {
        const info = await collector.collect()
        fs.writeFileSync(to, JSON.stringify(info));
        console.log(`saved recover info to ${to}`);
    } catch (err) {
        console.error(err.toString());
    }
}

setInterval(async () => { await main(); }, 1000);