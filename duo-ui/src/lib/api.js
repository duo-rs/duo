import ky from 'ky';

const API_URL = process.env.NODE_ENV === 'production' ? '/' : 'http://localhost:3000';

export const client = ky.extend({
    prefixUrl: API_URL,
});

export const api = {
    async getServices() {
        /** @type {{data: []}} */
        let response = await client.get('api/services').json();
        return response.data.sort();
    },
    /**
     * @returns {Promise<{fields: any}>}
     */
    async getSchema() {
        return await client.get('api/logs/schema').json();
    },
    /**
     * @param {URLSearchParams} searchParams
     * @returns {Promise<Object[]>}
     */
    async searchLogs(searchParams) {
        let response = await client.get("api/logs", {
            searchParams,
        });
        if (response.ok) {
            return response.json()
        } else {
            throw new Error(response.statusText);
        }
    },
    /** 
     * @param {string} field 
     * @param {URLSearchParams} searchParams
     * @returns {Promise<{count:number, value: string}[]>}
     */
    async getFieldStats(field, searchParams) {
        let response = await client.get(`api/logs/stats/${field}`, { searchParams });
        if (response.ok) {
            return response.json();
        } else {
            return [];
        }
    }
};

