/** @type {import('./$types').PageLoad} */
export async function load() {
    return {
        services: await getServices(),
        schema: await getLogSchema(),
    };
}

const API_URL = "http://localhost:3000";

async function getServices() {
    let response = await fetch(`${API_URL}/api/services`);
    return (await response.json()).data;

}

async function getLogSchema() {
    let response = await fetch(`${API_URL}/api/logs/schema`);
    return (await response.json());
}