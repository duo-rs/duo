import { api } from '$lib/api';

export const ssr = false;

/**
 * @type {import('./$types').PageLoad}
 * @return {Promise<{services: Array<{}>, schema: {fields: any}}>}
 */
export async function load() {
  return {
    services: await api.getServices(),
    schema: await api.getSchema(),
  };
}
