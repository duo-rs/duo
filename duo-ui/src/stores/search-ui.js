import { writable } from 'svelte/store';

function loadSearchUiConfig() {
    return JSON.parse(localStorage.getItem('config-log-search-ui') || '{}');
}

export const searchUiConfig = writable(loadSearchUiConfig());
searchUiConfig.subscribe(config => {
    localStorage.setItem('config-log-search-ui', JSON.stringify(config));
});