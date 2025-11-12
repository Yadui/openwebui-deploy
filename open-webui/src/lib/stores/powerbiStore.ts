import { writable } from 'svelte/store';

export const selectedPowerBIContext = writable({
	workspaceId: null,
	datasetId: null
});
