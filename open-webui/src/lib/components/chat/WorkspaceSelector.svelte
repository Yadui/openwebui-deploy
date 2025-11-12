<script lang="ts">
	import { onMount } from 'svelte';
	import { selectedPowerBIContext } from '$lib/stores/powerbiStore';
	import { fetchWorkspaces, fetchDatasets } from '$lib/apis/tools/powerbi';
	import { writable, get } from 'svelte/store';

	let workspaces = [];
	let datasets = [];
	let selectedWorkspace = writable(null);
	let selectedDataset = writable(null);
	let isOpen = writable(false);
	let isLoading = writable(false);

	const loadWorkspaces = async () => {
		isLoading.set(true);
		try {
			const res = await fetchWorkspaces();
			workspaces = res?.value ?? res; // adapt to your API
		} catch (err) {
			console.error('Failed to fetch workspaces:', err);
		} finally {
			isLoading.set(false);
		}
	};

	const loadDatasets = async (workspaceId: string) => {
		isLoading.set(true);
		try {
			const res = await fetchDatasets(workspaceId);
			datasets = res?.value ?? res;
		} catch (err) {
			console.error('Failed to fetch datasets:', err);
		} finally {
			isLoading.set(false);
		}
	};

	function toggleDropdown() {
		isOpen.update((v) => !v);
	}

	function selectWorkspace(ws) {
		selectedWorkspace.set(ws);
		selectedDataset.set(null);
		selectedPowerBIContext.set({ workspaceId: ws.id, datasetId: null });
		loadDatasets(ws.id);
	}

	function selectDataset(ds) {
		selectedDataset.set(ds);
		selectedPowerBIContext.set({
			workspaceId: get(selectedWorkspace)?.id,
			datasetId: ds.id
		});
		isOpen.set(false);
	}

	onMount(loadWorkspaces);
</script>

<div class="flex flex-col w-full items-start">
	<div class="flex w-full max-w-fit">
		<div class="overflow-hidden w-full">
			<div class="max-w-full mr-1">
				<div class="flex flex-col gap-2">
					<!-- Workspace Selector -->
					<div>
						<label class="text-xs font-medium text-gray-600 dark:text-gray-400 mb-1 block">
							Select Workspace
						</label>
						<select
							class="w-full rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none"
							on:change={(e) => selectWorkspace(workspaces.find(ws => ws.id === e.target.value))}
						>
							<option value="">Select a workspace</option>
							{#each workspaces as ws}
								<option value={ws.id} selected={get(selectedWorkspace)?.id === ws.id}>{ws.name}</option>
							{/each}
						</select>
					</div>

					<!-- Dataset Selector -->
					{#if $selectedWorkspace}
						<div>
							<label class="text-xs font-medium text-gray-600 dark:text-gray-400 mb-1 block">
								Select Dataset
							</label>
							<select
								class="w-full rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm px-3 py-2 focus:ring-2 focus:ring-green-500 focus:outline-none"
								on:change={(e) => selectDataset(datasets.find(ds => ds.id === e.target.value))}
							>
								<option value="">Select a dataset</option>
								{#each datasets as ds}
									<option value={ds.id} selected={get(selectedDataset)?.id === ds.id}>{ds.name}</option>
								{/each}
							</select>
						</div>
					{/if}
				</div>
			</div>
		</div>
	</div>
</div>

<style>
	@keyframes fadeIn {
		from { opacity: 0; transform: translateY(-6px); }
		to { opacity: 1; transform: translateY(0); }
	}
	.animate-fadeIn {
		animation: fadeIn 0.15s ease-out;
	}
</style>
