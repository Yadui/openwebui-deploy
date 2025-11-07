<script lang="ts">
	import { onMount, createEventDispatcher } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { user } from '$lib/stores';
	import { msalInstance } from '../../../msalClient';
	const dispatch = createEventDispatcher();

	let workspaces: any[] = [];
	let datasets = {};
	let selectedWorkspace = '';
	let selectedDataset = '';

	// Fetch all Power BI workspaces and datasets

	onMount(async () => {
		try {
			// ✅ Extract Entra token from cookies instead of localStorage
			const cookieToken = undefined;

			console.log('🪪 Using oauth_id_token (first 40 chars):');
			// Fetch workspaces
			const wsResponse = await fetch('/api/powerbi/workspaces', {
				headers: {},
				credentials: 'include'
			});

			if (!wsResponse.ok) throw new Error(await wsResponse.text());
			workspaces = await wsResponse.json();

			// Fetch datasets for each workspace
			for (const ws of workspaces) {
				const dsResponse = await fetch(`/api/powerbi/workspaces/${ws.id}/datasets`, {
					headers: {},
					credentials: 'include'
				});
				if (dsResponse.ok) datasets[ws.id] = await dsResponse.json();
			}
		} catch (err) {
			console.error('❌ Failed to load Power BI data:', err);
			toast.error('Failed to fetch Power BI workspaces');
		}
	});

	function selectWorkspace(id: string) {
		selectedWorkspace = id;
		selectedDataset = '';
		dispatch('updateSelection', { workspace: selectedWorkspace, dataset: selectedDataset });
	}

	function selectDataset(id: string) {
		selectedDataset = id;
		dispatch('updateSelection', { workspace: selectedWorkspace, dataset: selectedDataset });
	}
</script>

<!-- Styling to align with ModelSelector -->
<div class="flex items-center space-x-2">
	<div class="relative">
		<select
			bind:value={selectedWorkspace}
			class="px-2 py-1 rounded-lg bg-gray-100 dark:bg-gray-800 text-sm border-none focus:ring-2 focus:ring-blue-500"
			on:change={(e) => selectWorkspace(e.target.value)}
		>
			<option value="">Select workspace</option>
			{#each workspaces as ws}
				<option value={ws.id}>{ws.name}</option>
			{/each}
		</select>
	</div>

	{#if selectedWorkspace && datasets[selectedWorkspace]}
		<div class="relative">
			<select
				bind:value={selectedDataset}
				class="px-2 py-1 rounded-lg bg-gray-100 dark:bg-gray-800 text-sm border-none focus:ring-2 focus:ring-blue-500"
				on:change={(e) => selectDataset(e.target.value)}
			>
				<option value="">Select dataset</option>
				{#each datasets[selectedWorkspace] as ds}
					<option value={ds.id}>{ds.name}</option>
				{/each}
			</select>
		</div>
	{/if}
</div>
