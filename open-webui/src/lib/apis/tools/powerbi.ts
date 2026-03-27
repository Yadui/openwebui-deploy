export async function fetchWorkspaces() {
	const response = await fetch('/api/powerbi/workspaces', {
		credentials: 'include'
	});
	if (!response.ok) throw new Error('Failed to fetch Power BI workspaces');
	return response.json();
}

export async function fetchDatasets(workspaceId: string) {
	const response = await fetch(`/api/powerbi/workspaces/${workspaceId}/datasets`, {
		credentials: 'include'
	});
	if (!response.ok) throw new Error('Failed to fetch Power BI datasets');
	return response.json();
}

export async function fetchSchema(workspaceId: string, datasetId: string) {
	const response = await fetch(`/api/powerbi/schema/${workspaceId}/${datasetId}`, {
		credentials: 'include'
	});
	if (!response.ok) throw new Error('Failed to fetch Power BI schema');
	return response.json();
}
