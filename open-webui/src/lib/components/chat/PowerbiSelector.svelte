import { chats } from "$lib/stores/chats";
import { saveChat } from "$lib/apis/chats";

let selectedWorkspace = null;
let selectedDataset = null;
export let chat = null;

async function fetchSchema(workspaceId: string, datasetId: string) {
    try {
        const resp = await fetch(
            `/api/powerbi/schema/${workspaceId}/${datasetId}`,
            { method: "GET", credentials: "include" }
        );

        const data = await resp.json();

        if (data.error) {
            console.error("Schema fetch error:", data.error);
        } else {
            console.log("Fetched schema:", data.schema);
        }
    } catch (err) {
        console.error("Error fetching schema", err);
    }
}

function onWorkspaceSelect(event) {
    selectedWorkspace = event.target.value;
}

function onDatasetSelect(event) {
    selectedDataset = event.target.value;

    // 1. Fetch schema and save in backend
    fetchSchema(selectedWorkspace, selectedDataset);

    // 2. Add metadata to chat
    chat.metadata = {
        ...chat.metadata,
        powerbi_workspace_id: selectedWorkspace,
        powerbi_dataset_id: selectedDataset
    };

    // 3. Persist chat changes
    saveChat(chat.id, chat);
}