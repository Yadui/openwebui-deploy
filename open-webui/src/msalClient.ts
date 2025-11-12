import { PublicClientApplication, InteractionRequiredAuthError } from '@azure/msal-browser';

export const msalConfig = {
	auth: {
		clientId: import.meta.env.MICROSOFT_CLIENT_ID,
		authority: `https://login.microsoftonline.com/${import.meta.env.MICROSOFT_CLIENT_TENANT_ID}`,
		redirectUri: window.location.origin
	}
};

export const msalInstance = new PublicClientApplication(msalConfig);

// ✅ Correct utility to get a valid Power BI access token
export async function getValidAccessToken(
	scopes = ['https://analysis.windows.net/powerbi/api/.default']
) {
	const accounts = msalInstance.getAllAccounts();
	if (accounts.length === 0) throw new Error('No signed-in accounts found');

	try {
		const response = await msalInstance.acquireTokenSilent({
			account: accounts[0],
			scopes
		});
		return response.accessToken;
	} catch (error) {
		if (error instanceof InteractionRequiredAuthError) {
			const response = await msalInstance.acquireTokenPopup({
				account: accounts[0],
				scopes
			});
			return response.accessToken;
		}
		console.error('Access token acquisition failed:', error);
		throw error;
	}
}
