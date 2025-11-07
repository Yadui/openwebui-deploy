import { PublicClientApplication } from '@azure/msal-browser';

export const msalConfig = {
	auth: {
		clientId: import.meta.env.VITE_MICROSOFT_CLIENT_ID,
		authority: `https://login.microsoftonline.com/${import.meta.env.VITE_MICROSOFT_CLIENT_TENANT_ID}`,
		redirectUri: window.location.origin
	}
};
export const msalInstance = new PublicClientApplication(msalConfig);
