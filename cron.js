const {
	fetchAirtableRecordsCount,
	fetchWebflowCMSRecordsCount,
	fetchRecentlyUpdatedProfilesFromAirtable,
	fetchAllWebflowCMSRecords,
	addItemToWebflowCMS,
	updateWebflowCMSItem,
} = require("./external-requests");
const { profileSyncFunc } = require("./syncFunc");

const cronSyncFunction2 = async (
	lastCheckedDate = new Date().toISOString()
) => {
	console.log("Polling Airtable...");

	try {
		await profileSyncFunc();
	} catch (error) {
	} finally {
		console.log("Polled!");
	}
};

module.exports = { cronSyncFunction2 };
