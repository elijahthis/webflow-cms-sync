const axios = require("axios");
const dotenv = require("dotenv");
var Airtable = require("airtable");

// Load environment variables from .env file
dotenv.config();

// create and setup base
var base = new Airtable({ apiKey: process.env.AIRTABLE_TOKEN }).base(
	process.env.AIRTABLE_BASE_ID
);
const view = base
	.table("Profiles")
	.select({ view: "3 - Live on Fixinc", fields: ["Vendor"] });

const fetchProfilesFromAirtable = async (startIndex) => {
	// Make a request to Airtable API to fetch data
	const no_of_times = Math.floor(startIndex / 100) + 1;
	let responses = [];
	let offset;
	let url;

	console.log("no_of_times", no_of_times);

	for (let i = 0; i < no_of_times; i++) {
		console.log("Request#", i + 1);

		// Fetch 100 records at a time
		url = `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${
			process.env.AIRTABLE_PROFILE_TABLE_ID
		}?view=3%20-%20Live%20on%20Fixinc${offset ? `&offset=${offset}` : ""}`;

		try {
			let response = await axios.get(url, {
				headers: {
					Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
				},
			});
			responses.push(response.data);
			offset = response.data.offset;
			console.log(
				`response ${i + 1}`,
				response.data?.records?.length,
				offset,
				url
			);
		} catch (error) {
			console.log("Error fetching records", error.response);
			throw new Error("Error fetching records", error);
		}
	}

	// if (startIndex < 100) {
	// 	try {
	// 		let response = await axios.get(
	// 			`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${process.env.AIRTABLE_PROFILE_TABLE_ID}?view=3%20-%20Live%20on%20Fixinc`,
	// 			{
	// 				headers: {
	// 					Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
	// 				},
	// 			}
	// 		);
	// 		responses.push(response.data);
	// 	} catch (error) {
	// 		console.log("Error fetching remaining records", error.response);
	// 		throw new Error("Error fetching remaining records", error);
	// 	}
	// }

	// return [];
	return responses[responses.length - 1];
};

const addItemToWebflowCMS = async (
	collectionID,
	WEBFLOW_TOKEN,
	payload,
	res
) => {
	// console.log("payload", payload);

	try {
		// Make a request to Webflow CMS API to update data
		const response = await axios.post(
			`https://api.webflow.com/v2/collections/${collectionID}/items/live`,
			payload,
			{
				headers: {
					Authorization: `Bearer ${WEBFLOW_TOKEN}`,
				},
			}
		);

		return response.data;
	} catch (error) {
		// console.error("Error updating Webflow CMS:", error);
		// throw new Error("Failed to update Webflow CMS", error);
		if (res)
			res.status(error.status || 500).json({
				error: "Failed to update Webflow CMS",
				payload,
				error,
				errorData: error?.response?.data,
			});
	}
};

const updateWebflowCMSItem = async (
	collectionID,
	WEBFLOW_TOKEN,
	itemId,
	payload,
	res
) => {
	// console.log("payload", payload);

	try {
		// Make a request to Webflow CMS API to update data
		const response = await axios.patch(
			`https://api.webflow.com/v2/collections/${collectionID}/items/${itemId}`,
			payload,
			{
				headers: {
					Authorization: `Bearer ${WEBFLOW_TOKEN}`,
				},
			}
		);

		return response.data;
	} catch (error) {
		// console.error("Error updating Webflow CMS:", error);
		// throw new Error("Failed to update Webflow CMS", error);
		if (res)
			res.status(error.status || 500).json({
				error: "Failed to update Webflow CMS",
				payload,
				error,
				errorData: error?.response?.data,
			});
	}
};

const deleteWebflowCMSItem = async (
	collectionID,
	WEBFLOW_TOKEN,
	itemId,
	res
) => {
	// console.log("payload", payload);

	try {
		// Make a request to Webflow CMS API to update data
		const response = await axios.delete(
			`https://api.webflow.com/v2/collections/${collectionID}/items/${itemId}`,
			{
				headers: {
					Authorization: `Bearer ${WEBFLOW_TOKEN}`,
				},
			}
		);

		return response.data;
	} catch (error) {
		// console.error("Error updating Webflow CMS:", error);
		// throw new Error("Failed to update Webflow CMS", error);
		if (res)
			res.status(error.status || 500).json({
				error: "Failed to delete Webflow CMS",
				payload,
				error,
				errorData: error?.response?.data,
			});
	}
};

const fetchAirtableRecordsCount = async () => {
	try {
		const records = await view.all();

		const numRecordsInView = records.length;
		console.log("numRecordsInView", numRecordsInView);

		// const queryResult = await base
		// 	.table("Profiles")
		// 	._selectRecords({ view: "3 - Live on Fixinc", fields: ["Name"] })
		// 	.all();

		// console.log("queryResult", queryResult);

		// return queryResult.records?.length;
		return numRecordsInView;
	} catch (error) {
		console.error("Error polling Airtable:", error);
	}
};

// ------------------------------------------------- //
const fetchRecentlyUpdatedProfilesFromAirtable = async (lastCheckedDate) => {
	// Make a request to Airtable API to fetch data
	let responses = [];
	let offset;
	let url;

	do {
		// Fetch 100 records at a time
		url = `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${
			process.env.AIRTABLE_PROFILE_TABLE_ID
		}?view=3%20-%20Live%20on%20Fixinc${offset ? `&offset=${offset}` : ""}${
			lastCheckedDate
				? `&filterByFormula=IS_AFTER({Last%20Modified},%20"${lastCheckedDate}")`
				: ""
		}`;

		try {
			let response = await axios.get(url, {
				headers: {
					Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
				},
			});
			responses.push(response.data?.records);
			offset = response.data.offset;
			console.log(response.data?.records?.length, offset, url);
		} catch (error) {
			console.log("Error fetching records", error.response);
			throw new Error("Error fetching records", error);
		} finally {
			console.log("Fetched!");
		}
	} while (offset);

	return responses.flat();
};

const fetchRecentlyCreatedProfilesFromAirtable = async (lastCheckedDate) => {
	// Make a request to Airtable API to fetch data
	let responses = [];
	let offset;
	let url;

	do {
		// Fetch 100 records at a time
		url = `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${
			process.env.AIRTABLE_PROFILE_TABLE_ID
		}?view=3%20-%20Live%20on%20Fixinc${offset ? `&offset=${offset}` : ""}${
			lastCheckedDate
				? `&filterByFormula=IS_AFTER({Created},%20"${lastCheckedDate}")`
				: ""
		}`;

		try {
			let response = await axios.get(url, {
				headers: {
					Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
				},
			});
			responses.push(response.data?.records);
			offset = response.data.offset;
			console.log(response.data?.records?.length, offset, url);
		} catch (error) {
			console.log("Error fetching records", error.response);
			throw new Error("Error fetching records", error);
		} finally {
			console.log("Fetched!");
		}
	} while (offset);

	return responses.flat();
};

const fetchSingleProfileFromAirtable = async (recordId, res) => {
	// Make a request to Airtable API to fetch data
	url = `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${process.env.AIRTABLE_PROFILE_TABLE_ID}/${recordId}`;
	let response;
	try {
		response = await axios.get(url, {
			headers: {
				Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
			},
		});
	} catch (error) {
		// res.status(error.status || 500).json({
		// 	error: "Failed to fetch Airtable record",
		// 	error,
		// 	errorData: error?.response?.data,
		// });
		console.log("Error fetching records", error.response);
		throw new Error("Error fetching records", error);
	}
	return response?.data;
};

const publishWebflowCMSItems = async (
	collectionID,
	WEBFLOW_TOKEN,
	payload,
	res
) => {
	try {
		// Make a request to Webflow CMS API to update data
		const response = await axios.post(
			`https://api.webflow.com/v2/collections/${collectionID}/items/publish`,
			payload,
			{
				headers: {
					Authorization: `Bearer ${WEBFLOW_TOKEN}`,
				},
			}
		);

		return response.data;
	} catch (error) {
		// console.error("Error updating Webflow CMS:", error);
		// throw new Error("Failed to update Webflow CMS", error);
		if (res)
			res.status(error.status || 500).json({
				error: "Failed to publish Webflow CMS",
				payload,
				error,
				errorData: error?.response?.data,
			});
	}
};

// ------------------------------------------------- //
const fetchRecentlyUpdatedDirectoriesFromAirtable = async (
	lastCheckedDate,
	tableId
) => {
	// Make a request to Airtable API to fetch data
	let responses = [];
	let offset;
	let url;

	do {
		// Fetch 100 records at a time
		url = `https://api.airtable.com/v0/${
			process.env.AIRTABLE_BASE_ID
		}/${tableId}?view=Live%20View${offset ? `&offset=${offset}` : ""}${
			lastCheckedDate
				? `&filterByFormula=IS_AFTER({Last%20Modified},%20"${lastCheckedDate}")`
				: ""
		}`;
		// view=Grid%20View
		try {
			let response = await axios.get(url, {
				headers: {
					Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
				},
			});
			responses.push(response.data?.records);
			offset = response.data.offset;
			console.log(response.data?.records?.length, offset, url);
		} catch (error) {
			console.log("Error fetching records", error.response);
			throw new Error("Error fetching records", error);
		} finally {
			console.log("Fetched!");
		}
	} while (offset);

	return responses.flat();
};

const fetchWebflowCMSRecordsCount = async (collectionID) => {
	try {
		const response = await axios.get(
			`https://api.webflow.com/v2/collections/${collectionID}/items?limit=1`,
			{
				headers: {
					Authorization: `Bearer ${process.env.WEBFLOW_TOKEN_GENERAL}`,
				},
			}
		);
		const numRecordsInWebflow = response.data.pagination.total;
		console.log("numRecordsInWebflow", numRecordsInWebflow);

		return numRecordsInWebflow;
	} catch (error) {
		console.error("Error polling Webflow CMS:", error);
	}
};

const fetchAllWebflowCMSRecords = async (collectionID) => {
	try {
		const recordCount = await fetchWebflowCMSRecordsCount(collectionID);

		const no_of_times = Math.floor(recordCount / 100) + 1;
		let responses = [];

		for (let i = 0; i < no_of_times; i++) {
			console.log(`Number ${i + 1}`);
			const response = await axios.get(
				`https://api.webflow.com/v2/collections/${collectionID}/items?limit=100&offset=${
					i * 100
				}`,
				{
					headers: {
						Authorization: `Bearer ${process.env.WEBFLOW_TOKEN_GENERAL}`,
					},
				}
			);
			responses.push(response.data?.items);
		}

		return responses.flat();
	} catch (error) {
		console.error("Error polling Webflow CMS:", error);
	}
};

const fetchSingleDirectoryProfileFromAirtable = async (recordId, res) => {
	// Make a request to Airtable API to fetch data
	url = `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${process.env.AIRTABLE_DIRECTORY_BY_LOCATION_VENDOR_TABLE_ID}/${recordId}`;
	let response;
	try {
		response = await axios.get(url, {
			headers: {
				Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
			},
		});
	} catch (error) {
		// res.status(error.status || 500).json({
		// 	error: "Failed to fetch Airtable record",
		// 	error,
		// 	errorData: error?.response?.data,
		// });
		console.log("Error fetching records", error.response);
		throw new Error("Error fetching records", error);
	}
	return response?.data;
};

// ------------------------------------------------- //
const fetchRecentlyUpdatedServicesFromAirtable = async (
	lastCheckedDate,
	tableId,
	view = undefined
) => {
	// Make a request to Airtable API to fetch data
	let responses = [];
	let offset;
	let url;

	do {
		// Fetch 100 records at a time
		url = `https://api.airtable.com/v0/${
			process.env.AIRTABLE_BASE_ID
		}/${tableId}?${view ? `view=${view}&` : ""}${
			offset ? `offset=${offset}` : ""
		}${
			lastCheckedDate
				? `&filterByFormula=IS_AFTER({Last%20Modified},%20"${lastCheckedDate}")`
				: ""
		}`;
		// view=Grid%20View
		console.log("url", url);
		try {
			let response = await axios.get(url, {
				headers: {
					Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
				},
			});
			responses.push(response.data?.records);
			offset = response.data.offset;
			console.log(response.data?.records?.length, offset, url);
		} catch (error) {
			console.log("Error fetching records", error.response);
			throw new Error("Error fetching records", error);
		} finally {
			console.log("Fetched!");
		}
	} while (offset);

	return responses.flat();
};
// ------------------------------------------------- //

// ------------------------------------------------- //
const modifyAirtableRecord = async (tableId, recordId, payload, res) => {
	// Make a request to Airtable API to update data
	url = `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${tableId}/${recordId}`;
	let response;
	try {
		response = await axios.patch(url, payload, {
			headers: {
				Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
			},
		});
	} catch (error) {
		// res.status(error.status || 500).json({
		// 	error: "Failed to fetch Airtable record",
		// 	error,
		// 	errorData: error?.response?.data,
		// });
		console.log("Error fetching records", error.response);
		throw new Error("Error fetching records", error);
	}
	return response?.data;
};

module.exports = {
	fetchProfilesFromAirtable,
	addItemToWebflowCMS,
	fetchAirtableRecordsCount,
	fetchRecentlyUpdatedProfilesFromAirtable,
	fetchRecentlyCreatedProfilesFromAirtable,
	updateWebflowCMSItem,
	fetchSingleProfileFromAirtable,
	fetchRecentlyUpdatedDirectoriesFromAirtable,
	fetchAllWebflowCMSRecords,
	fetchSingleDirectoryProfileFromAirtable,
	modifyAirtableRecord,
	fetchRecentlyUpdatedServicesFromAirtable,
	publishWebflowCMSItems,
	deleteWebflowCMSItem,
};
