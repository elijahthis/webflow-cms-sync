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

const addItemToWebflowCMS = async (collectionID, payload, res) => {
	// console.log("payload", payload);

	try {
		// Make a request to Webflow CMS API to update data
		const response = await axios.post(
			`https://api.webflow.com/v2/collections/${collectionID}/items/live`,
			payload,
			{
				headers: {
					Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
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

const updateWebflowCMSItem = async (collectionID, itemId, payload, res) => {
	// console.log("payload", payload);

	try {
		// Make a request to Webflow CMS API to update data
		const response = await axios.patch(
			`https://api.webflow.com/v2/collections/${collectionID}/items/${itemId}`,
			payload,
			{
				headers: {
					Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
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

const fetchWebflowCMSRecordsCount = async () => {
	try {
		const response = await axios.get(
			`https://api.webflow.com/v2/collections/${process.env.WEBFLOW_VENDOR_COLLECTION_ID}/items?limit=1`,
			{
				headers: {
					Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
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

const fetchAllWebflowCMSRecords = async () => {
	try {
		const recordCount = await fetchWebflowCMSRecordsCount();

		const no_of_times = Math.floor(recordCount / 100) + 1;
		let responses = [];

		for (let i = 0; i < no_of_times; i++) {
			console.log(`Number ${i + 1}`);
			const response = await axios.get(
				`https://api.webflow.com/v2/collections/${
					process.env.WEBFLOW_VENDOR_COLLECTION_ID
				}/items?limit=100&offset=${i * 100}`,
				{
					headers: {
						Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
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

const fetchWebflowCMSDirectoryRecordsCount = async () => {
	try {
		const response = await axios.get(
			`https://api.webflow.com/v2/collections/${process.env.WEBFLOW_DIRECTORY_COLLECTION_ID}/items?limit=1`,
			{
				headers: {
					Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
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

const fetchAllWebflowCMSDirectoryRecords = async () => {
	try {
		const recordCount = await fetchWebflowCMSDirectoryRecordsCount();

		const no_of_times = Math.floor(recordCount / 100) + 1;
		let responses = [];

		for (let i = 0; i < no_of_times; i++) {
			console.log(`Number ${i + 1}`);
			const response = await axios.get(
				`https://api.webflow.com/v2/collections/${
					process.env.WEBFLOW_DIRECTORY_COLLECTION_ID
				}/items?limit=100&offset=${i * 100}`,
				{
					headers: {
						Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
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
	fetchWebflowCMSRecordsCount,
	fetchRecentlyUpdatedProfilesFromAirtable,
	fetchRecentlyCreatedProfilesFromAirtable,
	fetchAllWebflowCMSRecords,
	updateWebflowCMSItem,
	fetchSingleProfileFromAirtable,
	fetchRecentlyUpdatedDirectoriesFromAirtable,
	fetchAllWebflowCMSDirectoryRecords,
	fetchSingleDirectoryProfileFromAirtable,
	modifyAirtableRecord,
};
