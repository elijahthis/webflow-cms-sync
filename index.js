const express = require("express");
const axios = require("axios");
const dotenv = require("dotenv");
const cron = require("node-cron");
const { executeWithTiming } = require("./helpers");
const { cronSyncFunction, cronSyncFunction2 } = require("./cron");
const {
	profileSyncFunc,
	directoryByLocationSyncFunc,
	directoryByServiceSyncFunc,
	addWebflowIdToAirtableRecordsSyncFunc,
	serviceSyncFunc,
	disciplineSyncFunc,
	languagesSyncFunc,
	addWebflowIdToAirtableDisciplinesSyncFunc,
} = require("./syncFunc");
const {
	fetchRecentlyUpdatedProfilesFromAirtable,
	fetchRecentlyCreatedProfilesFromAirtable,
	fetchAllWebflowCMSRecords,
	fetchSingleProfileFromAirtable,
	fetchSingleDirectoryProfileFromAirtable,
	addItemToWebflowCMS,
} = require("./external-requests");

// Load environment variables from .env file
dotenv.config();

const app = express();
const port = process.env.PORT || 8000;

// State
const state = {
	profiles: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	webflow_ID_profiles: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	location_directory: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	service_directory: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	services: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	disciplines: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	languages: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
	webflow_ID_disciplines: {
		isJobRunning: false,
		lastCheckedDate: new Date("01-01-1970").toISOString(),
	},
};

app.get("/", (req, res) => {
	res.send("Hello, World!");
});

app.get("/test", async (req, res) => {
	try {
		console.log(req.query);
		const airtableProfile = await fetchSingleProfileFromAirtable(
			req.query.record_id
		);

		console.log({
			fieldData: {
				name: airtableProfile.fields["Vendor"],
				slug: airtableProfile.fields["Slug - Final"]?.trim(),
				"hq-city":
					airtableProfile.fields[
						"city_ascii (from City) (from Locations)"
					]?.[0],
				"hq-country": airtableProfile.fields["Country (from Locations)"]?.[0],
				cities:
					airtableProfile.fields[
						"city_ascii (from City) (from Locations)"
					]?.join(", "),
				countries:
					airtableProfile.fields["Country (from Locations)"]?.join(", "),
				"no-of-staff": airtableProfile.fields["No. of Staff"],
				languages: airtableProfile.fields["String Languages (from Languages)"],
				"testimonial-count":
					airtableProfile.fields["Client Testimonials"]?.length ?? 0,
				"no-of-awards": airtableProfile.fields["Awards"]?.length ?? 0,
				"is-in-person-2":
					airtableProfile.fields["Meeting Capabilities"]
						?.includes("In-person")
						?.toString() ?? "false",
				"is-emergency":
					airtableProfile.fields["Is Emergency (from Profile Type)"],
				"is-preferred-supplier":
					airtableProfile.fields["Is Preferred Supplier?"]?.toString(),
				service:
					airtableProfile.fields["String Service List [from Service (Parent)]"],
				discipline:
					airtableProfile.fields[
						"String Discipline List [from Discipline (Child)]"
					],
				logo: {
					url: airtableProfile.fields["Logo"]?.[0]?.thumbnails?.large?.url,
				},

				"title-tag-seo": airtableProfile.fields["MetaTitleFinal"],
				"meta-description-seo":
					airtableProfile.fields["Meta Description - Final"],
				"phone-number": airtableProfile.fields["Phone Number"]?.trim(),
				"email-address": airtableProfile.fields["Main Email"]?.trim(),
				tagline: airtableProfile.fields["Profile Tagline"],
				bio: `<p>${airtableProfile.fields["Vendor Bio"]
					?.replace(/\n\\/g, "<br><br><br>")
					?.replace(/\n/g, "<br><br>")}</p>`,

				"website-link": airtableProfile.fields["Website URL"]?.trim(),
				"profile-type": airtableProfile.fields["Name (from Profile Type)"]?.[0],
				"vendor-rating":
					Number(airtableProfile.fields["Total Vendor Rating"]?.toFixed(2)) ??
					0,
				// "vendor-rating": airtableProfile.fields["Total Vendor Rating"]?.toFixed(2),
				"badge-color":
					airtableProfile.fields["Name (from Profile Type)"][0] === "Enterprise"
						? "#EAAA08"
						: "#54F2D6",
			},
		});

		const webFl = addItemToWebflowCMS(
			process.env.WEBFLOW_VENDOR_COLLECTION_ID,
			{
				fieldData: {
					name: airtableProfile.fields["Vendor"],
					slug: airtableProfile.fields["Slug - Final"]?.trim(),
					"hq-city":
						airtableProfile.fields[
							"city_ascii (from City) (from Locations)"
						]?.[0],
					"hq-country": airtableProfile.fields["Country (from Locations)"]?.[0],
					cities:
						airtableProfile.fields[
							"city_ascii (from City) (from Locations)"
						]?.join(", "),
					countries:
						airtableProfile.fields["Country (from Locations)"]?.join(", "),
					"no-of-staff": airtableProfile.fields["No. of Staff"],
					languages:
						airtableProfile.fields["String Languages (from Languages)"],
					"testimonial-count":
						airtableProfile.fields["Client Testimonials"]?.length ?? 0,
					"no-of-awards": airtableProfile.fields["Awards"]?.length ?? 0,
					"is-in-person-2":
						airtableProfile.fields["Meeting Capabilities"]
							?.includes("In-person")
							?.toString() ?? "false",
					"is-emergency":
						airtableProfile.fields["Is Emergency (from Profile Type)"],
					"is-preferred-supplier":
						airtableProfile.fields["Is Preferred Supplier?"]?.toString(),
					service:
						airtableProfile.fields[
							"String Service List [from Service (Parent)]"
						],
					discipline:
						airtableProfile.fields[
							"String Discipline List [from Discipline (Child)]"
						],
					logo: {
						url: airtableProfile.fields["Logo"]?.[0]?.thumbnails?.large?.url,
					},

					"title-tag-seo": airtableProfile.fields["MetaTitleFinal"],
					"meta-description-seo":
						airtableProfile.fields["Meta Description - Final"],
					"phone-number": airtableProfile.fields["Phone Number"]?.trim(),
					"email-address": airtableProfile.fields["Main Email"]?.trim(),
					tagline: airtableProfile.fields["Profile Tagline"],
					bio: `<p>${airtableProfile.fields["Vendor Bio"]
						?.replace(/\n\\/g, "<br><br><br>")
						?.replace(/\n/g, "<br><br>")}</p>`,

					"website-link": airtableProfile.fields["Website URL"]?.trim(),
					"profile-type":
						airtableProfile.fields["Name (from Profile Type)"]?.[0],
					"vendor-rating":
						Number(airtableProfile.fields["Total Vendor Rating"]?.toFixed(2)) ??
						0,
					// "vendor-rating": airtableProfile.fields["Total Vendor Rating"]?.toFixed(2),
					"badge-color":
						airtableProfile.fields["Name (from Profile Type)"][0] ===
						"Enterprise"
							? "#EAAA08"
							: "#54F2D6",
				},
			}
			// res
		);

		res.json(webFl);
	} catch (error) {
		console.error("Error :", error);
		res.status(error.status || 500).json({
			error: "Failed to fetch Airtable record",
			error,
			errorData: error?.response?.data,
		});
	}
});

app.get("/test-2", async (req, res) => {
	try {
		console.log(req.query);
		const airtableProfile = await fetchSingleDirectoryProfileFromAirtable(
			req.query.record_id
		);

		const vendorObjList = airtableProfile.fields[
			"Webflow ID (from Vendor) (from Address for Service) (from City)"
		]
			? airtableProfile.fields[
					"Webflow ID (from Vendor) (from Address for Service) (from City)"
			  ]
					?.map((item, index) => ({
						webflowID:
							airtableProfile.fields[
								"Webflow ID (from Vendor) (from Address for Service) (from City)"
							]?.[index],
						vendor_rating:
							airtableProfile.fields[
								"Total Vendor Rating (from Vendor) (from Address for Service) (from City)"
							]?.[index],
					}))
					?.sort((a, b) => b.vendor_rating - a.vendor_rating)
			: [];

		console.log({
			fieldData: {
				name: airtableProfile.fields["H1 Title Text"],
				slug: airtableProfile.fields["Slug"]?.trim(),
				subheading: airtableProfile.fields["Hero Summary"],
				"breadcrumb-text": airtableProfile.fields["Breadcrumb Title"],
				city: airtableProfile.fields["city_ascii (from City)"]?.[0],
				country: airtableProfile.fields["Country"]?.[0],
				"featured-vendors":
					vendorObjList?.length > 0
						? vendorObjList?.slice(0, 2)?.map((item) => item?.webflowID)
						: [],
				vendors:
					vendorObjList?.length > 2
						? vendorObjList?.slice(2)?.map((item) => item?.webflowID)
						: [],
				"title-tag": airtableProfile.fields["Meta: Title"],
				"meta-description": airtableProfile.fields["Meta: Description"],
				"page-body-copy": airtableProfile.fields["Page Body Copy"] ?? "",
			},
		});

		res.json({});
	} catch (error) {
		console.error("Error :", error);
		res.status(error.status || 500).json({
			error: "Failed to fetch Airtable record",
			error,
			errorData: error?.response?.data,
		});
	}
});

const cronWrapper = async (syncFunction, stateKey) => {
	const cronAfterFunc = () => {
		state[stateKey].isJobRunning = false;
		console.log("CRON job is done.");
		state[stateKey].lastCheckedDate = new Date().toISOString();
	};

	// If the job is already running, exit early
	if (state[stateKey].isJobRunning) {
		console.log("Previous job is still running. Skipping this execution.");
		return;
	}

	// Set the flag to indicate that the job is now running
	state[stateKey].isJobRunning = true;

	try {
		executeWithTiming(
			async () =>
				await syncFunction(state[stateKey].lastCheckedDate, cronAfterFunc)
		);
	} catch (error) {
	} finally {
	}
};

// Schedule polling every 120 seconds --- profiles
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(profileSyncFunc, "profiles");
});

// Schedule polling every 120 seconds --- webflow_ID_profiles
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(addWebflowIdToAirtableRecordsSyncFunc, "webflow_ID_profiles");
});

// Schedule polling every 120 seconds --- location_directory
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(directoryByLocationSyncFunc, "location_directory");
});

// Schedule polling every 120 seconds --- service_directory
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(directoryByServiceSyncFunc, "service_directory");
});

// Schedule polling every 120 seconds --- services
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(serviceSyncFunc, "services");
});

// Schedule polling every 120 seconds --- disciplines
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(disciplineSyncFunc, "disciplines");
});

// Schedule polling every 120 seconds --- languages
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(languagesSyncFunc, "languages");
});

// Schedule polling every 120 seconds --- webflow_ID_disciplines
cron.schedule("*/90 * * * * *", async () => {
	cronWrapper(
		addWebflowIdToAirtableDisciplinesSyncFunc,
		"webflow_ID_disciplines"
	);
});

// Start the server
app.listen(port, () => {
	console.log(`Server is running on http://localhost:${port}`);
});
