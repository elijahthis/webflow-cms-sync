const {
	fetchProfilesFromAirtable,
	addItemToWebflowCMS,
	fetchAirtableRecordsCount,
	fetchWebflowCMSRecordsCount,
	fetchRecentlyUpdatedProfilesFromAirtable,
	fetchAllWebflowCMSRecords,
	updateWebflowCMSItem,
	fetchRecentlyUpdatedDirectoriesFromAirtable,
	fetchAllWebflowCMSDirectoryRecords,
	modifyAirtableRecord,
} = require("./external-requests");
const { generateRealIndex, executeWithTiming } = require("./helpers");

const profileSyncFunc = async (lastCheckedDate) => {
	try {
		const updatedAirtableProfiles =
			await fetchRecentlyUpdatedProfilesFromAirtable(lastCheckedDate);
		const allWebflowCMSRecords = await fetchAllWebflowCMSRecords();

		if (updatedAirtableProfiles.length === 0) {
			console.log("No updated profiles found in Airtable.");
			return [];
		}

		const batchSize = 15;
		let startIndex = 0;
		let endIndex = Math.min(batchSize, updatedAirtableProfiles.length);

		const responses = [];

		while (startIndex < updatedAirtableProfiles.length) {
			const batchAirtableProfiles = updatedAirtableProfiles.slice(
				startIndex,
				endIndex
			);

			const webflowUpdatePromises = batchAirtableProfiles.map(
				async (airtableProfile) => {
					let response;
					const webflowProfile = allWebflowCMSRecords.find(
						(webflowProfile) =>
							webflowProfile.fieldData.slug ===
							airtableProfile.fields["Slug - Final"]
					);

					if (webflowProfile) {
						response = await updateWebflowCMSItem(
							process.env.WEBFLOW_VENDOR_COLLECTION_ID,
							webflowProfile?.id,
							{
								fieldData: {
									name: airtableProfile.fields["Vendor"],
									slug: airtableProfile.fields["Slug - Final"]?.trim(),
									"hq-city":
										airtableProfile.fields[
											"city_ascii (from City) (from Locations)"
										]?.[0],
									"hq-country":
										airtableProfile.fields["Country (from Locations)"]?.[0],
									cities:
										airtableProfile.fields[
											"city_ascii (from City) (from Locations)"
										]?.join(", "),
									countries:
										airtableProfile.fields["Country (from Locations)"]?.join(
											", "
										),
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
										airtableProfile.fields[
											"Is Preferred Supplier?"
										]?.toString(),
									service:
										airtableProfile.fields[
											"String Service List [from Service (Parent)]"
										],
									discipline:
										airtableProfile.fields[
											"String Discipline List [from Discipline (Child)]"
										],
									logo: {
										url: airtableProfile.fields["Logo"]?.[0]?.thumbnails?.large
											?.url,
									},
									"title-tag-seo": airtableProfile.fields["MetaTitleFinal"],
									"meta-description-seo":
										airtableProfile.fields["Meta Description - Final"],
									"phone-number":
										airtableProfile.fields["Phone Number"]?.trim(),
									"email-address": airtableProfile.fields["Main Email"]?.trim(),
									tagline: airtableProfile.fields["Profile Tagline"],
									bio: `<p>${airtableProfile.fields["Vendor Bio"]
										?.replace(/\n\\/g, "<br><br><br>")
										?.replace(/\n/g, "<br><br>")}</p>`,
									"website-link": airtableProfile.fields["Website URL"]?.trim(),
									"profile-type":
										airtableProfile.fields["Name (from Profile Type)"]?.[0],
									"vendor-rating":
										Number(
											airtableProfile.fields["Total Vendor Rating"]?.toFixed(2)
										) ?? 0,
									// "vendor-rating": airtableProfile.fields["Total Vendor Rating"]?.toFixed(2),
									"badge-color":
										airtableProfile.fields["Name (from Profile Type)"][0] ===
										"Enterprise"
											? "#EAAA08"
											: "#54F2D6",
								},
							}
						);
						console.log(
							`Updating Webflow CMS record with ID ${webflowProfile.id} ${webflowProfile?.fieldData?.name}...`
						);
						console.log("Not today...");
					} else {
						response = await addItemToWebflowCMS(
							process.env.WEBFLOW_VENDOR_COLLECTION_ID,
							{
								fieldData: {
									name: airtableProfile.fields["Vendor"],
									slug: airtableProfile.fields["Slug - Final"]?.trim(),
									"hq-city":
										airtableProfile.fields[
											"city_ascii (from City) (from Locations)"
										]?.[0],
									"hq-country":
										airtableProfile.fields["Country (from Locations)"]?.[0],
									cities:
										airtableProfile.fields[
											"city_ascii (from City) (from Locations)"
										]?.join(", "),
									countries:
										airtableProfile.fields["Country (from Locations)"]?.join(
											", "
										),
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
										airtableProfile.fields[
											"Is Preferred Supplier?"
										]?.toString(),
									service:
										airtableProfile.fields[
											"String Service List [from Service (Parent)]"
										],
									discipline:
										airtableProfile.fields[
											"String Discipline List [from Discipline (Child)]"
										],
									logo: {
										url: airtableProfile.fields["Logo"]?.[0]?.thumbnails?.large
											?.url,
									},

									"title-tag-seo": airtableProfile.fields["MetaTitleFinal"],
									"meta-description-seo":
										airtableProfile.fields["Meta Description - Final"],
									"phone-number":
										airtableProfile.fields["Phone Number"]?.trim(),
									"email-address": airtableProfile.fields["Main Email"]?.trim(),
									tagline: airtableProfile.fields["Profile Tagline"],
									bio: `<p>${airtableProfile.fields["Vendor Bio"]
										?.replace(/\n\\/g, "<br><br><br>")
										?.replace(/\n/g, "<br><br>")}</p>`,

									"website-link": airtableProfile.fields["Website URL"]?.trim(),
									"profile-type":
										airtableProfile.fields["Name (from Profile Type)"]?.[0],
									"vendor-rating":
										Number(
											airtableProfile.fields["Total Vendor Rating"]?.toFixed(2)
										) ?? 0,
									// "vendor-rating": airtableProfile.fields["Total Vendor Rating"]?.toFixed(2),
									"badge-color":
										airtableProfile.fields["Name (from Profile Type)"][0] ===
										"Enterprise"
											? "#EAAA08"
											: "#54F2D6",
								},
							}
						);
						console.log(
							`Creating a new Webflow CMS record for ${airtableProfile.fields["Vendor"]}...`
						);
					}

					return response;
				}
			);

			const batchResponses = await Promise.all(webflowUpdatePromises);
			responses.push(...batchResponses);

			// if (responses.length < 45) {
			startIndex = endIndex;
			endIndex = Math.min(
				startIndex + batchSize,
				updatedAirtableProfiles.length
			);
			// }
		}

		console.log(responses.length, "Profiles Updated/Created in Webflow CMS:");

		return responses;
	} catch (error) {
		console.log(error);
		return [];
	}
};

const directoryByLocationSyncFunc = async (lastCheckedDate) => {
	try {
		const updatedAirtableProfiles =
			await fetchRecentlyUpdatedDirectoriesFromAirtable(
				lastCheckedDate,
				process.env.AIRTABLE_DIRECTORY_BY_LOCATION_VENDOR_TABLE_ID
			);
		const allWebflowCMSRecords = await fetchAllWebflowCMSDirectoryRecords();

		console.log(updatedAirtableProfiles.length, "Updated Directories Found.");
		if (updatedAirtableProfiles.length === 0) {
			console.log("No updated Directories found in Airtable.");
			return [];
		}

		const batchSize = 15;
		let startIndex = 0;
		let endIndex = Math.min(batchSize, updatedAirtableProfiles.length);

		const responses = [];

		while (startIndex < updatedAirtableProfiles.length) {
			const batchAirtableProfiles = updatedAirtableProfiles.slice(
				startIndex,
				endIndex
			);
			console.log("startIndex", startIndex, "endIndex", endIndex);
			// console.log(
			// 	"Batch Airtable Profiles: ",
			// 	batchAirtableProfiles.map((airtableProfile) => ({
			// 		name: airtableProfile.fields["H1 Title Text"],
			// 		vendors: airtableProfile.fields?.[
			// 			"Webflow ID (from Vendor) (from Address for Service) (from City)"
			// 		]?.map(
			// 			(item, index) =>
			// 				airtableProfile.fields[
			// 					"Vendor (from Vendor) (from Address for Service) (from City)"
			// 				]?.[index]
			// 		),
			// 	}))
			// );

			const webflowUpdatePromises = batchAirtableProfiles.map(
				async (airtableProfile) => {
					let response;
					const webflowProfile = allWebflowCMSRecords.find(
						(webflowProfile) =>
							webflowProfile.fieldData.slug === airtableProfile.fields["Slug"]
					);

					if (webflowProfile) {
						const vendorObjList = airtableProfile.fields?.[
							"Webflow ID (from Vendor) (from Address for Service) (from City)"
						]
							? airtableProfile.fields?.[
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
						response = await updateWebflowCMSItem(
							process.env.WEBFLOW_DIRECTORY_COLLECTION_ID,
							webflowProfile?.id,
							{
								fieldData: {
									name: airtableProfile.fields["H1 Title Text"],
									slug: airtableProfile.fields["Slug"]?.trim(),
									subheading: airtableProfile.fields["Hero Summary"],
									"hero-tag-summary":
										airtableProfile.fields["Hero Tag Summary"],
									"breadcrumb-text": airtableProfile.fields["Breadcrumb Title"],
									city: airtableProfile.fields["city_ascii (from City)"]?.[0],
									country: airtableProfile.fields["Country"]?.[0],
									"featured-vendors":
										vendorObjList?.length > 0
											? vendorObjList
													?.slice(0, 2)
													?.map((item) => item?.webflowID)
											: [],
									vendors:
										vendorObjList?.length > 2
											? vendorObjList?.slice(2)?.map((item) => item?.webflowID)
											: [],
									"title-tag": airtableProfile.fields["Meta: Title"],
									"meta-description":
										airtableProfile.fields["Meta: Description"],
									"page-body-copy":
										airtableProfile.fields["Page Body Copy"] ?? "",
								},
							}
						);
						console.log(
							`Updating Webflow CMS record with ID ${webflowProfile.id} ${webflowProfile?.fieldData?.name}...`
						);
						// console.log("Not today...");
					} else {
						const vendorObjList = airtableProfile.fields?.[
							"Webflow ID (from Vendor) (from Address for Service) (from City)"
						]
							? airtableProfile.fields?.[
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

						response = await addItemToWebflowCMS(
							process.env.WEBFLOW_DIRECTORY_COLLECTION_ID,
							{
								fieldData: {
									name: airtableProfile.fields["H1 Title Text"],
									slug: airtableProfile.fields["Slug"]?.trim(),
									subheading: airtableProfile.fields["Hero Summary"],
									"hero-tag-summary":
										airtableProfile.fields["Hero Tag Summary"],
									"breadcrumb-text": airtableProfile.fields["Breadcrumb Title"],
									city: airtableProfile.fields["city_ascii (from City)"]?.[0],
									country: airtableProfile.fields["Country"]?.[0],
									"featured-vendors":
										vendorObjList?.length > 0
											? vendorObjList
													?.slice(0, 2)
													?.map((item) => item?.webflowID)
											: [],
									vendors:
										vendorObjList?.length > 2
											? vendorObjList?.slice(2)?.map((item) => item?.webflowID)
											: [],
									"title-tag": airtableProfile.fields["Meta: Title"],
									"meta-description":
										airtableProfile.fields["Meta: Description"],
									"page-body-copy":
										airtableProfile.fields["Page Body Copy"] ?? "",
								},
							}
						);
						console.log(
							`Creating a new Webflow CMS record for ${airtableProfile.fields["H1 Title Text"]} ${airtableProfile.fields["Slug"]} ...`
						);
					}

					return response;
				}
			);

			const batchResponses = await Promise.all(webflowUpdatePromises);
			responses.push(...batchResponses);

			// if (responses.length < 45) {
			startIndex = endIndex;
			endIndex = Math.min(
				startIndex + batchSize,
				updatedAirtableProfiles.length
			);
			// }
		}

		console.log(
			responses.length,
			"Directories Updated/Created in Webflow CMS:"
		);

		return responses;
	} catch (error) {
		console.log(error);
		return [];
	}
};
const directoryByServiceSyncFunc = async (lastCheckedDate) => {
	try {
		const updatedAirtableProfiles =
			await fetchRecentlyUpdatedDirectoriesFromAirtable(
				lastCheckedDate,
				process.env.AIRTABLE_DIRECTORY_BY_SERVICE_DISCIPLINES_TABLE_ID
			);
		const allWebflowCMSRecords = await fetchAllWebflowCMSDirectoryRecords();

		console.log(updatedAirtableProfiles.length, "Updated Directories Found.");
		if (updatedAirtableProfiles.length === 0) {
			console.log("No updated Directories found in Airtable.");
			return [];
		}

		const batchSize = 15;
		let startIndex = 0;
		let endIndex = Math.min(batchSize, updatedAirtableProfiles.length);

		const responses = [];

		while (startIndex < updatedAirtableProfiles.length) {
			const batchAirtableProfiles = updatedAirtableProfiles.slice(
				startIndex,
				endIndex
			);
			console.log("startIndex", startIndex, "endIndex", endIndex);
			// console.log(
			// 	"Batch Airtable Profiles: ",
			// 	batchAirtableProfiles.map((airtableProfile) => ({
			// 		name: airtableProfile.fields["H1 Title Text"],
			// 		vendors: airtableProfile.fields?.[
			// 			"Webflow ID (from Profiles) (from Disciplines)"
			// 		]?.map(
			// 			(item, index) =>
			// 				airtableProfile.fields[
			// 					"Vendor (from Profiles) (from Disciplines)"
			// 				]?.[index]
			// 		),
			// 	}))
			// );

			const webflowUpdatePromises = batchAirtableProfiles.map(
				async (airtableProfile) => {
					let response;
					const webflowProfile = allWebflowCMSRecords.find(
						(webflowProfile) =>
							webflowProfile.fieldData.slug === airtableProfile.fields["Slug"]
					);

					if (webflowProfile) {
						const vendorObjList = airtableProfile.fields?.[
							"Webflow ID (from Profiles) (from Disciplines)"
						]
							? airtableProfile.fields?.[
									"Webflow ID (from Profiles) (from Disciplines)"
							  ]
									?.map((item, index) => ({
										webflowID:
											airtableProfile.fields[
												"Webflow ID (from Profiles) (from Disciplines)"
											]?.[index],
										vendor_rating:
											airtableProfile.fields[
												"Total Vendor Rating (from Profiles) (from Disciplines)"
											]?.[index],
									}))
									?.sort((a, b) => b.vendor_rating - a.vendor_rating)
							: [];

						response = await updateWebflowCMSItem(
							process.env.WEBFLOW_DIRECTORY_COLLECTION_ID,
							webflowProfile?.id,
							{
								fieldData: {
									name: airtableProfile.fields["H1 Title Text"],
									slug: airtableProfile.fields["Slug"]?.trim(),
									subheading: airtableProfile.fields["Hero Summary"],
									"breadcrumb-text": airtableProfile.fields["Breadcrumb Title"],
									"hero-tag-summary":
										airtableProfile.fields["Hero Tag Summary"],
									city: airtableProfile.fields?.["city_ascii (from City)"]?.[0],
									country: airtableProfile.fields?.["Country"]?.[0],
									"featured-vendors":
										vendorObjList?.length > 0
											? vendorObjList
													?.slice(0, 2)
													?.map((item) => item?.webflowID)
											: [],
									vendors:
										vendorObjList?.length > 2
											? vendorObjList?.slice(2)?.map((item) => item?.webflowID)
											: [],
									"title-tag": airtableProfile.fields["Meta: Title"],
									"meta-description":
										airtableProfile.fields["Meta: Description"],
									"page-body-copy":
										airtableProfile.fields["Page Body Copy"] ?? "",
								},
							}
						);
						console.log(
							`Updating Webflow CMS record with ID ${webflowProfile.id} ${webflowProfile?.fieldData?.name}...`
						);
						// console.log("Not today...");
					} else {
						const vendorObjList = airtableProfile.fields?.[
							"Webflow ID (from Profiles) (from Disciplines)"
						]
							? airtableProfile.fields?.[
									"Webflow ID (from Profiles) (from Disciplines)"
							  ]
									?.map((item, index) => ({
										webflowID:
											airtableProfile.fields[
												"Webflow ID (from Profiles) (from Disciplines)"
											]?.[index],
										vendor_rating:
											airtableProfile.fields[
												"Total Vendor Rating (from Profiles) (from Disciplines)"
											]?.[index],
									}))
									?.sort((a, b) => b.vendor_rating - a.vendor_rating)
							: [];

						response = await addItemToWebflowCMS(
							process.env.WEBFLOW_DIRECTORY_COLLECTION_ID,
							{
								fieldData: {
									name: airtableProfile.fields["H1 Title Text"],
									slug: airtableProfile.fields["Slug"]?.trim(),
									subheading: airtableProfile.fields["Hero Summary"],
									"breadcrumb-text": airtableProfile.fields["Breadcrumb Title"],
									"hero-tag-summary":
										airtableProfile.fields["Hero Tag Summary"],
									city: airtableProfile.fields?.["city_ascii (from City)"]?.[0],
									country: airtableProfile.fields?.["Country"]?.[0],
									"featured-vendors":
										vendorObjList?.length > 0
											? vendorObjList
													?.slice(0, 2)
													?.map((item) => item?.webflowID)
											: [],
									vendors:
										vendorObjList?.length > 2
											? vendorObjList?.slice(2)?.map((item) => item?.webflowID)
											: [],
									"title-tag": airtableProfile.fields["Meta: Title"],
									"meta-description":
										airtableProfile.fields["Meta: Description"],
									"page-body-copy":
										airtableProfile.fields["Page Body Copy"] ?? "",
								},
							}
						);
						console.log(
							`Creating a new Webflow CMS record for ${airtableProfile.fields["H1 Title Text"]} ${airtableProfile.fields["Slug"]} ...`
						);
					}

					return response;
				}
			);

			const batchResponses = await Promise.all(webflowUpdatePromises);
			responses.push(...batchResponses);

			// if (responses.length < 45) {
			startIndex = endIndex;
			endIndex = Math.min(
				startIndex + batchSize,
				updatedAirtableProfiles.length
			);
			// }
		}

		console.log(
			responses.length,
			"Directories Updated/Created in Webflow CMS:"
		);

		return responses;
	} catch (error) {
		console.log(error);
		return [];
	}
};

const addWebflowIdToAirtableRecordsSyncFunc = async (lastCheckedDate) => {
	try {
		const updatedAirtableProfiles =
			await fetchRecentlyUpdatedProfilesFromAirtable(lastCheckedDate);
		const allWebflowCMSRecords = await fetchAllWebflowCMSRecords();

		if (updatedAirtableProfiles.length === 0) {
			console.log("No updated profiles found in Airtable.");
			return [];
		}

		const batchSize = 15;
		let startIndex = 0;
		let endIndex = Math.min(batchSize, updatedAirtableProfiles.length);

		const responses = [];

		while (startIndex < updatedAirtableProfiles.length) {
			const batchAirtableProfiles = updatedAirtableProfiles.slice(
				startIndex,
				endIndex
			);

			const webflowUpdatePromises = batchAirtableProfiles.map(
				async (airtableProfile) => {
					let response;
					const webflowProfile = allWebflowCMSRecords.find(
						(webflowProfile) =>
							webflowProfile.fieldData.slug ===
							airtableProfile.fields["Slug - Final"]
					);

					if (webflowProfile) {
						response = await modifyAirtableRecord(
							process.env.AIRTABLE_PROFILE_TABLE_ID,
							airtableProfile.id,
							{
								fields: {
									"Webflow ID": webflowProfile.id,
								},
							}
						);

						console.log(
							`Updating Webflow CMS record with ID ${webflowProfile.id} ${webflowProfile?.fieldData?.name}...`
						);
						// console.log("Not today...");
					} else {
						console.log(`No matching profile found in Webflow CMS.`);
					}

					return response;
				}
			);

			const batchResponses = await Promise.all(webflowUpdatePromises);
			responses.push(...batchResponses);

			// if (responses.length < 45) {
			startIndex = endIndex;
			endIndex = Math.min(
				startIndex + batchSize,
				updatedAirtableProfiles.length
			);
			// }
		}

		console.log(responses.length, "Webflow IDs Added to Airtable:");

		return responses;
	} catch (error) {
		console.log(error);
		return [];
	}
};

module.exports = {
	profileSyncFunc,
	directoryByLocationSyncFunc,
	directoryByServiceSyncFunc,
	addWebflowIdToAirtableRecordsSyncFunc,
};
