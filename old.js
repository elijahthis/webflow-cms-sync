// // Schedule polling every 120 seconds
// cron.schedule("*/90 * * * * *", async () => {
// 	const cronAfterFunc = () => {
// 		setIsJobRunning(false);
// 		console.log("CRON job is done.");
// 		state.lastCheckedDate = new Date().toISOString();
// 	};

// 	// If the job is already running, exit early
// 	if (state.isJobRunning) {
// 		console.log("Previous job is still running. Skipping this execution.");
// 		return;
// 	}

// 	// Set the flag to indicate that the job is now running
// 	state.isJobRunning = true;

// 	try {
// 		executeWithTiming(
// 			async () => await profileSyncFunc(state.lastCheckedDate, cronAfterFunc)
// 		);
// 		executeWithTiming(
// 			async () =>
// 				await addWebflowIdToAirtableRecordsSyncFunc(state.lastCheckedDate)
// 		);
// 		executeWithTiming(
// 			async () =>
// 				await directoryByLocationSyncFunc(state.lastCheckedDate, cronAfterFunc)
// 		);
// 		executeWithTiming(
// 			async () =>
// 				await directoryByServiceSyncFunc(state.lastCheckedDate, cronAfterFunc)
// 		);
// 		executeWithTiming(
// 			async () => await serviceSyncFunc(state.lastCheckedDate, cronAfterFunc)
// 		);
// 		executeWithTiming(
// 			async () => await disciplineSyncFunc(state.lastCheckedDate, cronAfterFunc)
// 		);
// 		executeWithTiming(
// 			async () => await languagesSyncFunc(state.lastCheckedDate, cronAfterFunc)
// 		);
// 		executeWithTiming(
// 			async () =>
// 				await addWebflowIdToAirtableDisciplinesSyncFunc(
// 					state.lastCheckedDate,
// 					cronAfterFunc
// 				)
// 		);
// 	} catch (error) {
// 	} finally {
// 		// Reset the flag after the job is done
// 		// state.isJobRunning = false;
// 	}
// });
