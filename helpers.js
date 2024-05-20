const generateRealIndex = (index, start = true) => {
	const no_of_times = Math.floor(index / 100);
	if (index < 100 || (!start && index % 100 === 0)) {
		return index;
	} else {
		return index - no_of_times * 100;
	}
};

async function executeWithTiming(func) {
	const startTime = Date.now();
	await func();
	const endTime = Date.now();
	const executionTime = endTime - startTime;
	console.log(`Function took ${executionTime / 1000} seconds to execute.`);
}

function filterExcessItemsToDelete(arr1, arr2, key1, key2) {
	const slugSet = new Set(arr2.map((obj) => obj?.fields?.[key2]));
	return arr1.filter((obj) => !slugSet.has(obj?.fieldData?.[key1]));
}

module.exports = {
	generateRealIndex,
	executeWithTiming,
	filterExcessItemsToDelete,
};
