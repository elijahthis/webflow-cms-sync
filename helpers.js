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

module.exports = { generateRealIndex, executeWithTiming };
