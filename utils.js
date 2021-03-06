module.exports = (function () {
	const itemize = function (param) {
		if (param === null) {
			return { NULL: true };
		} else if (param === "") {
			return { NULL: true };
		} else if (typeof param === "boolean") {
			return { BOOL: param };
		} else if (typeof param === "string") {
			return { S: param };
		} else if (param instanceof Date) {
			return { S: param.toString() };
		} else if (typeof param !== "object" && !Array.isArray(param) && !isNaN(param)) {
			return { N: "" + param };
		} else if (Array.isArray(param) && param.length > 0) {
			if (param.length === 1 && param[0] === null) {
				return { NULL: true };
			} else if (typeof param[0] === "string") {
				return { SS: param };
			} else if (typeof param[0] !== "object" && !isNaN(param[0])) {
				return {
					NS: param.map(function (n) {
						return "" + n;
					})
				};
			} else {
				return {
					L: param.map(function (value) {
						return itemize(value);
					})
				};
			}
		} else if (Array.isArray(param) && param.length === 0) {
			return { NULL: true };
		} else if (param && param.prototype) {
			const object = {};

			for (const key in param) {
				if (param.hasOwnProperty(key)) {
					const value = itemize(param[key]);
					if (value !== false) {
						object[key] = value;
					}
				}
			}

			return { M: object };
		} else {
			const object = {};

			for (const key in param) {
				const value = itemize(param[key]);
				if (value !== false) {
					object[key] = value;
				}
			}

			return { M: object };
		}
	};

	const deitemize = function (item) {
		if (!item) {
			return null;
		}

		if (item.S) {
			return item.S;
		}

		if (item.N) {
			return +item.N;
		}

		if (item.SS) {
			return item.SS;
		}

		if (item.NS) {
			return item.NS.map(function (n) {
				return +n;
			});
		}

		if (typeof item.BOOL !== "undefined") {
			return item.BOOL ? true : false;
		}

		if (item.NULL === true) {
			return null;
		}

		if (item.L) {
			const array = [];
			item.L.forEach(function (lItem) {
				array.push(deitemize(lItem));
			});

			return array;
		}

		if (item.M) {
			const object = {};
			Object.keys(item.M).forEach(function (key) {
				object[key] = deitemize(item.M[key]);
			});

			if (Object.keys(object).length > 0) {
				return object;
			}

			return null;
		}

		const items = {};
		Object.keys(item).forEach(function (key) {
			items[key] = deitemize(item[key]);
		});

		return items;
	};

	return {
		itemize: itemize,
		deitemize: deitemize
	};
})();
