const modules = new Map([
	["virtual:emdash/config", "export default {};"],
	["virtual:emdash/object-cache", "export const createObjectCache = undefined;"],
	["virtual:emdash/wait-until", "export const waitUntil = undefined;"],
]);
export function resolve(specifier, context, nextResolve) {
	if (modules.has(specifier)) {
		return {
			url: "data:text/javascript," + encodeURIComponent(modules.get(specifier)),
			shortCircuit: true,
		};
	}
	return nextResolve(specifier, context);
}
export async function load(url, context, nextLoad) {
	const loaded = await nextLoad(url, context);
	const builtCore = new URL("../../packages/core/dist/", import.meta.url).href;
	if (url.startsWith(builtCore) && loaded.format === "module" && loaded.source) {
		const source =
			typeof loaded.source === "string" ? loaded.source : new TextDecoder().decode(loaded.source);
		return {
			...loaded,
			source: "import.meta.env = Object.freeze({ DEV: false, PROD: true });\n" + source,
		};
	}
	return loaded;
}
