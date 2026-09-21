// 同一份输入、同一种并发、同一个 stopOnError 取值，两个入口各给什么读数。
import pMap, {pMapIterable} from '../index.js';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function fmtError(error) {
	if (error instanceof AggregateError) {
		return `AggregateError(${JSON.stringify(error.errors.map(e => e.message))})`;
	}

	return `${error && error.constructor.name}: ${error && error.message}`;
}

async function collect(asyncIterable) {
	const values = [];
	let error;

	try {
		for await (const value of asyncIterable) {
			values.push(value);
		}
	} catch (error_) {
		error = error_;
	}

	return {values, error};
}

// ---- Case A: mapper throws for element 3 (and 5), concurrency 1 ----
for (const [label, options] of [
	['default (no stopOnError)', {concurrency: 1}],
	['stopOnError:false', {concurrency: 1, stopOnError: false}],
]) {
	const mapped = [];
	const mapper = async value => {
		mapped.push(value);
		await sleep(5);
		if (value === 3 || value === 5) {
			throw new Error(`boom-${value}`);
		}

		return value * 10;
	};

	const input = [1, 2, 3, 4, 5, 6];

	const pmapResult = [];
	let pmapError;
	const pmapMapped = [];
	try {
		pmapResult.push(...await pMap(input, async value => {
			pmapMapped.push(value);
			await sleep(5);
			if (value === 3 || value === 5) {
				throw new Error(`boom-${value}`);
			}

			return value * 10;
		}, options));
	} catch (error) {
		pmapError = error;
	}

	const iterableOut = await collect(pMapIterable(input, mapper, options));

	console.log(`A | ${label}`);
	console.log(`  pMap          -> result=${JSON.stringify(pmapResult)} error=${fmtError(pmapError)} mappers run=${JSON.stringify(pmapMapped)}`);
	console.log(`  pMapIterable  -> yielded=${JSON.stringify(iterableOut.values)} error=${fmtError(iterableOut.error)} mappers run=${JSON.stringify(mapped)}`);
	console.log(`  identical? ${JSON.stringify(iterableOut.values) === JSON.stringify(iterableOut.values) && JSON.stringify(pmapMapped) === JSON.stringify(mapped) ? 'SAME mapper coverage' : 'DIFFERENT mapper coverage'}`);
}

// ---- Case B: rejected input ELEMENT (no mapper error at all), concurrency 1 ----
for (const [label, options] of [
	['default', {concurrency: 1}],
	['stopOnError:false', {concurrency: 1, stopOnError: false}],
]) {
	const makeInput = () => [1, Promise.reject(new Error('input 2 rejected')), 3, 4];

	let pmapResult;
	let pmapError;
	try {
		pmapResult = await pMap(makeInput(), async value => value * 10, options);
	} catch (error) {
		pmapError = error;
	}

	const iterableOut = await collect(pMapIterable(makeInput(), async value => value * 10, options));

	console.log(`B | ${label}`);
	console.log(`  pMap          -> result=${JSON.stringify(pmapResult)} error=${fmtError(pmapError)}`);
	console.log(`  pMapIterable  -> yielded=${JSON.stringify(iterableOut.values)} error=${fmtError(iterableOut.error)}`);
}

// ---- Case C: the option is swallowed, not validated ----
let validationError;
try {
	pMapIterable([1], async value => value, {concurrency: 1, stopOnError: false, nonsenseOption: 123});
} catch (error) {
	validationError = error;
}

console.log(`C | pMapIterable({stopOnError:false, nonsenseOption:123}) at call time -> ${fmtError(validationError) || 'no error, silently ignored'}`);

let pmapValidationError;
try {
	await pMap([1], async value => value, {concurrency: 1, nonsenseOption: 123});
} catch (error) {
	pmapValidationError = error;
}

console.log(`C | pMap({nonsenseOption:123}) -> ${fmtError(pmapValidationError) || 'no error'}`);

// ---- Case D: does the abandoned tail still hold values the consumer never sees? ----
const settled = [];
const out = await collect(pMapIterable([1, 2, 3, 4, 5, 6], async value => {
	const result = await sleep(5).then(() => {
		settled.push(value);
		return value * 10;
	});
	if (value === 2) {
		throw new Error('boom-2');
	}

	return result;
}, {concurrency: Number.POSITIVE_INFINITY, stopOnError: false}));

console.log(`D | concurrency:Infinity stopOnError:false -> yielded=${JSON.stringify(out.values)} error=${fmtError(out.error)} settled=${JSON.stringify(settled)}`);
