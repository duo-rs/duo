<script>
	import dayjs from 'dayjs';
	import { Badge } from '$lib/components/ui/badge/index.js';
	import { Separator } from '$lib/components/ui/separator';

	/**
	 * @type {string}
	 */
	export let message;
	/**
	 * @type {string}
	 */
	export let process_id;
	/**
	 * @type {number}
	 */
	export let trace_id;
	/**
	 * @type {number}
	 */
	export let span_id;
	/**
	 * @type {string}
	 */
	export let level;
	/**
	 * @type {string}
	 */
	export let target;
	/**
	 * @type {string}
	 */
	export let file;
	/**
	 * @type {number}
	 */
	export let line;
	/**
	 * @type {number}
	 */
	export let time;
	let expand = false;
	let levelColor = 'white';

	$: {
		switch (level.toLowerCase()) {
			case 'info':
				levelColor = '#206ce8';
				break;
			case 'warn':
				levelColor = '#ff9c00';
				break;
			case 'error':
				levelColor = '#f55555';
				break;
			case 'debug':
			default:
				levelColor = '#47474a';
				break;
		}
	}

	function allFields() {
		return {
			message,
			process_id,
			trace_id,
			span_id,
			level,
			target,
			file: `${file}:${line}`,
			time: dayjs(time / 1000).format('YYYY-MM-DD HH:mm:ss.SSS'),
			...$$restProps
		};
	}
</script>

<button
	class="text-md flex w-full flex-row px-4 py-2 text-sm text-slate-600 hover:cursor-pointer"
	on:click={() => (expand = !expand)}
>
	<div class="flex grow flex-row flex-wrap">
		<div class="text-slate-500">{dayjs(time / 1000).format('YYYY-MM-DD HH:mm:ss.SSS')}</div>
		<div class="flex" style:color={levelColor}>
			<div class="mx-2 w-10"><code>{level}</code></div>
			<div class="mx-2">{message}</div>
		</div>
		{#if $$restProps}
			{#each Object.entries($$restProps) as [key, value]}
				<div class="mx-2">
					<span class="mr-1 rounded-sm bg-slate-100 px-2 py-1">{key}:</span><span>{value}</span>
				</div>
			{/each}
		{/if}
	</div>
	<Badge class="flex items-center self-end" variant="outline">{process_id}</Badge>
</button>
<Separator />
{#if expand}
	<div class="p-4">
		{#each Object.entries(allFields()) as [key, value]}
			<div class="grid grid-cols-8 gap-4 text-sm text-slate-500">
				<code class="col-span-1 text-end">{key}</code>
				<div class="col-span-7">{value || ''}</div>
			</div>
			<Separator />
		{/each}
	</div>
{/if}
