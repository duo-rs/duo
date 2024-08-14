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
</script>

<div class="text-md mx-4 my-2 flex flex-row">
	<div class="flex grow flex-row flex-wrap">
		<div class="mx-2">{level}</div>
		<div class="mx-2">{dayjs(time / 1000).format('YYYY-MM-DD HH:mm:ss.SSS')}</div>
		<div class="mx-2">{message}</div>
		{#if $$restProps}
			{#each Object.entries($$restProps) as [key, value]}
				<div class="mx-2">
					<span>{key}</span>:<span>{value}</span>
				</div>
			{/each}
		{/if}
	</div>
	<Badge class="flex self-end" variant="outline">{process_id}</Badge>
</div>
<Separator />
