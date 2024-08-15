<script>
	import Svelecte from 'svelecte';
	import CalendarIcon from 'lucide-svelte/icons/calendar';
	import { Progress } from '$lib/components/ui/progress';
	import { Input } from '$lib/components/ui/input';
	import { Button } from '$lib/components/ui/button';
	import { Separator } from '$lib/components/ui/separator';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import * as Collapsible from '$lib/components/ui/collapsible';
	import * as Resizable from '$lib/components/ui/resizable';
	import InfiniteLoading from 'svelte-infinite-loading';
	import { onMount } from 'svelte';
	import { DatePicker } from '@svelte-plugins/datepicker';
	import { cn } from '$lib/utils';
	import dayjs from 'dayjs';
	import LogItem from '$lib/components/LogItem.svelte';
	import Datatype from '$lib/components/Datatype.svelte';
	import { searchUiConfig } from '../stores/search-ui';

	/** @type {import('./$types').PageData} */
	export let data;
	/**
	 * @type {number}
	 */
	let startDate = dayjs().subtract(30, 'minute').valueOf();
	/**
	 * @type {number}
	 */
	let endDate = dayjs().valueOf();
	/**
	 * @type {string}
	 */
	let startDateTime = dayjs().subtract(30, 'minute').format('HH:mm');
	/**
	 * @type {string}
	 */
	let endDateTime = dayjs().format('HH:mm');
	/**
	 * @type {Object[]}
	 */
	let logs = [];
	let limit = 50;
	let isOpen = false;
	/**
	 * @type {string}
	 */
	let pickedDateTimeRange = '';

	$: {
		if (startDate && endDate) {
			pickedDateTimeRange = `${dayjs(startDate).format('MMM DD,YYYY')} ${startDateTime} - ${dayjs(endDate).format('MMM DD,YYYY')} ${endDateTime}`;
		}
	}

	const toggleDatePicker = () => (isOpen = !isOpen);

	/**
	 * @param {number} date
	 * @param {string} time
	 *
	 * @returns {number} the timestamps
	 */
	function dateTimeToTimestamp(date, time) {
		let [hour, minute] = time.split(':');
		return dayjs(date).hour(parseInt(hour)).minute(parseInt(minute)).valueOf();
	}

	function filterableFields() {
		let excludedFields = ['message', 'time', 'line'];
		return data.schema.fields.filter(
			(/** @type {{ name: string; }} */ field) => !excludedFields.includes(field.name)
		);
	}

	function queryParams() {
		let params = new URLSearchParams({
			service: $searchUiConfig.currentSevice,
			limit: `${limit}`,
			skip: `${logs.length}`,
			// start/end should be microseconds
			start: `${dateTimeToTimestamp(startDate, startDateTime)}000`,
			end: `${dateTimeToTimestamp(endDate, endDateTime)}000`
		});
		if ($searchUiConfig.keyword) {
			params.append('keyword', $searchUiConfig.keyword);
		}
		return params;
	}

	async function fetchLogs() {
		let params = queryParams();
		let response = await fetch(`http://localhost:3000/api/logs?${params.toString()}`);
		if (response.ok) {
			return await response.json();
		} else {
			throw new Error(response.statusText);
		}
	}

	async function search() {
		logs = [];
		logs = await fetchLogs();
	}

	/**
	 * @param {string} field
	 */
	async function getFieldStats(field) {
		let items = [];
		let max = 0;
		let total = 0;
		let params = queryParams();
		let response = await fetch(
			`http://localhost:3000/api/logs/stats/${field}?${params.toString()}`
		);
		if (response.ok) {
			items = await response.json();
			for (let item of items) {
				total += item.count;
				max = Math.max(max, item.count);
			}
		}
		return {
			total,
			max,
			items
		};
	}

	/**
	 * @param event {import('svelte-infinite-loading').InfiniteEvent}
	 */
	async function infiniteHandler({ detail: { loaded, complete, error } }) {
		try {
			let newBatch = await fetchLogs();
			console.log('infiniteHandler, len:', newBatch.length);
			logs = [...logs, ...newBatch];
			if (newBatch.length < limit) {
				complete();
			} else {
				loaded();
			}
		} catch (e) {
			error();
			console.error(e);
		}
	}

	onMount(async () => {
		if (!$searchUiConfig.currentSevice && data.services && data.services.length > 0) {
			$searchUiConfig.currentSevice = data.services[0];
		}
		await search();
	});
</script>

<div class="m-6">
	<div class="mx-4 flex items-center">
		Service:
		<Svelecte
			class="ml-4"
			options={data.services}
			searchable={false}
			resetOnBlur={false}
			bind:value={$searchUiConfig.currentSevice}
			on:change={search}
		></Svelecte>
		<Input
			class="mx-4 max-w-screen-md"
			placeholder="Search log by keyword"
			bind:value={$searchUiConfig.keyword}
		/>
		<div class="mx-6">
			<DatePicker
				bind:isOpen
				bind:startDate
				bind:endDate
				bind:startDateTime
				bind:endDateTime
				align={'right'}
				isRange
				isMultipane
				showTimePicker
				showPresets
				enableFutureDates={false}
			>
				<Button
					variant="outline"
					class={cn(
						'w-[300px] justify-start text-left font-normal',
						!startDate && 'text-muted-foreground'
					)}
					on:click={toggleDatePicker}
				>
					<CalendarIcon class="mr-2 h-4 w-4" />
					{pickedDateTimeRange ? pickedDateTimeRange : 'Pick a date'}
				</Button>
			</DatePicker>
		</div>
		<Button on:click={search}>Search</Button>
	</div>
	<Separator class="my-8" />
	<Resizable.PaneGroup direction="horizontal" class="rounded-lg border py-2">
		<Resizable.Pane defaultSize={18} minSize={12} maxSize={24}>
			<h2 class="px-4 py-2 text-center text-lg font-bold">Fields</h2>
			{#each filterableFields() as field}
				<Collapsible.Root class="my-1 text-sm">
					<Collapsible.Trigger class="w-full">
						<div class="flex items-center px-4 py-1 text-slate-500 hover:bg-gray-100">
							<code class="flex grow">
								{field.name}
							</code>
							<Datatype type={field.data_type} />
						</div>
					</Collapsible.Trigger>
					<Collapsible.Content class="px-4">
						{#await getFieldStats(field.name)}
							<p>loading...</p>
						{:then stats}
							{#each stats.items as { value, count }}
								<div class="flex items-center text-xs text-gray-400">
									<code class="flex grow text-nowrap">{value}</code>
									<code class="w-min-[10px]">{count}</code>
								</div>
								<Progress value={count} max={stats.max} class="my-1 h-1 w-full" />
							{/each}
						{:catch error}
							<p>Error: {error.message}</p>
						{/await}
					</Collapsible.Content>
				</Collapsible.Root>
			{/each}
		</Resizable.Pane>
		<Resizable.Handle />
		<Resizable.Pane defaultSize={80} minSize={48}>
			{#if logs.length > 0}
				<ScrollArea class="h-[75vh]">
					{#each logs as log}
						<LogItem {...log} />
					{/each}
					<InfiniteLoading on:infinite={infiniteHandler} />
				</ScrollArea>
			{:else}
				<p class="text-center">No logs found.</p>
			{/if}
		</Resizable.Pane>
	</Resizable.PaneGroup>
</div>
