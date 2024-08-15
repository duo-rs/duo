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
	import { onMount } from 'svelte';
	import { DatePicker } from '@svelte-plugins/datepicker';
	import { cn } from '$lib/utils';
	import dayjs from 'dayjs';
	import LogItem from '$lib/components/LogItem.svelte';
	import Datatype from '$lib/components/Datatype.svelte';

	export const ssr = false;

	/** @type {import('./$types').PageData} */
	export let data;
	/**
	 * @type {string}
	 */
	let currentSevice;
	/**
	 * @type {string}
	 */
	let keyword;
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
	 * @type {number}
	 */
	let limit = 20;
	/**
	 * @type {Object[]}
	 */
	let logs = [];

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
			service: currentSevice,
			// start/end should be microseconds
			start: `${dateTimeToTimestamp(startDate, startDateTime)}000`,
			end: `${dateTimeToTimestamp(endDate, endDateTime)}000`
		});
		if (keyword) {
			params.append('keyword', keyword);
		}
		return params;
	}

	async function onSearch() {
		let params = queryParams();
		let response = await fetch(`http://localhost:3000/api/logs?${params.toString()}`);
		if (response.ok) {
			logs = [...logs, ...(await response.json())];
		}
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
				if (item.count > max) {
					max = item.count;
				}
			}
		}
		return {
			total,
			max,
			items
		};
	}

	onMount(async () => {
		if (data.services && data.services.length > 0) {
			currentSevice = data.services[0];
		}
		console.log(data);
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
			bind:value={currentSevice}
		></Svelecte>
		<Input class="mx-4 max-w-screen-md" placeholder="Search log by keyword" bind:value={keyword} />
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
		<Button on:click={onSearch}>Search</Button>
	</div>
	<Separator class="my-8" />
	<Resizable.PaneGroup direction="horizontal" class="rounded-lg border py-2">
		<Resizable.Pane defaultSize={18}>
			<h2 class="px-4 text-center text-lg font-bold">Fields</h2>
			{#each filterableFields() as field}
				<Collapsible.Root class="space-y-2 text-sm">
					<div class="space-x-4 px-4">
						<Collapsible.Trigger class="w-full">
							<div class="flex items-center py-1 text-slate-500 hover:bg-gray-100">
								<code class="flex grow">
									{field.name}
								</code>
								<Datatype type={field.data_type} />
							</div>
						</Collapsible.Trigger>
					</div>
					<Collapsible.Content class="space-y-2">
						<div class="px-4">
							{#await getFieldStats(field.name)}
								<p>loading...</p>
							{:then stats}
								{#each stats.items as { value, count }}
									<div class="flex items-center space-x-4 text-sm text-gray-400">
										<code class="flex grow">{value}</code>
										<code>{count}</code>
									</div>
									<Progress value={count} max={stats.max} class="h-2 w-full" />
								{/each}
							{:catch error}
								<p>Error: {error.message}</p>
							{/await}
						</div>
					</Collapsible.Content>
				</Collapsible.Root>
			{/each}
		</Resizable.Pane>
		<Resizable.Handle />
		<Resizable.Pane>
			<ScrollArea class="h-[75vh]">
				{#each logs as log}
					<LogItem {...log} />
				{/each}
			</ScrollArea>
		</Resizable.Pane>
	</Resizable.PaneGroup>
</div>
