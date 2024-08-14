<script>
	import Svelecte from 'svelecte';
	import CalendarIcon from 'lucide-svelte/icons/calendar';
	import { Input } from '$lib/components/ui/input';
	import { Button } from '$lib/components/ui/button';
	import { Separator } from '$lib/components/ui/separator';
	import { onMount } from 'svelte';
	import { DatePicker } from '@svelte-plugins/datepicker';
	import { cn } from '$lib/utils';
	import dayjs from 'dayjs';
	import LogItem from '$lib/components/LogItem.svelte';

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

	async function onSearch() {
		let params = new URLSearchParams({
			service: currentSevice,
			// start/end should be microseconds
			start: `${dateTimeToTimestamp(startDate, startDateTime)}000`,
			end: `${dateTimeToTimestamp(endDate, endDateTime)}000`
		});
		if (keyword) {
			params.append('keyword', keyword);
		}
		let response = await fetch(`http://localhost:3000/api/logs?${params.toString()}`);
		if (response.ok) {
			logs = [...logs, ...(await response.json())];
		}
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
	{#each logs as log}
		<LogItem {...log} />
	{/each}
</div>
