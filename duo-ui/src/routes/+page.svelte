<script>
	import CalendarIcon from 'lucide-svelte/icons/calendar';
	import { Input } from '$lib/components/ui/input';
	import { Button } from '$lib/components/ui/button';
	import { onMount } from 'svelte';
	import { DatePicker } from '@svelte-plugins/datepicker';
	import { cn } from '$lib/utils';
	import dayjs from 'dayjs';

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
	let startDate;
	/**
	 * @type {number}
	 */
	let endDate;
	/**
	 * @type {string}
	 */
	let startDateTime;
	/**
	 * @type {string}
	 */
	let endDateTime;
	/**
	 * @type {number}
	 */
	let limit = 20;

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

	function onSearch() {}

	onMount(async () => {});
</script>

<div class="m-6">
	<div class="flex items-center">
		<Input class="mx-4 max-w-screen-md" placeholder="Search log by keyword" />
		<Button on:click={onSearch}>Search</Button>
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
	</div>
</div>
