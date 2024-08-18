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
  import { onMount, onDestroy } from 'svelte';
  import { DatePicker } from '@svelte-plugins/datepicker';
  import { cn } from '$lib/utils';
  import dayjs from 'dayjs';
  import LogItem from '$lib/components/LogItem.svelte';
  import Datatype from '$lib/components/Datatype.svelte';
  import { writable } from 'svelte/store';
  import { api } from '$lib/api';

  const searchUi = writable(JSON.parse(localStorage.getItem('config-log-search-ui') || '{}'));
  let searchUiUnsubscribe = searchUi.subscribe((config) => {
    localStorage.setItem('config-log-search-ui', JSON.stringify(config));
  });

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
      (/** @type {{ name: string; }} */ field) => !excludedFields.includes(field.name),
    );
  }

  function queryParams() {
    let params = new URLSearchParams({
      service: $searchUi.currentSevice,
      limit: `${$searchUi.perPage}`,
      skip: `${logs.length}`,
      // start/end should be microseconds
      start: `${dateTimeToTimestamp(startDate, startDateTime)}000`,
      end: `${dateTimeToTimestamp(endDate, endDateTime)}000`,
    });
    if ($searchUi.expr) {
      params.append('expr', $searchUi.expr);
    }
    return params;
  }

  async function search() {
    logs = [];
    logs = await api.searchLogs(queryParams());
  }

  /**
   * @param {string} field
   */
  async function getFieldStats(field) {
    let max = 0;
    let total = 0;
    let items = await api.getFieldStats(field, queryParams());
    for (let item of items) {
      total += item.count;
      max = Math.max(max, item.count);
    }
    return {
      total,
      max,
      items,
    };
  }

  /**
   * @param {import('svelte-infinite-loading').InfiniteEvent} event
   */
  async function infiniteHandler({ detail: { loaded, complete, error } }) {
    try {
      let newBatch = await api.searchLogs(queryParams());
      console.log('infiniteHandler, len:', newBatch.length);
      logs = [...logs, ...newBatch];
      if (newBatch.length < $searchUi.perPage) {
        complete();
      } else {
        loaded();
      }
    } catch (e) {
      error();
      console.error(e);
    }
  }

  /**
   * @param {KeyboardEvent} event
   */
  function onKeydown(event) {
    if (event.key === 'Enter') {
      search();
    }
  }

  onMount(async () => {
    if (!$searchUi.currentSevice && data.services && data.services.length > 0) {
      $searchUi.currentSevice = data.services[0];
    }
    if (!$searchUi.perPage) {
      $searchUi.perPage = 50;
    }
    await search();
  });

  onDestroy(() => {
    searchUiUnsubscribe();
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
      bind:value={$searchUi.currentSevice}
      on:change={search}
    ></Svelecte>
    <Input
      class="mx-4 max-w-screen-md"
      placeholder="Input sql filter expression (e.g level='DEBUG') or any keyword"
      bind:value={$searchUi.expr}
      on:keydown={onKeydown}
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
            !startDate && 'text-muted-foreground',
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
  <Resizable.PaneGroup direction="horizontal" class="rounded-lg border">
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
      <div
        class="flex w-full flex-row items-center bg-gray-100 text-start text-xs font-semibold uppercase tracking-wider text-gray-600"
      >
        <div class="min-w-[180px] px-1 py-2">Timestamp</div>
        <div class="min-w-[55px] px-1 py-2">Level</div>
        <div class="grow px-1 py-2">Message</div>
        <div class="min-w-[100px] px-1 py-2">Fields</div>
        <div class="min-w-[100px] justify-end px-1 py-2">Process</div>
      </div>
      {#if logs.length > 0}
        <ScrollArea class="h-[75vh]">
          {#each logs as log}
            <LogItem {...log} keyword={$searchUi.expr} />
          {/each}
          <InfiniteLoading on:infinite={infiniteHandler} />
        </ScrollArea>
      {:else}
        <p class="text-center">No logs found.</p>
      {/if}
    </Resizable.Pane>
  </Resizable.PaneGroup>
  <div
    class="absolute bottom-0 right-0 flex flex-row items-center justify-end rounded bg-gray-100 py-2 px-4"
  >
    <span>
      Loaded: {logs.length}
    </span>
    <span class="pl-4">
      Per page:
      <select bind:value={$searchUi.perPage} on:change={search} class="border">
        {#each [10, 20, 50, 100, 500] as perPage}
          <option value={perPage}>{perPage}</option>
        {/each}
      </select>
    </span>
  </div>
</div>
