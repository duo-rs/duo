<script>
  import dayjs from 'dayjs';
  import { Badge } from '$lib/components/ui/badge/index.js';
  import { Separator } from '$lib/components/ui/separator';

  /**
   * Search keyword for matching
   * @type {string}
   */
  export let keyword;
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
      ...$$restProps,
    };
  }

  /**
   * @param {string} text
   */
  function matchKeyword(text) {
    if (!keyword) return text;
    const regex = new RegExp(`(${keyword})`, 'gi');
    return text.replace(regex, '<span class="rounded py-1 bg-[#fffe55]">$1</span>');
  }
</script>

<button
  class="text-md flex w-full flex-row items-center py-1 text-xs text-slate-600 hover:cursor-pointer"
  on:click={() => (expand = !expand)}
>
  <div class="flex grow flex-row items-center text-start" style:color={levelColor}>
    <div class="min-w-[180px] whitespace-nowrap p-1 text-slate-500">
      <code>{dayjs(time / 1000).format('YYYY-MM-DD HH:mm:ss.SSS')}</code>
    </div>
    <div class="min-w-[55px] p-1"><code>{level}</code></div>
    <div class="flex grow flex-wrap p-1 text-start text-sm">
      {@html matchKeyword(message)}
    </div>
    <div class="flex min-w-28 max-w-28 flex-wrap text-ellipsis">
      {#if $$restProps}
        {#each Object.entries($$restProps) as [key, value]}
          <div class="my-1">
            <span class="mr-1 rounded-sm bg-slate-100 px-2 py-1">{key}:</span><span>{value}</span>
          </div>
        {/each}
      {/if}
    </div>
  </div>
  <div class="min-w-[100px] text-start justify-end">
    <Badge
      class="inline max-w-24  text-ellipsis whitespace-nowrap font-normal"
      variant="outline"
      >{process_id}
    </Badge>
  </div>
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
