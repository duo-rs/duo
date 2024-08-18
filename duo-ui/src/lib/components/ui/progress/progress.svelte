<script>
  import { Progress as ProgressPrimitive } from 'bits-ui';
  import { cn } from '$lib/utils.js';
  let className = undefined;
  export let max = 100;
  export let value = undefined;
  export { className as class };
</script>

<ProgressPrimitive.Root
  class={cn('relative h-4 w-full overflow-hidden rounded-full bg-secondary', className)}
  {...$$restProps}
>
  <div
    class="h-full w-full flex-1 bg-primary transition-all"
    style={`transform: translateX(-${100 - (100 * (value ?? 0)) / (max ?? 1)}%)`}
  ></div>
</ProgressPrimitive.Root>
