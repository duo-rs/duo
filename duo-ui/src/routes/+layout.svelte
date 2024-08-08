<script>
	const menus = [
		{
			name: 'Log',
			path: '/'
		},
		{
			name: 'Trace',
			path: '/trace'
		}
	];
</script>

<ul class="list-none flex-row flex py-5 text-xl">
	{#each menus as menu}
		<li class="px-5 rounded-md hover:bg-[#f9bc2d46]">
			<a href={menu.path}>{menu.name}</a>
		</li>
	{/each}
</ul>
<div>
    <slot />
</div>
