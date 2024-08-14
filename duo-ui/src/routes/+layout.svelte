<script>
	import '../app.css';
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

<div class="m-5 flex flex-row items-center">
	<h1 class="mx-5 text-2xl font-bold">DUO</h1>
	<ul class="flex list-none flex-row text-md">
		{#each menus as menu}
			<li class="hover:bg-secondary rounded-md px-5 py-1">
				<a href={menu.path}>{menu.name}</a>
			</li>
		{/each}
	</ul>
</div>
<div>
	<slot></slot>
</div>
