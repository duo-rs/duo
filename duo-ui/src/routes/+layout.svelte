<script>
  import '../app.css';
  const menus = [
    {
      name: 'Log',
      path: '/',
    },
    {
      name: 'Trace',
      path: '/trace',
    },
  ];
</script>

<div class="m-5 flex flex-row items-center">
  <h1 class="mx-5 text-2xl font-bold">DUO</h1>
  <ul class="text-md flex list-none flex-row">
    {#each menus as menu}
      <li class="rounded-md px-5 py-1 hover:bg-secondary">
        <a href={menu.path}>{menu.name}</a>
      </li>
    {/each}
  </ul>
</div>
<div>
  <slot></slot>
</div>
