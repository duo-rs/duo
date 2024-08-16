<script>
	const src = process.env.NODE_ENV === 'production' ? '/trace' : 'http://localhost:3000';
</script>

<iframe
	{src}
	frameborder="0"
	title="trace view"
	allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
></iframe>

<style>
	iframe {
		width: 100%;
		height: 100vh;
	}
</style>
