<script>
	/**
	 * @type {string}
	 */
	export let type = '';
	/**
	 * @type {string}
	 */
	let typeName;
	/**
	 * @type {string}
	 */
	let color;

	$: {
		typeName = convertTypeName();
		switch (typeName) {
			case 'number':
				color = '#975f0f';
				break;
			case 'str':
				color = '#079ea3';
				break;
			case 'bool':
				color = '#7e0571';
				break;
			default:
				color = 'gray';
				break;
		}
	}

	function convertTypeName() {
		switch (type.toLowerCase()) {
			case 'utf8':
				return 'str';
			case 'int8':
			case 'int16':
			case 'int32':
			case 'int64':
			case 'uint8':
			case 'uint16':
			case 'uint32':
			case 'uint64':
				return 'num';
			case 'boolean':
				return 'bool';
			default:
				return '';
		}
	}
</script>

{#if typeName}
	<div class="rounded border text-xs px-1" style:color style:border-color={color}>
		{typeName}
	</div>
{/if}
