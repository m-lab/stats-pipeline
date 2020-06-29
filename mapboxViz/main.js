
// This loads an american map into the webpage
mapboxgl.accessToken = 'pk.eyJ1IjoibmV3YW1lcmljYSIsImEiOiIyM3ZnYUtrIn0.57fFgg_iM7S1wLH2GQC71g';
var map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/newamerica/cjpn4e4df2gak2rp7nef3am21',
    bounds: new mapboxgl.LngLatBounds(
		new mapboxgl.LngLat(-64.13512973018138, 55.068543614519314),
		new mapboxgl.LngLat(-148.30882882584655, 18.958132980357163)
	)
});

// The function that will be called when the user clicks on the map.  The feature under the mouse is passed in.
var popupFn;

	function mlabPopupFn(props) {
		function rate(key) {
			if (key in props) {
				return `${props[key].toFixed(2)} Mbps`;
			} else {
				return '[No results]';
			}
		}
		const rows = [
			["Dec 2014", "ml_download_Mbps_dec_2014", "ml_upload_Mbps_dec_2014"],
			["Jun 2015", "ml_download_Mbps_jun_2015", "ml_upload_Mbps_jun_2015"],
			["Dec 2015", "ml_download_Mbps_dec_2015", "ml_upload_Mbps_dec_2015"],
			["Jun 2016", "ml_download_Mbps_jun_2016", "ml_upload_Mbps_jun_2016"],
			["Dec 2016", "ml_download_Mbps_dec_2016", "ml_upload_Mbps_dec_2016"],
			["Jun 2017", "ml_download_Mbps_jun_2017", "ml_upload_Mbps_jun_2017"],
			["Dec 2017", "ml_download_Mbps_dec_2017", "ml_upload_Mbps_dec_2017"],
			["Jun 2018", "ml_download_Mbps_jun_2018", "ml_upload_Mbps_jun_2018"],
			["Dec 2018", "ml_download_Mbps_dec_2018", "ml_upload_Mbps_dec_2018"],
		];
		const formatRow = ([time, dl_key, ul_key]) =>
			`<div class="report-date">${time}</div><div class="report-value">${rate(dl_key)}</div>` +
			`<div class="report-value">${rate(ul_key)}</div>`;

		var measureGroup = document.getElementById('measurement');
		var dataset = measureGroup.options[measureGroup.selectedIndex].parentNode.id;

		return ('<h2>' + (props['county_name'] || props['tract_name'] || props['name']) + '</h2>' +
			'<em>Source:' + dataset + '</em>' +
			'<div class="report">' +
			'<div></div><div class="report-header">↓</div><div class="report-header">↑</div>' +
			rows.map(formatRow).join('\n') +
			'</div>'
		);
	}

	function fccPopupFn(props) {
		function rate(key) {
			if (key in props) {
				return `${props[key].toFixed(2)} Mbps`;
			} else {
				return '[No results]';
			}
		}
		const rows = [
			["Dec 2014", "fcc_advertised_down_dec_2014", "fcc_advertised_up_dec_2014"],
			["Jun 2015", "fcc_advertised_down_jun_2015", "fcc_advertised_up_jun_2015"],
			["Dec 2015", "fcc_advertised_down_dec_2015", "fcc_advertised_up_dec_2015"],
			["Jun 2016", "fcc_advertised_down_jun_2016", "fcc_advertised_up_jun_2016"],
			["Dec 2016", "fcc_advertised_down_dec_2016", "fcc_advertised_up_dec_2016"],
			["Jun 2017", "fcc_advertised_down_jun_2017", "fcc_advertised_up_jun_2017"],
		];
		const formatRow = ([time, dl_key, ul_key]) =>
			`<div class="report-date">${time}</div><div class="report-value">${rate(dl_key)}</div>` +
			`<div class="report-value">${rate(ul_key)}</div>`;

		var measureGroup = document.getElementById('measurement');
		var dataset = measureGroup.options[measureGroup.selectedIndex].parentNode.id;

		return ('<h2>' + (props['county_name'] || props['tract_name'] || props['name'] || props['NAME']) + '</h2>' +
			'<em>Source:' + dataset + '</em>' +
			'<div class="report">' +
			'<div></div><div class="report-header">↓</div><div class="report-header">↑</div>' +
			rows.map(formatRow).join('\n') +
			'</div>'
		);
	}

	function diffPopupFn(props){
		function rate(key1,key2) {
			if (key1 in props && key2 in props) {
				let diff = props[key1]-props[key2];
				return `${diff.toFixed(2)} Mbps`;
			} else {
				return '[No results]';
			}
		}
		const rows = [
			["Dec 2014", "fcc_advertised_down_dec_2014", "ml_download_Mbps_dec_2014", "fcc_advertised_up_dec_2014", "ml_upload_Mbps_dec_2014"],
			["Jun 2015", "fcc_advertised_down_jun_2015", "ml_download_Mbps_jun_2015", "fcc_advertised_up_jun_2015", "ml_upload_Mbps_jun_2015"],
			["Dec 2015", "fcc_advertised_down_dec_2015", "ml_download_Mbps_dec_2015", "fcc_advertised_up_dec_2015", "ml_upload_Mbps_dec_2015"],
			["Jun 2016", "fcc_advertised_down_jun_2016", "ml_download_Mbps_jun_2016", "fcc_advertised_up_jun_2016", "ml_upload_Mbps_jun_2016"],
			["Dec 2016", "fcc_advertised_down_dec_2016", "ml_download_Mbps_dec_2016", "fcc_advertised_up_dec_2016", "ml_upload_Mbps_dec_2016"],
			["Jun 2017", "fcc_advertised_down_jun_2017", "ml_download_Mbps_jun_2017", "fcc_advertised_up_jun_2017", "ml_upload_Mbps_jun_2017"]
		];
		const formatRow = ([time, dl_key_fcc, dl_key_mlab, ul_key_fcc, ul_key_mlab]) =>
			`<div class="report-date">${time}</div><div class="report-value">${rate(dl_key_fcc,dl_key_mlab)}</div>` +
			`<div class="report-value">${rate(ul_key_fcc,ul_key_mlab)}</div>`;

		var measureGroup = document.getElementById('measurement');
		var dataset = measureGroup.options[measureGroup.selectedIndex].parentNode.id;

		return ('<h2>' + (props['county_name'] || props['tract_name'] || props['name'] || props['NAME']) + '</h2>' +
			'<em>Source:' + dataset + '</em>' +
			'<div class="report">' +
			'<div></div><div class="report-header">Download Difference</div><div class="report-header">Upload Difference</div>' +
			rows.map(formatRow).join('\n') +
			'</div>'
		);
	}

	function mkRamp({
		displayName, internalName, popupFn, mapboxGlStyleFn, stops, maxTime
	}) {
		const styleFn = mapboxGlStyleFn;
		return {
			displayName, internalName, popupFn, stops, maxTime, mapboxGlStyleFn(timePeriod) {
				return styleFn(timePeriod, stops);
			}
		};
	}

	var config = {
		time_periods: // time periods, as an array of [internalName, displayName]
			[['dec_2014', 'Dec 2014'],
			['jun_2015', 'Jun 2015'],
			['dec_2015', 'Dec 2015'],
			['jun_2016', 'Jun 2016'],
			['dec_2016', 'Dec 2016'],
			['jun_2017', 'Jun 2017'],
			['dec_2017', 'Dec 2017'],
			['jun_2018', 'Jun 2018'],
			['dec_2018', 'Dec 2018']],
		state_centers: // jump-to-state info, an array of [displayName, [lon, lat, zoom]]
			[
				["All States", [38.526600, -96.726486, 3]],
				["Alabama", [32.806671, -86.791130, 6]],
				["Alaska", [61.370716, -152.404419, 3]],
				["Arizona", [33.96, -112.93, 5]],
				["Arkansas", [34.969704, -92.373123, 5]],
				["California", [37.56, -120.70, 5]],
				["Colorado", [38.96, -106.95, 5.5]],
				["Connecticut", [41.597782, -72.755371, 5]],
				["Delaware", [39.318523, -75.507141, 7]],
				["District of Columbia", [38.897438, -77.026817, 10]],
				["Florida", [27.766279, -84.40, 5]],
				["Georgia", [33.040619, -83.643074, 5]],
				["Hawaii", [21.094318, -157.498337, 5]],
				["Idaho", [44.240459, -114.478828, 5]],
				["Illinois", [40.349457, -88.986137, 5]],
				["Indiana", [39.849426, -86.258278, 5]],
				["Iowa", [42.011539, -93.210526, 5]],
				["Kansas", [38.526600, -96.726486, 5]],
				["Kentucky", [37.668140, -84.670067, 5]],
				["Louisiana", [31.169546, -91.867805, 5]],
				["Maine", [45.33, -69.48, 6]],
				["Maryland", [39.063946, -76.802101, 5]],
				["Massachusetts", [42.230171, -71.530106, 5]],
				["Michigan", [43.326618, -87.40, 5.5]],
				["Minnesota", [45.694454, -93.900192, 5]],
				["Mississippi", [32.741646, -89.678696, 6]],
				["Missouri", [38.456085, -92.288368, 5]],
				["Montana", [46.921925, -110.454353, 5]],
				["Nebraska", [41.125370, -98.268082, 5]],
				["Nevada", [38.313515, -117.055374, 5]],
				["New Hampshire", [43.452492, -71.563896, 5]],
				["New Jersey", [40.298904, -74.521011, 5]],
				["New Mexico", [34.840515, -106.248482, 5]],
				["New York", [42.165726, -74.948051, 5]],
				["North Carolina", [35.630066, -79.806419, 5]],
				["North Dakota", [47.528912, -99.784012, 5]],
				["Ohio", [40.388783, -82.764915, 5]],
				["Oklahoma", [35.565342, -96.928917, 5]],
				["Oregon", [44.572021, -122.070938, 5]],
				["Pennsylvania", [40.70, -78.78, 6]],
				["Puerto Rico", [18.155, -66.6963, 7]],
				["Rhode Island", [41.680893, -71.511780, 7]],
				["South Carolina", [33.856892, -80.945007, 5]],
				["South Dakota", [44.299782, -99.438828, 5]],
				["Tennessee", [35.747845, -86.692345, 5]],
				["Texas", [31.054487, -101.33, 5]],
				["Utah", [40.150032, -111.862434, 5]],
				["Vermont", [44.045876, -72.710686, 5]],
				["Virginia", [37.769337, -78.169968, 5]],
				["Washington", [47.400902, -121.490494, 5]],
				["West Virginia", [38.491226, -80.954453, 5]],
				["Wisconsin", [44.268543, -89.616508, 5]],
				["Wyoming", [42.755966, -107.302490], 5]],
		geo_units: // Array of dynamic layer info, [displayName, layerId, layerSrc, internalLayerName]
			[
				['County', 'county', 'mapbox://newamerica.usbb_county', 'usbb_county'],
				['Zip Code', 'zcta', 'mapbox://newamerica.usbb_zcta', 'usbb_zcta'],
			    ['US Census Tracts', 'census_tracts', 'mapbox://newamerica.usbb_tract', 'usbb_tract'],
				['State Senate Districts', 'state_senate', 'mapbox://newamerica.usbb_state_senate', 'usbb_state_senate'],
				['State House Districts', 'state_house', 'mapbox://newamerica.usbb_state_house', 'usbb_state_house'],
			],
		ramps: // Array of display styling info, {displayName, internalName, mapboxGlStyleFn, stops, popupFn}
			[mkRamp({
				displayName: 'MLab Median Download',
				internalName: 'ml_download_Mbps',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_download_Mbps_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `ml_download_Mbps_${timePeriod}`], ...stops],
					    '#808080'];
				},
				maxTime: 'dec_2018',
				stops: [
					-Infinity, '#cde5fa',
					0.2, '#cde5fa',
					4, '#78b2c6',
					10, '#5299a9',
					25, '#307f8a',
					50, '#12666a',
					100, '#004d47'
				]
			}), mkRamp({
				displayName: 'MLab Median Upload',
				internalName: 'ml_upload_Mbps',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_upload_Mbps_${timePeriod}`],
						['interpolate', ['linear'], ['get', `ml_upload_Mbps_${timePeriod}`], ...stops],
						'#808080'];
				},
				maxTime: 'dec_2018',
				stops: [
					-Infinity, '#cde5fa',
					0.2, '#acd2e8',
					1, '#8cbfd4',
					3, '#6facbf',
					5, '#5299a9',
					10, '#388692',
					25, '#20737a',
					50, '#0b6061',
					100, '#004d47']
			}), mkRamp({
				displayName: 'MLab Min RTT',
				internalName: 'mlab_min_rtt',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_min_rtt_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `ml_min_rtt_${timePeriod}`], ...stops],
						'#808080'];
				},
				maxTime: 'dec_2018',
				stops: [0, '#cde5fa',
						5, '#dfa8a9',
						10, '#c78c8c',
						25, '#b07070',
						50, '#995556',
						100, '#803a3c',
						300, '#cde5fa']
			}), mkRamp({
				displayName: 'MLab Download Test Count',
				internalName: 'mlab_dl_count_tests',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_dl_count_tests_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `ml_dl_count_tests_${timePeriod}`], ...stops],
						'#808080'];
				},
				maxTime: 'dec_2018',
				stops: [100, '#cde5fa',
						500, '#99c7dc',
						1000, '#69a8bb',
						5000, '#3d8997',
						10000, '#176b70',
						25000, '#004d47']
			}), mkRamp({
				displayName: 'MLab Upload Test Count',
				internalName: 'mlab_ul_count_tests',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_ul_count_tests_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `ml_ul_count_tests_${timePeriod}`], ...stops],
					    '#808080'];
				},
				maxTime: 'dec_2018',
				stops: [100, '#cde5fa',
						500, '#99c7dc',
						1000, '#69a8bb',
						5000, '#3d8997',
						10000, '#176b70',
						25000, '#004d47']
			}), mkRamp({
				displayName: 'MLab Unique Download IPs',
				internalName: 'mlab_dl_count_ips',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_dl_count_ips_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `ml_dl_count_ips_${timePeriod}`], ...stops],
					    '#808080'];
				},
				maxTime: 'dec_2018',
				stops: [100, '#cde5fa',
						500, '#99c7dc',
						1000, '#69a8bb',
						5000, '#3d8997',
						10000, '#176b70',
						25000, '#004d47']
			}), mkRamp({
				displayName: 'MLab Unique Upload IPs',
				internalName: 'mlab_ul_count_ips',
				popupFn: mlabPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `ml_ul_count_ips_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `ml_ul_count_ips_${timePeriod}`], ...stops],
					    '#808080'];
				},
				maxTime: 'dec_2018',
				stops: [100, '#cde5fa',
						500, '#99c7dc',
						1000, '#69a8bb',
						5000, '#3d8997',
						10000, '#176b70',
						25000, '#004d47']
			}), mkRamp({
				displayName: 'FCC Advertised Download Speed',
				internalName: 'fcc_advertised_dl',
				popupFn: fccPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `fcc_advertised_down_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `fcc_advertised_down_${timePeriod}`], ...stops],
					    '#808080'];
				},
				maxTime: 'dec_2017',
				stops: [
				-Infinity, '#cde5fa',
					0.2, '#cde5fa',
					4, '#78b2c6',
					10, '#5299a9',
					25, '#307f8a',
					50, '#12666a',
					100, '#004d47']
			}), mkRamp({
				displayName: 'FCC Advertised Upload Speed',
				internalName: 'fcc_advertised_ul',
				popupFn: fccPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['has', `fcc_advertised_up_${timePeriod}`],
					    ['interpolate', ['linear'], ['get', `fcc_advertised_up_${timePeriod}`], ...stops],
					    '#808080'];
				},
				maxTime: 'dec_2017',
				stops: [
					-Infinity, '#cde5fa',
					0.2, '#cde5fa',
					4, '#78b2c6',
					10, '#5299a9',
					25, '#307f8a',
					50, '#12666a',
					100, '#004d47']
			}), mkRamp({
				displayName: 'Download Comparison (FCC - MLab)',
				internalName: 'mlab_fcc_dl_comp',
				popupFn: diffPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['all', ['has', `fcc_advertised_down_${timePeriod}`], ['has', `ml_download_Mbps_${timePeriod}`]],
					    ['interpolate',['linear'],
						['-', ['get', `fcc_advertised_down_${timePeriod}`], ['get', `ml_download_Mbps_${timePeriod}`]],
						...stops],'#808080'];
				},
				maxTime: 'dec_2017',
				stops: [
					-Infinity, '#690408',
					-25, '#96352d',
					-15, '#be6256',
					-10, '#e09287',
					-5, '#f7c7bf',
					-1, '#ffffff',
					1, '#d2cdf2',
					5, '#a39fdc',
					10, '#7374be',
					15, '#434d9b',
					25, '#002a74'
				]
			}), mkRamp({
				displayName: 'Upload Comparison (FCC - MLab)',
				internalName: 'mlab_fcc_ul_comp',
				popupFn: diffPopupFn,
				mapboxGlStyleFn(timePeriod, stops) {
					return ['case', 
					    ['all', ['has', `fcc_advertised_down_${timePeriod}`], ['has', `ml_download_Mbps_${timePeriod}`]],
					    ['interpolate',['linear'],
						['-', ['get', `fcc_advertised_up_${timePeriod}`], ['get', `ml_upload_Mbps_${timePeriod}`]], ...stops], '#808080'];
				},
				maxTime: 'dec_2017',
				stops: [
					-Infinity, '#690408',
					-25, '#96352d',
					-15, '#be6256',
					-10, '#e09287',
					-5, '#f7c7bf',
					-1, '#ffffff',
					1, '#d2cdf2',
					5, '#a39fdc',
					10, '#7374be',
					15, '#434d9b',
					25, '#002a74'
				]
			})
			]
	};

	// Encode the map state into the fragment for linkability.
	function updateFragment() {
		var measureGroup = document.getElementById('measurement');
		const geo = document.getElementById("geo").value;
		const timeperiod = config.time_periods[document.getElementById("slider").value][0];
		const measure = document.getElementById("measurement").value;
		const { lng: x, lat: y } = map.getCenter();
		const z = map.getZoom();
		const url = new URL(window.location);
		url.hash = `#${geo}/${timeperiod}/${measure}/${x.toFixed(2)}/${y.toFixed(2)}/${z.toFixed(2)}`;
		window.history.replaceState(undefined, 'USBB', url.toString());
	}


function configureMap() {
	const measure = document.getElementById('measurement').value;
	const geo = document.getElementById('geo').value;
	const timeSlider = document.getElementById('slider');

	const ramp = config.ramps.find((ramp) => ramp.internalName == measure);

  // Apply time constraint and clamp value.
  const maxTimeIndex = config.time_periods.findIndex((period) => period[0] == ramp.maxTime);
	console.log(ramp.maxTime, maxTimeIndex, config.time_periods.length);
  timeSlider.max = maxTimeIndex == -1 ? config.time_periods.length - 1 : maxTimeIndex;
	timeSlider.value = Math.min(timeSlider.max, timeSlider.value);

	const timePeriod = config.time_periods[timeSlider.value];

	const style = ramp.mapboxGlStyleFn(timePeriod[0]);

    // Apply layer visibility and style.
	for (const layer of config.geo_units) {
		map.setLayoutProperty(layer[1], 'visibility', layer[1] == geo ? 'visible' : 'none');
		map.setPaintProperty(layer[1], 'fill-color', style);
	}
	// Update popup template.
	popupFn = ramp.popupFn;
	// Update legend.
	const breaks = ramp.stops;
	const cssRamp = breaks.filter((_, idx) => idx % 2).join(',');
	document.getElementById('legend').style.background =
	  `linear-gradient(to right, ${cssRamp})`;
	const cssLabels = breaks.filter((_, idx) => !(idx % 2)).filter(b => Number.isFinite(b));
	document.getElementById('legendlabel').innerHTML =
		cssLabels.map((l) => `<div class="label">${l}</div>`).join('');
	
	// Update time period label.
	document.getElementById('time_period').innerText = timePeriod[1];

	// Update fragment.
	updateFragment();
}

    const measureSelect = document.getElementById('measurement');
	for (const {displayName,internalName} of config.ramps) {
		const opt = document.createElement('option')
		opt.value = internalName;
		opt.innerText = displayName;
		measureSelect.appendChild(opt);
	}
	const geoSelect = document.getElementById('geo');
	for (const [displayName, layerId, src, srcLyr] of config.geo_units) {
		const opt = document.createElement('option');
		opt.value = layerId;
		opt.innerText = displayName;
		geoSelect.appendChild(opt);
	}
	const timeSlider = document.getElementById('slider');
	timeSlider.min = 0;
	timeSlider.max = config.time_periods.length - 1;
	timeSlider.step = 1;
	const stateSelector = document.getElementById('state_select');
	for (const [displayName, mapCoordinates] of config.state_centers) {
		const opt = document.createElement('option');
		opt.value = displayName;
		opt.innerText = displayName;
		stateSelector.appendChild(opt);
	}
	[measureSelect, geoSelect, timeSlider].forEach((el) => el.addEventListener('input', () => {
		configureMap();
	}))
	stateSelector.addEventListener('input', (el) => {
		const state = config.state_centers.find((row) => row[0] == stateSelector.value);
		const state_geo = map.querySourceFeatures('us_states', {
			source_layer: 'us_states',
			filter: ['in', 'STATE', stateSelector.value]});
		console.log(state_geo);
		if (state) {
			const [ lat, lon, zoom ] = state[1];
			map.flyTo({
				center: { lon, lat },
				zoom
			});
		}
	});
	map.on('load', function () {
		for (const [_, id, url, sourceLayer] of config.geo_units) {
			map.addLayer({
				id,
				type: "fill",
				source: { url, type: "vector" },
				"source-layer": sourceLayer,
				layout: { visibility: 'none' },
				paint: {
					'fill-opacity': 0.7
				}
			})

			// Change the cursor to a pointer when the mouse is over the states layer.
			map.on('mouseenter', id, () => {
				map.getCanvas().style.cursor = 'pointer';
			});

			// Change it back to a pointer when it leaves.
			map.on('mouseleave', id, () => {
				map.getCanvas().style.cursor = '';
			});
		}

		map.addLayer({
			"id": "us_states",
			"type": "line",
			"source": {
				type: 'vector',
				url: 'mapbox://newamerica.cjvopv39u05jt2wmna9mg5wz4-7j8om'
			},
			"layout": {
				'visibility': 'visible'
			},
			"source-layer": "us_states",
			"paint": {
				"line-color": "#000",
				"line-width": 0.5
			}
		});

		var popup = new mapboxgl.Popup({className: "popup-box"});

        const layers = config.geo_units.map((g) => g[1]);
		map.on('click', (e) => {
			let f = map.queryRenderedFeatures(e.point, {layers});
			if (f.length) {
				console.log(f[0]);
				popup.setLngLat(e.lngLat)
					.setHTML(popupFn(f[0].properties))
					.addTo(map);
			}
		});



		// Read settings from URL, if any.
		// :geo/:timeperiod/:measure/:x/:y/:z
		let fragment = window.location.hash;
		if (fragment) {
			const check = (v, test) => test(v) ? v : undefined;

			const parts = fragment.slice(1).split("/");
			const geo = check(parts[0], (v) => config.geo_units.some((u) => u[1] == v));
			const timeperiod = check(parts[1], (v) => config.time_periods.some((x) => x[0] == v));
			const measure = check(parts[2], (v) => config.ramps.some((r) => r.internalName == v));
			const x = check(Number(parts[3]), (v) => !Number.isNaN(v));
			const y = check(Number(parts[4]), (v) => !Number.isNaN(v));
			const z = check(Number(parts[5]), (v) => !Number.isNaN(v));

			if (geo) {
				const el = document.querySelector(`#geo [value='${geo}']`) || document.querySelector(`#geo :first-child`);
				el.selected = true;
			}
			if (timeperiod !== undefined) {
				const idx = config.time_periods.findIndex((x) => x[0] == timeperiod);
				if (idx >= 0) {
					document.getElementById('slider').value = idx
				};
			}
			if (measure) {
				const el = document.querySelector(`#measurement [value='${measure}']`) || document.querySelector(`#measurement :first-child`);
				el.selected = true;
			}
			if ([x, y, z].every(n => n !== undefined)) {
				map.jumpTo({ center: [x, y], zoom: z });
			}
		}
		// Run the color ramp to start
		configureMap();

		map.on('moveend', updateFragment);
	});