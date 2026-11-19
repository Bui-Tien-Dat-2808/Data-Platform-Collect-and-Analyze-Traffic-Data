// Urban Traffic Intelligence Frontend JavaScript
// Supports Multi-City switching, Google Maps-style traffic ribbons, and Auto-Refresh.

let map = null;
let markerLayer = L.layerGroup();
let currentCity = 'HAN';
let refreshCountdown = 60;
let chartInstance = null;
let allIncidents = [];
let listFeatures = [];
let focusedId = null;
const activeChips = { jam: true, closed: true, works: true };

// City coordinate centers
const CITIES = {
  HAN: { name: 'Hà Nội', center: [21.0285, 105.8542], zoom: 12 },
  SGN: { name: 'TP. Hồ Chí Minh', center: [10.7769, 106.7009], zoom: 12 },
  DAD: { name: 'Đà Nẵng', center: [16.0544, 108.2022], zoom: 13 }
};

document.addEventListener('DOMContentLoaded', () => {
  initMap();
  loadDashboardData(currentCity);
  setupEventListeners();
  startAutoRefreshTimer();
});

function initMap() {
  const defaultCity = CITIES[currentCity];
  map = L.map('map', {
    zoomControl: true,
    attributionControl: false
  }).setView(defaultCity.center, defaultCity.zoom);

  // OpenStreetMap Standard Tiles — Hoàn toàn miễn phí 100%, không cần bất kỳ API Key nào
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    subdomains: ['a', 'b', 'c']
  }).addTo(map);

  markerLayer.addTo(map);
}

function setupEventListeners() {
  const citySelect = document.getElementById('citySelect');
  citySelect.addEventListener('change', (e) => {
    currentCity = e.target.value;
    focusedId = null;
    const cityInfo = CITIES[currentCity];
    if (cityInfo) {
      map.setView(cityInfo.center, cityInfo.zoom, { animate: false });
    }
    applyFocus();
    loadDashboardData(currentCity);
    refreshCountdown = 60;
  });

  document.getElementById('btnShowAll').addEventListener('click', () => {
    focusedId = null;
    map.closePopup();
    applyFocus();
    renderIncidentList();
  });

  document.getElementById('btnRefresh').addEventListener('click', () => {
    loadDashboardData(currentCity);
    refreshCountdown = 60;
  });

  document.getElementById('searchInput').addEventListener('input', renderIncidentList);

  const chipBindings = { chipJam: 'jam', chipClosed: 'closed', chipWorks: 'works' };
  Object.entries(chipBindings).forEach(([id, key]) => {
    document.getElementById(id).addEventListener('click', (e) => {
      activeChips[key] = !activeChips[key];
      e.currentTarget.classList.toggle('opacity-40', !activeChips[key]);
      renderIncidentList();
    });
  });
}

function startAutoRefreshTimer() {
  setInterval(() => {
    refreshCountdown--;
    if (refreshCountdown <= 0) {
      loadDashboardData(currentCity);
      refreshCountdown = 60;
    }
    document.getElementById('refreshTimer').textContent = `Tự làm mới: ${refreshCountdown}s`;
  }, 1000);
}

async function loadDashboardData(cityId) {
  try {
    const [kpiRes, incidentsRes, distRes] = await Promise.all([
      fetch(`/api/kpi?city_id=${cityId}`).then(r => r.json()),
      fetch(`/api/incidents?city_id=${cityId}`).then(r => r.json()),
      fetch(`/api/distribution?city_id=${cityId}`).then(r => r.json())
    ]);

    if (kpiRes.status === 'success') updateKPIs(kpiRes.data);
    if (incidentsRes.status === 'success') renderIncidents(incidentsRes.data);
    if (distRes.status === 'success') updateChart(distRes.data);
  } catch (error) {
    console.error('Lỗi khi tải dữ liệu dashboard:', error);
  }
}

function updateKPIs(kpi) {
  document.getElementById('kpiIncidents').textContent = kpi.total_incidents.toLocaleString();
  document.getElementById('kpiLength').textContent = kpi.total_congestion_length_km;
  document.getElementById('kpiDelay').textContent = kpi.avg_delay_minutes;
  document.getElementById('kpiTime').textContent = kpi.last_updated_time || '--';
  document.getElementById('kpiHotspot').textContent = kpi.top_congested_corridor || 'Không có điểm tắc lớn';
}

function severityBadge(item) {
  if (item.incident_type === 1) return { text: 'Tai nạn', cls: 'bg-purple-100 text-purple-700' };
  if ([7, 8, 9].includes(item.incident_type)) return { text: 'Đóng đường', cls: 'bg-slate-200 text-slate-700' };
  const delay = item.delay_minutes || 0;
  if (delay >= 5) return { text: 'Tắc nặng', cls: 'bg-rose-100 text-rose-700' };
  if (delay >= 2) return { text: 'Ùn ứ', cls: 'bg-amber-100 text-amber-700' };
  return { text: 'Chậm nhẹ', cls: 'bg-emerald-100 text-emerald-700' };
}

function renderIncidents(incidents) {
  allIncidents = incidents;
  const popupWasOpen = !!document.querySelector('.leaflet-popup');
  markerLayer.clearLayers();

  listFeatures = [];

  incidents.forEach((item) => {
    if (!item.latitude || !item.longitude) return;

    let coordsList = [];
    if (item.geometry_coordinates) {
      try {
        const rawCoords = typeof item.geometry_coordinates === 'string'
          ? JSON.parse(item.geometry_coordinates)
          : item.geometry_coordinates;
        if (Array.isArray(rawCoords) && rawCoords.length > 0) {
          // TomTom format: [lon, lat] -> Leaflet format: [lat, lon]
          coordsList = rawCoords.map(pt => [pt[1], pt[0]]);
        }
      } catch (e) {
        coordsList = [];
      }
    }

    const badge = severityBadge(item);
    const popupContent = `
      <div class="space-y-1.5 text-xs min-w-[220px]">
        <div class="flex items-center justify-between space-x-3">
          <span class="font-bold text-sm" style="color: ${item.color_hex}">${item.type_label}</span>
          <span class="text-[10px] font-bold px-2 py-0.5 rounded-full ${badge.cls}">${badge.text}</span>
        </div>
        <div class="text-slate-600"><strong>Tuyến đường:</strong> <span class="text-blue-600 font-semibold">${item.road_from || 'Không rõ'}</span> &rarr; ${item.road_to || 'Không rõ'}</div>
        <div class="bg-slate-100 rounded-lg p-2 grid grid-cols-2 gap-2">
          <div>
            <div class="text-slate-500">Chiều dài kẹt:</div>
            <div class="text-slate-800 font-semibold">${(item.length_km || 0).toFixed(2)} km</div>
          </div>
          <div>
            <div class="text-slate-500">Thời gian trễ:</div>
            <div class="text-rose-600 font-bold">+${Math.round(item.delay_minutes || 0)} phút</div>
          </div>
        </div>
        <div class="text-slate-500 text-[10px] italic">${item.description || ''}</div>
      </div>
    `;

    let mapFeature = null;
    const layers = [];

    if (coordsList.length > 1) {
      // 1. DẢI ĐƯỜNG KẸT XE PHONG CÁCH GOOGLE MAPS (Polyline có viền tương phản)
      const roadCasing = L.polyline(coordsList, {
        color: '#0f172a',
        weight: 7.5,
        opacity: 0.5,
        lineCap: 'round',
        lineJoin: 'round',
        interactive: false
      });
      layers.push(roadCasing);

      const roadLine = L.polyline(coordsList, {
        color: item.color_hex,
        weight: 5,
        opacity: 0.95,
        lineCap: 'round',
        lineJoin: 'round'
      });
      roadLine.bindPopup(popupContent);

      roadLine.on('mouseover', function () {
        this.setStyle({ weight: 7.5, opacity: 1.0 });
        this.bringToFront();
      });
      roadLine.on('mouseout', function () {
        this.setStyle({ weight: 5, opacity: 0.95 });
      });

      layers.push(roadLine);
      mapFeature = roadLine;

      if ([7, 8, 9].includes(item.incident_type)) {
        const midIdx = Math.floor(coordsList.length / 2);
        const closureIcon = L.divIcon({
          html: '<span class="text-xs shadow-md bg-rose-600 rounded-full px-1 py-0.5 border border-white text-white font-bold">⛔</span>',
          className: 'custom-traffic-icon',
          iconSize: [20, 20],
          iconAnchor: [10, 10]
        });
        const closureMarker = L.marker(coordsList[midIdx], { icon: closureIcon }).bindPopup(popupContent);
        layers.push(closureMarker);
      } else if (item.incident_type === 1) {
        const accidentIcon = L.divIcon({
          html: '<span class="text-xs shadow-md bg-purple-600 rounded-full px-1 py-0.5 border border-white text-white font-bold">⚠️</span>',
          className: 'custom-traffic-icon',
          iconSize: [20, 20],
          iconAnchor: [10, 10]
        });
        const accidentMarker = L.marker(coordsList[0], { icon: accidentIcon }).bindPopup(popupContent);
        layers.push(accidentMarker);
      }
    } else {
      // 2. Điểm tắc đơn lẻ (Point)
      const pointMarker = L.circleMarker([item.latitude, item.longitude], {
        radius: 5,
        fillColor: item.color_hex,
        color: '#FFFFFF',
        weight: 1.5,
        opacity: 0.9,
        fillOpacity: 0.85
      });
      pointMarker.bindPopup(popupContent);
      layers.push(pointMarker);
      mapFeature = pointMarker;
    }

    listFeatures.push({ item, coordsList, mapFeature, layers });
  });

  if (focusedId !== null && !listFeatures.some((f) => f.item.incident_id === focusedId)) {
    focusedId = null;
  }
  applyFocus();
  if (focusedId !== null && popupWasOpen) {
    const focused = listFeatures.find((f) => f.item.incident_id === focusedId);
    if (focused) focused.mapFeature.openPopup();
  }
  renderIncidentList();
}

function applyFocus() {
  markerLayer.clearLayers();
  listFeatures.forEach((f) => {
    if (focusedId === null || f.item.incident_id === focusedId) {
      f.layers.forEach((layer) => markerLayer.addLayer(layer));
    }
  });
  document.getElementById('btnShowAll').classList.toggle('hidden', focusedId === null);
}

function chipAllows(item) {
  const label = (item.type_label || '').toLowerCase();
  if (label.includes('jam')) return activeChips.jam;
  if (label.includes('closed')) return activeChips.closed;
  if (label.includes('works')) return activeChips.works;
  return true;
}

function renderIncidentList() {
  const query = (document.getElementById('searchInput').value || '').trim().toLowerCase();
  const listContainer = document.getElementById('incidentList');
  listContainer.innerHTML = '';

  const filtered = listFeatures.filter(({ item }) => {
    if (!chipAllows(item)) return false;
    if (!query) return true;
    const haystack = `${item.road_from || ''} ${item.road_to || ''}`.toLowerCase();
    return haystack.includes(query);
  });

  filtered.slice(0, 10).forEach(({ item, coordsList, mapFeature }, index) => {
    const itemEl = document.createElement('div');
    const isFocused = focusedId === item.incident_id;
    itemEl.className = isFocused
      ? 'p-2 rounded-lg bg-blue-50/60 border border-blue-400 ring-1 ring-blue-300 flex items-center justify-between cursor-pointer transition shadow-sm'
      : 'p-2 rounded-lg bg-white border border-slate-200 flex items-center justify-between cursor-pointer hover:border-blue-300 hover:bg-blue-50/40 transition shadow-sm';
    itemEl.innerHTML = `
      <div class="flex items-center space-x-2 truncate mr-2">
        <span class="w-6 h-6 shrink-0 rounded-md bg-slate-100 text-slate-600 text-[11px] font-bold flex items-center justify-center">${index + 1}</span>
        <div class="truncate">
          <div class="font-semibold text-slate-800 truncate">${item.road_from || 'Điểm tắc'}</div>
          <div class="text-[10px] text-slate-500">${item.type_label} • ${(item.length_km || 0).toFixed(1)} km</div>
        </div>
      </div>
      <span class="text-xs font-bold px-1.5 py-0.5 rounded shrink-0" style="background-color: ${item.color_hex}33; color: ${item.color_hex}">
        +${Math.round(item.delay_minutes || 0)}p
      </span>
    `;
    itemEl.addEventListener('click', () => {
      if (focusedId === item.incident_id) {
        focusedId = null;
        map.closePopup();
        applyFocus();
        renderIncidentList();
        return;
      }
      focusedId = item.incident_id;
      applyFocus();
      renderIncidentList();
      // Hủy animation đang chạy (flyTo đổi thành phố, lần click trước) để không đè lên điều hướng mới
      map.stop();
      if (coordsList.length > 1) {
        map.fitBounds(mapFeature.getBounds(), { maxZoom: 16, padding: [30, 30], animate: false });
      } else {
        map.setView([item.latitude, item.longitude], 15, { animate: false });
      }
      mapFeature.openPopup();
    });
    listContainer.appendChild(itemEl);
  });
}

function updateChart(distribution) {
  const ctx = document.getElementById('incidentChart').getContext('2d');
  const labels = distribution.map(d => d.type_label);
  const data = distribution.map(d => d.incident_count);
  const colors = ['#EA2027', '#FFC312', '#8B5CF6', '#30336B', '#10AC84', '#686DE0'];

  if (chartInstance) {
    chartInstance.destroy();
  }

  chartInstance = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels,
      datasets: [{
        data: data,
        backgroundColor: colors.slice(0, labels.length),
        borderWidth: 2,
        borderColor: '#FFFFFF'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '62%',
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#FFFFFF',
          titleColor: '#1E293B',
          bodyColor: '#475569',
          borderColor: '#E2E8F0',
          borderWidth: 1
        }
      }
    }
  });
}