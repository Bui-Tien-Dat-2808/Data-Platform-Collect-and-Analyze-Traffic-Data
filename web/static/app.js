// Urban Traffic Intelligence Frontend JavaScript
// Supports Multi-City switching, Google Maps-style color coding, Leaflet Heatmap, and Auto-Refresh.

let map = null;
let markerLayer = L.layerGroup();
let heatLayer = null;
let isHeatmapActive = true;
let currentCity = 'HAN';
let refreshCountdown = 60;
let chartInstance = null;

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

  // CartoDB Dark Matter tile layer for slick dark UI
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19,
    subdomains: 'abcd',
  }).addTo(map);

  markerLayer.addTo(map);
}

function setupEventListeners() {
  const citySelect = document.getElementById('citySelect');
  citySelect.addEventListener('change', (e) => {
    currentCity = e.target.value;
    const cityInfo = CITIES[currentCity];
    if (cityInfo) {
      map.flyTo(cityInfo.center, cityInfo.zoom, { duration: 1.5 });
    }
    loadDashboardData(currentCity);
    refreshCountdown = 60;
  });

  document.getElementById('btnRefresh').addEventListener('click', () => {
    loadDashboardData(currentCity);
    refreshCountdown = 60;
  });

  document.getElementById('toggleHeatmap').addEventListener('click', () => {
    isHeatmapActive = !isHeatmapActive;
    document.getElementById('heatStatus').textContent = isHeatmapActive ? 'BẬT' : 'TẮT';
    document.getElementById('heatStatus').className = isHeatmapActive ? 'text-emerald-400 font-bold' : 'text-slate-400 font-bold';
    if (heatLayer) {
      if (isHeatmapActive) {
        map.addLayer(heatLayer);
      } else {
        map.removeLayer(heatLayer);
      }
    }
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
  document.getElementById('kpiLength').textContent = `${kpi.total_congestion_length_km} km`;
  document.getElementById('kpiDelay').textContent = `${kpi.avg_delay_minutes} phút`;
  document.getElementById('kpiTime').textContent = kpi.last_updated_time || '--';
  document.getElementById('kpiHotspot').textContent = kpi.top_congested_corridor || 'Không có điểm tắc lớn';
}

function renderIncidents(incidents) {
  markerLayer.clearLayers();
  if (heatLayer) {
    map.removeLayer(heatLayer);
  }

  const heatPoints = [];
  const listContainer = document.getElementById('incidentList');
  listContainer.innerHTML = '';

  incidents.forEach((item, index) => {
    if (!item.latitude || !item.longitude) return;

    // Collect coordinates for Heatmap
    const intensity = Math.min((item.delay_minutes || 1) / 10, 1.0);
    heatPoints.push([item.latitude, item.longitude, intensity]);

    // Google Maps Style Circle Marker
    const marker = L.circleMarker([item.latitude, item.longitude], {
      radius: Math.min(Math.max((item.delay_minutes || 2) * 1.5, 6), 18),
      fillColor: item.color_hex,
      color: '#FFFFFF',
      weight: 1.5,
      opacity: 0.9,
      fillOpacity: 0.85
    });

    // Custom Interactive Popup
    const popupContent = `
      <div class="space-y-1 text-xs">
        <div class="font-bold text-sm" style="color: ${item.color_hex}">${item.type_label}</div>
        <div class="text-slate-300"><strong>Đoạn:</strong> ${item.road_from || 'Không rõ'} &rarr; ${item.road_to || 'Không rõ'}</div>
        <div class="text-slate-300"><strong>Chiều dài kẹt xe:</strong> ${(item.length_km || 0).toFixed(2)} km</div>
        <div class="text-slate-300"><strong>Thời gian trễ:</strong> <span class="font-bold text-amber-400">${item.delay_minutes || 0} phút</span></div>
        <div class="text-slate-400 text-[10px] mt-1">${item.description || ''}</div>
      </div>
    `;
    marker.bindPopup(popupContent);
    markerLayer.addLayer(marker);

    // Populate Top 10 list on sidebar
    if (index < 10) {
      const itemEl = document.createElement('div');
      itemEl.className = 'p-2 rounded bg-slate-800/60 border border-slate-700/60 flex items-center justify-between cursor-pointer hover:bg-slate-700/60 transition';
      itemEl.innerHTML = `
        <div class="truncate mr-2">
          <div class="font-semibold text-slate-200 truncate">${item.road_from || 'Điểm tắc'}</div>
          <div class="text-[10px] text-slate-400">${item.type_label} • ${(item.length_km || 0).toFixed(1)} km</div>
        </div>
        <span class="text-xs font-bold px-1.5 py-0.5 rounded" style="background-color: ${item.color_hex}33; color: ${item.color_hex}">
          +${Math.round(item.delay_minutes || 0)}p
        </span>
      `;
      itemEl.addEventListener('click', () => {
        map.flyTo([item.latitude, item.longitude], 15);
        marker.openPopup();
      });
      listContainer.appendChild(itemEl);
    }
  });

  // Create Heatmap Layer
  if (heatPoints.length > 0) {
    heatLayer = L.heatLayer(heatPoints, {
      radius: 25,
      blur: 15,
      maxZoom: 17,
      gradient: { 0.2: '#10AC84', 0.5: '#FFC312', 0.8: '#EA2027', 1.0: '#880000' }
    });
    if (isHeatmapActive) {
      heatLayer.addTo(map);
    }
  }
}

function updateChart(distribution) {
  const ctx = document.getElementById('incidentChart').getContext('2d');
  const labels = distribution.map(d => d.type_label);
  const data = distribution.map(d => d.incident_count);
  const colors = ['#EA2027', '#FFC312', '#E056FD', '#30336B', '#10AC84', '#686DE0'];

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
        borderWidth: 1,
        borderColor: '#1E293B'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: { color: '#CBD5E1', boxWidth: 10, font: { size: 10 } }
        }
      }
    }
  });
}

