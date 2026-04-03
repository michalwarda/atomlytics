(function () {
  "use strict";

  const METRICS = [
    { key: "UniqueVisitors", label: "Unique visitors", showInRealtime: false },
    { key: "Visits", label: "Total visits", showInRealtime: true },
    { key: "Pageviews", label: "Total pageviews", showInRealtime: true },
    { key: "BounceRate", label: "Bounce rate", showInRealtime: false },
    { key: "AvgVisitDuration", label: "Visit duration", showInRealtime: false },
  ];

  const FILTER_FIELDS = [
    { id: "filter_page", label: "Path", dataKey: "page_url_path" },
    { id: "filter_country", label: "Country", dataKey: "country" },
    { id: "filter_device", label: "Device", dataKey: "device_type" },
    { id: "filter_os", label: "Operating system", dataKey: "operating_system" },
    { id: "filter_browser", label: "Browser", dataKey: "browser" },
    { id: "filter_referrer", label: "Referrer", dataKey: "referrer" },
    { id: "filter_source", label: "Source", dataKey: "source" },
    { id: "filter_campaign", label: "UTM tags", dataKey: "utm_campaign" },
    { id: "filter_region", label: "Region", dataKey: "region" },
    { id: "filter_city", label: "City", dataKey: "city" },
  ];

  const FILTER_FIELD_MAP = Object.fromEntries(FILTER_FIELDS.map((field) => [field.id, field]));

  const GROUPS = {
    location: {
      defaultValue: "Country",
      labelId: "location-column-label",
      tone: "warm",
      valueToLabel: {
        Country: "Country",
        Region: "Region",
        City: "City",
      },
      valueToField: {
        Country: "country",
        Region: "region",
        City: "city",
      },
      valueToFilter: {
        Country: "filter_country",
        Region: "filter_region",
        City: "filter_city",
      },
      tableId: "location-table",
    },
    device: {
      defaultValue: "Browser",
      labelId: "device-column-label",
      tone: "cool",
      valueToLabel: {
        Browser: "Browser",
        OperatingSystem: "Operating system",
        DeviceType: "Device",
      },
      valueToField: {
        Browser: "browser",
        OperatingSystem: "operating_system",
        DeviceType: "device_type",
      },
      valueToFilter: {
        Browser: "filter_browser",
        OperatingSystem: "filter_os",
        DeviceType: "filter_device",
      },
      tableId: "device-table",
    },
    source: {
      defaultValue: "Source",
      labelId: "source-column-label",
      tone: "warm",
      valueToLabel: {
        Source: "Source",
        Referrer: "Referrer",
        Campaign: "Campaign",
      },
      valueToField: {
        Source: "source",
        Referrer: "referrer",
        Campaign: "source",
      },
      valueToFilter: {
        Source: "filter_source",
        Referrer: "filter_referrer",
        Campaign: "filter_campaign",
      },
      tableId: "source-table",
    },
    page: {
      defaultValue: "Page",
      labelId: "page-column-label",
      tone: "warm",
      valueToLabel: {
        Page: "Page",
        EntryPage: "Entry page",
        ExitPage: "Exit page",
      },
      valueToField: {
        Page: "page_path",
        EntryPage: "entry_page_path",
        ExitPage: "exit_page_path",
      },
      valueToFilter: {
        Page: "filter_page",
        EntryPage: "filter_page",
        ExitPage: "filter_page",
      },
      tableId: "page-table",
    },
  };

  let filterValuesCache = null;
  let currentFilterField = null;
  let currentFilterSearch = "";

  function getBasePath() {
    const parts = window.location.pathname.split("/").filter(Boolean);
    return `/${parts[0]}`;
  }

  function currentUrlParams() {
    return new URLSearchParams(window.location.search);
  }

  function createEmptyFilterState() {
    return FILTER_FIELDS.reduce((state, field) => {
      state[field.id] = { include: [], exclude: [] };
      return state;
    }, {});
  }

  function filterStateFromUrl() {
    const params = currentUrlParams();
    const state = createEmptyFilterState();

    FILTER_FIELDS.forEach((field) => {
      state[field.id] = {
        include: params.getAll(field.id).filter(Boolean),
        exclude: params.getAll(`${field.id}_not`).filter(Boolean),
      };
    });

    return state;
  }

  function writeFilterStateToParams(params, state) {
    FILTER_FIELDS.forEach((field) => {
      params.delete(field.id);
      params.delete(`${field.id}_not`);

      state[field.id].include.forEach((value) => params.append(field.id, value));
      state[field.id].exclude.forEach((value) => params.append(`${field.id}_not`, value));
    });
  }

  function getQueryParams() {
    const params = currentUrlParams();
    return {
      metric: params.get("metric") || "UniqueVisitors",
      timeframe: params.get("timeframe") || "Today",
      granularity: params.get("granularity") || "Hours",
      locationGrouping: params.get("locationGrouping") || "Country",
      deviceGrouping: params.get("deviceGrouping") || "Browser",
      sourceGrouping: params.get("sourceGrouping") || "Source",
      pageGrouping: params.get("pageGrouping") || "Page",
    };
  }

  function buildDashboardRequest(filterState) {
    const params = getQueryParams();
    const request = new URLSearchParams({
      timeframe: params.timeframe,
      granularity: params.granularity,
      metric: params.metric,
      location_grouping: getActiveGroupValue("location"),
      device_grouping: getActiveGroupValue("device"),
      source_grouping: getActiveGroupValue("source"),
      page_grouping: getActiveGroupValue("page"),
    });
    writeFilterStateToParams(request, filterState || filterStateFromUrl());
    return request;
  }

  function getActiveMetric() {
    return document.querySelector(".metric-card.is-active")?.dataset.metric || "UniqueVisitors";
  }

  function getActiveGroupValue(group) {
    return document.querySelector(`.panel-tab[data-group="${group}"].is-active`)?.dataset.value || GROUPS[group].defaultValue;
  }

  function setActiveGroupValue(group, value) {
    document.querySelectorAll(`.panel-tab[data-group="${group}"]`).forEach((button) => {
      const isActive = button.dataset.value === value;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }

  function updateUrl(nextMetric) {
    const params = currentUrlParams();
    params.set("metric", nextMetric || getActiveMetric());
    params.set("timeframe", document.getElementById("timeframe").value);
    params.set("granularity", document.getElementById("granularity").value);
    params.set("locationGrouping", getActiveGroupValue("location"));
    params.set("deviceGrouping", getActiveGroupValue("device"));
    params.set("sourceGrouping", getActiveGroupValue("source"));
    params.set("pageGrouping", getActiveGroupValue("page"));
    window.history.pushState({}, "", `${window.location.pathname}?${params.toString()}`);
  }

  function updateGranularity(metricOverride) {
    const timeframe = document.getElementById("timeframe").value;
    const select = document.getElementById("granularity");
    const previous = select.value;
    const options = [];

    if (timeframe === "Realtime") {
      options.push("Minutes");
    } else if (timeframe === "Today" || timeframe === "Yesterday") {
      options.push("Minutes", "Hours");
    } else {
      options.push("Hours", "Days");
    }

    select.innerHTML = "";
    options.forEach((value) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = value;
      select.appendChild(option);
    });

    select.value = options.includes(previous) ? previous : options[0];

    let nextMetric = metricOverride || getActiveMetric();
    if (timeframe === "Realtime" && !METRICS.find((metric) => metric.key === nextMetric)?.showInRealtime) {
      nextMetric = "UniqueVisitors";
    }

    updateUrl(nextMetric);
    fetchDashboard().catch((error) => console.error(error));
  }

  function formatDuration(seconds) {
    if (!seconds && seconds !== 0) return "-";
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  }

  function metricTitle(metricKey) {
    return METRICS.find((metric) => metric.key === metricKey)?.label || metricKey;
  }

  function rawMetricValue(metricKey, item) {
    switch (metricKey) {
      case "Visits":
        return item.total_visits ?? item.visits ?? 0;
      case "Pageviews":
        return item.total_pageviews ?? item.pageviews ?? 0;
      case "AvgVisitDuration":
        return item.avg_visit_duration ?? 0;
      case "BounceRate":
        return item.bounce_rate ?? 0;
      default:
        return item.unique_visitors ?? item.visitors ?? item.current_visits ?? 0;
    }
  }

  function metricValue(metricKey, item) {
    switch (metricKey) {
      case "Visits":
        return item.visits ?? item.total_visits ?? 0;
      case "Pageviews":
        return item.pageviews ?? item.total_pageviews ?? 0;
      case "AvgVisitDuration":
        return formatDuration(item.avg_visit_duration ?? 0);
      case "BounceRate":
        return `${item.bounce_rate ?? 0}%`;
      default:
        return item.visitors ?? item.unique_visitors ?? 0;
    }
  }

  function renderMetrics(data) {
    const container = document.getElementById("metrics");
    const timeframe = document.getElementById("timeframe").value;
    const activeMetric = getQueryParams().metric;
    document.getElementById("currentVisitorsCount").textContent = String(data.realtime_aggregates.current_visits ?? 0);

    const cards = METRICS
      .filter((metric) => timeframe !== "Realtime" || metric.showInRealtime)
      .map((metric) => {
        let value = rawMetricValue(metric.key, data.aggregates);
        if (metric.key === "AvgVisitDuration") value = formatDuration(value);
        if (metric.key === "BounceRate") value = `${value}%`;
        return { key: metric.key, label: metric.label, value };
      });

    container.innerHTML = cards
      .map(
        (card) => `
          <button type="button" class="metric-card${activeMetric === card.key ? " is-active" : ""}" data-metric="${card.key}">
            <div class="metric-card__label">${escapeHtml(card.label)}</div>
            <div class="metric-card__value">${escapeHtml(String(card.value))}</div>
            <div class="metric-card__hint">Click to update the trend chart</div>
          </button>
        `,
      )
      .join("");

    container.querySelectorAll("[data-metric]").forEach((button) => {
      button.addEventListener("click", () => updateGranularity(button.dataset.metric));
    });
  }

  function formatAxis(metric, value) {
    if (metric === "AvgVisitDuration") return formatDuration(value);
    if (metric === "BounceRate") return `${value}%`;
    return `${value}`;
  }

  function formatChartLabel(timestamp, granularity) {
    const date = new Date(timestamp * 1000);
    if (granularity === "Days") {
      return date.toLocaleDateString([], { month: "short", day: "numeric" });
    }
    return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  }

  function renderChart(data) {
    const container = document.getElementById("chart");
    const metric = getActiveMetric();
    const granularity = document.getElementById("granularity").value;
    document.getElementById("chartTitle").textContent = metricTitle(metric);

    if (!Array.isArray(data.stats) || data.stats.length === 0) {
      container.innerHTML = '<div class="chart-empty">No data for this range.</div>';
      return;
    }

    const values = data.stats.map(([, value]) => Number(value) || 0);
    const labels = data.stats.map(([timestamp]) => formatChartLabel(timestamp, granularity));
    const width = 1080;
    const height = 320;
    const paddingLeft = 44;
    const paddingRight = 16;
    const paddingTop = 14;
    const paddingBottom = 32;
    const usableWidth = width - paddingLeft - paddingRight;
    const usableHeight = height - paddingTop - paddingBottom;
    const maxValue = Math.max(...values, 1);
    const yTicks = Array.from({ length: 5 }, (_, index) => Math.round(maxValue - (maxValue * index) / 4));

    const points = values.map((value, index) => {
      const x = paddingLeft + (values.length === 1 ? usableWidth / 2 : (usableWidth * index) / (values.length - 1));
      const y = paddingTop + usableHeight - (value / Math.max(maxValue, 1)) * usableHeight;
      return { x, y };
    });

    const areaPoints = [
      `${points[0].x},${height - paddingBottom}`,
      ...points.map((point) => `${point.x},${point.y}`),
      `${points[points.length - 1].x},${height - paddingBottom}`,
    ].join(" ");
    const solidPoints = points.slice(0, -1).map((point) => `${point.x},${point.y}`).join(" ");
    const previousPoint = points[points.length - 2];
    const lastPoint = points[points.length - 1];
    const xLabelStep = Math.max(Math.ceil(labels.length / 6), 1);

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" class="w-full" role="img" aria-label="${escapeHtml(metricTitle(metric))} chart">
        <defs>
          <linearGradient id="dashboard-chart-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stop-color="rgba(85, 103, 255, 0.18)"></stop>
            <stop offset="100%" stop-color="rgba(85, 103, 255, 0.02)"></stop>
          </linearGradient>
        </defs>
        ${yTicks.map((value, index) => {
          const y = paddingTop + (usableHeight * index) / Math.max(yTicks.length - 1, 1);
          return `
            <g>
              <line x1="${paddingLeft}" x2="${width - paddingRight}" y1="${y}" y2="${y}" stroke="rgba(148, 163, 184, 0.22)"></line>
              <text class="chart-axis-label" x="8" y="${y + 4}">${escapeHtml(formatAxis(metric, value))}</text>
            </g>
          `;
        }).join("")}
        <polygon points="${areaPoints}" fill="url(#dashboard-chart-fill)"></polygon>
        ${solidPoints ? `<polyline points="${solidPoints}" fill="none" stroke="var(--accent-line)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></polyline>` : ""}
        ${previousPoint ? `<line x1="${previousPoint.x}" y1="${previousPoint.y}" x2="${lastPoint.x}" y2="${lastPoint.y}" stroke="var(--accent-line)" stroke-width="2.5" stroke-linecap="round" stroke-dasharray="4 4"></line>` : ""}
        <circle cx="${lastPoint.x}" cy="${lastPoint.y}" r="3.5" fill="var(--accent-line)"></circle>
      </svg>
      <div class="mt-4 flex items-center justify-between gap-3 overflow-hidden text-xs text-[var(--text-soft)]">
        ${labels.map((label, index) => (index % xLabelStep === 0 || index === labels.length - 1 ? `<span>${escapeHtml(label)}</span>` : "")).join("")}
      </div>
    `;
  }

  function addFilterValue(state, fieldId, value, mode) {
    const entry = state[fieldId];
    if (!entry || !value) return;
    entry.include = entry.include.filter((item) => item !== value);
    entry.exclude = entry.exclude.filter((item) => item !== value);
    if (mode === "exclude") {
      entry.exclude.push(value);
    } else {
      entry.include.push(value);
    }
  }

  function removeFilterValue(state, fieldId, value) {
    const entry = state[fieldId];
    if (!entry) return;
    entry.include = entry.include.filter((item) => item !== value);
    entry.exclude = entry.exclude.filter((item) => item !== value);
  }

  function toggleFilterMode(state, fieldId, value) {
    const entry = state[fieldId];
    if (!entry) return;
    if (entry.include.includes(value)) {
      entry.include = entry.include.filter((item) => item !== value);
      if (!entry.exclude.includes(value)) entry.exclude.push(value);
      return;
    }
    if (entry.exclude.includes(value)) {
      entry.exclude = entry.exclude.filter((item) => item !== value);
      if (!entry.include.includes(value)) entry.include.push(value);
      return;
    }
    entry.include.push(value);
  }

  function activeFilterChips(state) {
    const sourceState = state || filterStateFromUrl();
    const chips = [];
    FILTER_FIELDS.forEach((field) => {
      sourceState[field.id].include.forEach((value) => chips.push({ fieldId: field.id, fieldLabel: field.label, value, mode: "include" }));
      sourceState[field.id].exclude.forEach((value) => chips.push({ fieldId: field.id, fieldLabel: field.label, value, mode: "exclude" }));
    });
    return chips;
  }

  function renderFilterChips(chips) {
    const container = document.getElementById("activeFilters");
    const clearButton = document.getElementById("clearFilters");
    clearButton.classList.toggle("hidden", chips.length === 0);

    container.innerHTML = chips.map((chip) => `
      <div class="filter-editor-chip">
        <span class="filter-editor-chip__segment filter-editor-chip__segment--field">${escapeHtml(chip.fieldLabel)}</span>
        <button type="button" class="filter-editor-chip__toggle${chip.mode === "exclude" ? " is-exclude" : ""}" data-chip-action="toggle" data-chip-field="${chip.fieldId}" data-chip-value="${escapeHtml(chip.value)}">${chip.mode === "exclude" ? "is not" : "is"}</button>
        <span class="filter-editor-chip__segment">${escapeHtml(chip.value)}</span>
        <button type="button" class="filter-editor-chip__remove" data-chip-action="remove" data-chip-field="${chip.fieldId}" data-chip-value="${escapeHtml(chip.value)}">×</button>
      </div>
    `).join("");

    container.querySelectorAll("[data-chip-action]").forEach((button) => {
      button.addEventListener("click", async () => {
        const state = filterStateFromUrl();
        const fieldId = button.dataset.chipField;
        const value = button.dataset.chipValue;
        if (button.dataset.chipAction === "remove") {
          removeFilterValue(state, fieldId, value);
        } else {
          toggleFilterMode(state, fieldId, value);
        }
        applyFilterState(state);
      });
    });
  }

  async function loadFilterValues() {
    const response = await fetch(`${getBasePath()}/api/filter-values?${buildDashboardRequest(filterStateFromUrl()).toString()}`);
    if (!response.ok) {
      throw new Error(`filter values request failed: ${response.status}`);
    }
    filterValuesCache = await response.json();
  }

  function closeFilterMenus() {
    document.getElementById("filterFieldMenu").classList.add("hidden");
    document.getElementById("filterValueMenu").classList.add("hidden");
    document.getElementById("filterValueSearch").value = "";
    currentFilterSearch = "";
    currentFilterField = null;
  }

  function renderFieldMenu() {
    const list = document.getElementById("filterFieldMenuList");
    list.innerHTML = FILTER_FIELDS.map((field) => `
      <button type="button" class="filter-menu__item" data-filter-field="${field.id}">
        <span class="filter-menu__item-label">${escapeHtml(field.label)}</span>
      </button>
    `).join("");

    list.querySelectorAll("[data-filter-field]").forEach((button) => {
      button.addEventListener("click", async () => {
        currentFilterField = button.dataset.filterField;
        currentFilterSearch = "";
        document.getElementById("filterFieldMenu").classList.add("hidden");
        await loadFilterValues();
        renderValueMenu();
        document.getElementById("filterValueMenu").classList.remove("hidden");
        document.getElementById("filterValueSearch").focus();
      });
    });
  }

  function filteredValueOptions() {
    if (!currentFilterField) return [];
    const field = FILTER_FIELD_MAP[currentFilterField];
    const values = filterValuesCache?.[field.dataKey] || [];
    const search = currentFilterSearch.trim().toLowerCase();
    if (!search) return values;
    return values.filter((value) => String(value).toLowerCase().includes(search));
  }

  function renderValueMenu() {
    const list = document.getElementById("filterValueMenuList");
    const values = filteredValueOptions();
    if (values.length === 0) {
      list.innerHTML = '<div class="filter-menu__empty">No matching values.</div>';
      return;
    }

    list.innerHTML = values.map((value) => `
      <button type="button" class="filter-menu__item" data-filter-value="${escapeHtml(String(value))}">
        <span class="filter-menu__item-label">${escapeHtml(String(value))}</span>
      </button>
    `).join("");

    list.querySelectorAll("[data-filter-value]").forEach((button) => {
      button.addEventListener("click", () => {
        const state = filterStateFromUrl();
        addFilterValue(state, currentFilterField, button.dataset.filterValue, "include");
        closeFilterMenus();
        applyFilterState(state);
      });
    });
  }

  function applyFilterState(state) {
    const params = currentUrlParams();
    writeFilterStateToParams(params, state);
    window.history.pushState({}, "", `${window.location.pathname}?${params.toString()}`);
    fetchDashboard().catch((error) => console.error(error));
  }

  function renderTable(targetId, rows, groupingKey, filterKey, metric, tone) {
    const target = document.getElementById(targetId);
    if (!Array.isArray(rows) || rows.length === 0) {
      target.innerHTML = '<div class="panel-empty-state">No data for this range.</div>';
      return;
    }

    const filterState = filterStateFromUrl();
    const maxValue = Math.max(...rows.map((row) => Number(rawMetricValue(metric, row)) || 0), 1);
    const body = rows.map((row) => {
      const value = row[groupingKey] || "Unknown";
      const active = filterState[filterKey]?.include.includes(value) || filterState[filterKey]?.exclude.includes(value);
      const normalized = Math.max((Number(rawMetricValue(metric, row)) / maxValue) * 100, 12);
      return `
        <button type="button" class="metric-table__row${active ? " is-active" : ""}${tone === "cool" ? " metric-table__row--cool" : ""}" data-filter-key="${filterKey}" data-filter-value="${escapeHtml(String(value))}">
          <span class="metric-table__primary">
            <span class="metric-table__fill" style="width:${Math.min(normalized, 100)}%"></span>
            <span class="metric-table__label">${escapeHtml(String(value))}</span>
          </span>
          <span class="metric-table__count">${escapeHtml(String(metricValue(metric, row)))}</span>
        </button>
      `;
    }).join("");

    target.innerHTML = `<div class="metric-table">${body}</div>`;
    target.querySelectorAll("[data-filter-key]").forEach((button) => {
      button.addEventListener("click", () => {
        const state = filterStateFromUrl();
        addFilterValue(state, button.dataset.filterKey, button.dataset.filterValue, "include");
        applyFilterState(state);
      });
    });
  }

  function renderBreakdowns(data) {
    const metric = getActiveMetric();
    Object.entries(GROUPS).forEach(([groupName, config]) => {
      const activeValue = getActiveGroupValue(groupName);
      document.getElementById(config.labelId).textContent = config.valueToLabel[activeValue];
      renderTable(config.tableId, data[`${groupName}_metrics`], config.valueToField[activeValue], config.valueToFilter[activeValue], metric, config.tone);
    });
  }

  async function fetchDashboard() {
    renderFilterChips(activeFilterChips());
    const response = await fetch(`${getBasePath()}/api/filtered-statistics?${buildDashboardRequest(filterStateFromUrl()).toString()}`);
    if (!response.ok) {
      throw new Error(`dashboard request failed: ${response.status}`);
    }
    const data = await response.json();
    renderMetrics(data);
    renderChart(data);
    renderBreakdowns(data);
  }

  function syncControlsFromQuery() {
    const initial = getQueryParams();
    document.getElementById("timeframe").value = initial.timeframe;
    document.getElementById("granularity").value = initial.granularity;
    setActiveGroupValue("location", initial.locationGrouping);
    setActiveGroupValue("device", initial.deviceGrouping);
    setActiveGroupValue("source", initial.sourceGrouping);
    setActiveGroupValue("page", initial.pageGrouping);
  }

  function attachListeners() {
    syncControlsFromQuery();

    ["timeframe", "granularity"].forEach((id) => {
      document.getElementById(id).addEventListener("change", () => updateGranularity());
    });

    document.querySelectorAll(".panel-tab").forEach((button) => {
      button.addEventListener("click", () => {
        setActiveGroupValue(button.dataset.group, button.dataset.value);
        updateUrl();
        fetchDashboard().catch((error) => console.error(error));
      });
    });

    document.getElementById("filterButton").addEventListener("click", async (event) => {
      event.stopPropagation();
      const fieldMenu = document.getElementById("filterFieldMenu");
      const isOpen = !fieldMenu.classList.contains("hidden");
      closeFilterMenus();
      if (!isOpen) {
        renderFieldMenu();
        fieldMenu.classList.remove("hidden");
      }
    });

    document.getElementById("filterValueSearch").addEventListener("input", (event) => {
      currentFilterSearch = event.target.value;
      renderValueMenu();
    });

    document.getElementById("currentVisitorsTrigger").addEventListener("click", () => {
      document.getElementById("timeframe").value = "Realtime";
      updateGranularity("UniqueVisitors");
    });

    document.getElementById("clearFilters").addEventListener("click", () => {
      applyFilterState(createEmptyFilterState());
    });

    document.addEventListener("click", (event) => {
      if (!event.target.closest("#filterPopover")) {
        closeFilterMenus();
      }
    });

    window.addEventListener("popstate", () => {
      syncControlsFromQuery();
      closeFilterMenus();
      fetchDashboard().catch((error) => console.error(error));
    });
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  document.addEventListener("DOMContentLoaded", () => {
    attachListeners();
    updateGranularity(getQueryParams().metric);
    window.setInterval(() => {
      fetchDashboard().catch((error) => console.error(error));
    }, 10000);
  });
})();
