use leptos::prelude::*;

use crate::domain::DomainSummary;

#[derive(Clone, Copy)]
struct TabOption {
    value: &'static str,
    label: &'static str,
}

pub fn render_shell(content: impl IntoView, title: &str, page_script: Option<&str>) -> String {
    let page_script = page_script.map(ToString::to_string);
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>{title.to_string()}</title>
                <link rel="stylesheet" href="/assets/app.css" />
            </head>
            <body>
                {content}
                {page_script.map(|script| view! { <script src={script}></script> })}
            </body>
        </html>
    }
    .to_html()
}

pub fn domains_page(domains: &[DomainSummary]) -> String {
    render_shell(view! { <DomainList domains=domains.to_vec() /> }, "Atomlytics", None)
}

pub fn dashboard_page(domain: &str) -> String {
    render_shell(
        view! { <Dashboard domain=domain.to_string() /> },
        &format!("Atomlytics · {domain}"),
        Some("/assets/dashboard.js"),
    )
}

#[component]
fn DomainList(domains: Vec<DomainSummary>) -> impl IntoView {
    view! {
        <main class="page-shell">
            <section class="page-frame page-frame--wide">
                <header class="page-hero">
                    <div class="page-hero__copy">
                        <p class="page-eyebrow">"Atomlytics"</p>
                        <h1 class="page-title">"Choose a tracked domain"</h1>
                        <p class="page-subtitle">
                            "Dashboards are scoped by hostname. Each tracked site gets its own analytics route under /{domain}/dashboard."
                        </p>
                    </div>
                    <div class="page-summary-card">
                        <span class="page-summary-card__label">"Tracked domains"</span>
                        <strong class="page-summary-card__value">{domains.len()}</strong>
                        <p class="page-summary-card__hint">"Domains appear automatically once events are ingested."</p>
                    </div>
                </header>

                <section class="domain-grid">
                    {if domains.is_empty() {
                        view! {
                            <article class="domain-empty-state">
                                <h2>"No domains tracked yet"</h2>
                                <p>
                                    "Start sending events to /api/event and this page will populate automatically."
                                </p>
                            </article>
                        }
                            .into_any()
                    } else {
                        domains
                            .into_iter()
                            .map(|domain| view! { <DomainCard domain=domain /> })
                            .collect_view()
                            .into_any()
                    }}
                </section>
            </section>
        </main>
    }
}

#[component]
fn DomainCard(domain: DomainSummary) -> impl IntoView {
    let href = format!("/{}/dashboard", domain.domain);
    let last_seen = domain
        .last_seen_at
        .map(|timestamp| timestamp.to_string())
        .unwrap_or_else(|| "Never".to_string());

    view! {
        <a class="domain-card" href={href}>
            <div class="domain-card__header">
                <div>
                    <p class="domain-card__label">"Domain"</p>
                    <h2 class="domain-card__title">{domain.domain}</h2>
                </div>
                <span class="domain-card__action">"Open dashboard"</span>
            </div>
            <dl class="domain-card__stats">
                <div>
                    <dt>"Total visits"</dt>
                    <dd>{domain.total_visits}</dd>
                </div>
                <div>
                    <dt>"Last 24h"</dt>
                    <dd>{domain.visits_last_24h}</dd>
                </div>
            </dl>
            <p class="domain-card__footnote">"Last activity epoch: " {last_seen}</p>
        </a>
    }
}

#[component]
fn Dashboard(domain: String) -> impl IntoView {
    view! {
        <main class="dashboard-shell" data-domain=domain.clone() id="dashboard-root">
            <section class="dashboard-surface">
                <header class="dashboard-topbar">
                    <div class="dashboard-topbar__left">
                        <a class="dashboard-backlink" href="/">"Atomlytics"</a>
                        <span class="dashboard-separator">"•"</span>
                        <span class="dashboard-domain-name">{domain.clone()}</span>
                        <button class="dashboard-live-indicator" id="currentVisitorsTrigger" type="button">
                            <span class="dashboard-live-indicator__dot"></span>
                            <strong id="currentVisitorsCount">"0"</strong>
                            <span>"current visitors"</span>
                        </button>
                    </div>
                    <div class="dashboard-topbar__right">
                        <div class="filter-popover" id="filterPopover">
                            <button class="toolbar-button" id="filterButton" type="button">
                                "Filters"
                            </button>
                            <div class="filter-menu hidden" id="filterFieldMenu">
                                <div class="filter-menu__list" id="filterFieldMenuList"></div>
                            </div>
                            <div class="filter-menu filter-menu--values hidden" id="filterValueMenu">
                                <div class="filter-menu__search">
                                    <input autocomplete="off" id="filterValueSearch" placeholder="Search values" type="text" />
                                </div>
                                <div class="filter-menu__list" id="filterValueMenuList"></div>
                            </div>
                        </div>
                        <select class="toolbar-select" id="timeframe">
                            <option value="Realtime">"Realtime"</option>
                            <option selected value="Today">"Today"</option>
                            <option value="Yesterday">"Yesterday"</option>
                            <option value="Last7Days">"Last 7 Days"</option>
                            <option value="Last30Days">"Last 30 Days"</option>
                            <option value="AllTime">"All Time"</option>
                        </select>
                    </div>
                </header>

                <div class="active-filter-row" id="activeFilters"></div>
                <button class="clear-filters-button hidden" id="clearFilters" type="button">
                    "Clear filters"
                </button>

                <section class="hero-card">
                    <div class="stats-strip" id="metrics"></div>
                    <div class="chart-header">
                        <div>
                            <p class="chart-header__eyebrow">"Trend"</p>
                            <h2 class="chart-header__title" id="chartTitle">"Unique visitors"</h2>
                        </div>
                        <select class="toolbar-select toolbar-select--small" id="granularity">
                            <option value="Minutes">"Minutes"</option>
                            <option selected value="Hours">"Hours"</option>
                            <option value="Days">"Days"</option>
                        </select>
                    </div>
                    <div class="chart-surface" id="chart"></div>
                </section>

                <section class="breakdown-grid">
                    <BreakdownPanel
                        group="source"
                        table_id="source-table"
                        label_id="source-column-label"
                        default_column_label="Source"
                        tabs=vec![
                            TabOption { value: "Source", label: "Sources" },
                            TabOption { value: "Referrer", label: "Referrers" },
                            TabOption { value: "Campaign", label: "Campaigns" },
                        ]
                    />
                    <BreakdownPanel
                        group="page"
                        table_id="page-table"
                        label_id="page-column-label"
                        default_column_label="Page"
                        tabs=vec![
                            TabOption { value: "Page", label: "Top pages" },
                            TabOption { value: "EntryPage", label: "Entry pages" },
                            TabOption { value: "ExitPage", label: "Exit pages" },
                        ]
                    />
                    <BreakdownPanel
                        group="location"
                        table_id="location-table"
                        label_id="location-column-label"
                        default_column_label="Country"
                        tabs=vec![
                            TabOption { value: "Country", label: "Countries" },
                            TabOption { value: "Region", label: "Regions" },
                            TabOption { value: "City", label: "Cities" },
                        ]
                    />
                    <BreakdownPanel
                        group="device"
                        table_id="device-table"
                        label_id="device-column-label"
                        default_column_label="Browser"
                        tabs=vec![
                            TabOption { value: "Browser", label: "Browsers" },
                            TabOption { value: "OperatingSystem", label: "Operating systems" },
                            TabOption { value: "DeviceType", label: "Devices" },
                        ]
                    />
                </section>
            </section>

        </main>
    }
}

#[component]
fn BreakdownPanel(
    group: &'static str,
    table_id: &'static str,
    label_id: &'static str,
    default_column_label: &'static str,
    tabs: Vec<TabOption>,
) -> impl IntoView {
    view! {
        <article class="panel-card">
            <header class="panel-card__header">
                <DashboardTabs group=group tabs=tabs />
            </header>
            <div class="panel-table-head">
                <span id={label_id}>{default_column_label}</span>
                <span>"Visitors"</span>
            </div>
            <div id={table_id} class="panel-table-body"></div>
        </article>
    }
}

#[component]
fn DashboardTabs(group: &'static str, tabs: Vec<TabOption>) -> impl IntoView {
    view! {
        <div class="panel-tabs" data-tab-group=group>
            {tabs
                .into_iter()
                .enumerate()
                .map(|(index, tab)| {
                    view! {
                        <button
                            aria-pressed={if index == 0 { "true" } else { "false" }}
                            class={if index == 0 { "panel-tab is-active" } else { "panel-tab" }}
                            data-group={group}
                            data-value={tab.value}
                            type="button"
                        >
                            {tab.label}
                        </button>
                    }
                })
                .collect_view()}
        </div>
    }
}
