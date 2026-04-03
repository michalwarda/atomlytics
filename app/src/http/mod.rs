use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use tracing::error;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    analytics::{calculate_source, extract_domain, extract_path, extract_utm_params, AnalyticsService},
    auth,
    domain::{DashboardQuery, DashboardResponse, DomainSummary, EventPayload, FilterValuesResponse},
    geo::GeoLocation,
    ui,
    AppState,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        health_check,
        serve_script,
        track_event,
        list_domains,
        domain_dashboard,
        filtered_statistics,
        filter_values,
        statistics
    ),
    components(
        schemas(
            EventPayload,
            DashboardQuery,
            DashboardResponse,
            FilterValuesResponse,
            DomainSummary
        )
    ),
    tags(
        (name = "tracker", description = "Public ingestion endpoints"),
        (name = "dashboard", description = "Domain-scoped dashboard endpoints")
    )
)]
pub struct ApiDoc;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/", get(list_domains))
        .route("/{domain}", get(redirect_domain_dashboard))
        .route("/{domain}/dashboard", get(domain_dashboard))
        .route("/{domain}/api/filtered-statistics", get(filtered_statistics))
        .route("/{domain}/api/statistics", get(statistics))
        .route("/{domain}/api/filter-values", get(filter_values))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::basic_auth,
        ));

    Router::new()
        .route("/health", get(health_check))
        .route("/script.js", get(serve_script))
        .route("/api/event", post(track_event))
        .merge(protected)
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .nest_service("/assets", tower_http::services::ServeDir::new("app/src/assets"))
        .with_state(state)
}

#[utoipa::path(get, path = "/health", responses((status = 200, description = "Health check succeeded")), tag = "tracker")]
pub async fn health_check() -> StatusCode {
    StatusCode::OK
}

#[utoipa::path(get, path = "/script.js", responses((status = 200, description = "Embeddable tracker script")), tag = "tracker")]
pub async fn serve_script() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        include_str!("../assets/tracker.js"),
    )
}

#[utoipa::path(
    post,
    path = "/api/event",
    request_body = EventPayload,
    responses((status = 201, description = "Event persisted"), (status = 400, description = "Invalid event payload")),
    tag = "tracker"
)]
pub async fn track_event(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(payload): Json<EventPayload>,
) -> Result<StatusCode, StatusCode> {
    let analytics = AnalyticsService::new(state.db.clone());
    let ip = forwarded_ip(&headers, addr);
    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");

    let Some(domain) = extract_domain(&payload.page_url) else {
        return Err(StatusCode::BAD_REQUEST);
    };

    let page_url_path = extract_path(&payload.page_url);
    let (utm_source, utm_medium, utm_campaign, utm_content, utm_term) =
        extract_utm_params(&payload.page_url);
    let referrer = payload.referrer.clone();
    let source = calculate_source(&referrer, &utm_source);
    let user_agent_info = state.geo.parse_user_agent(user_agent);
    let location = demo_geo_override(&headers, state.config.allow_demo_geo_override)
        .unwrap_or_else(|| state.geo.lookup_location(&ip));
    let now = Utc::now();
    let salt = analytics
        .ensure_daily_salt(now.date_naive())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let visitor_id = analytics.visitor_id(&salt, &domain, &ip, user_agent);

    let mut event = crate::domain::EnrichedEvent {
        payload: EventPayload {
            referrer,
            ..payload
        },
        domain: domain.clone(),
        page_url_path,
        source,
        browser: user_agent_info.browser,
        operating_system: user_agent_info.operating_system,
        device_type: user_agent_info.device_type,
        country: location.country,
        region: location.region,
        city: location.city,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        timestamp: now,
        visitor_id: visitor_id.clone(),
        visit_id: None,
    };

    match analytics.find_active_visit(&domain, &visitor_id, now).await {
        Ok(Some(active_visit)) => {
            event.visit_id = Some(active_visit.id);
            analytics
                .update_visit(&active_visit, &event)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
        Ok(None) => {
            let visit_id = analytics
                .create_visit(&event)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            event.visit_id = Some(visit_id);
        }
        Err(error) => {
            error!("failed to resolve active visit: {error}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    analytics
        .save_event(&event)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::CREATED)
}

#[utoipa::path(get, path = "/", responses((status = 200, description = "Tracked domain list")), tag = "dashboard")]
pub async fn list_domains(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let analytics = AnalyticsService::new(state.db.clone());
    let domains = analytics
        .list_domains()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(ui::domains_page(&domains)))
}

async fn redirect_domain_dashboard(Path(domain): Path<String>) -> Redirect {
    Redirect::temporary(&format!("/{domain}/dashboard"))
}

#[utoipa::path(get, path = "/{domain}/dashboard", params(("domain" = String, Path, description = "Tracked hostname")), responses((status = 200, description = "Dashboard page")), tag = "dashboard")]
pub async fn domain_dashboard(Path(domain): Path<String>) -> Html<String> {
    Html(ui::dashboard_page(&domain))
}

#[utoipa::path(
    get,
    path = "/{domain}/api/filtered-statistics",
    params(
        ("domain" = String, Path, description = "Tracked hostname"),
        DashboardQuery
    ),
    responses((status = 200, body = DashboardResponse)),
    tag = "dashboard"
)]
pub async fn filtered_statistics(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<DashboardResponse>, StatusCode> {
    let analytics = AnalyticsService::new(state.db.clone());
    analytics
        .get_dashboard_data(&domain, &query)
        .await
        .map(Json)
        .map_err(|error| {
            error!(?error, %domain, ?query, "failed to get dashboard data");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

#[utoipa::path(
    get,
    path = "/{domain}/api/statistics",
    params(("domain" = String, Path, description = "Tracked hostname"), DashboardQuery),
    responses((status = 200, body = DashboardResponse)),
    tag = "dashboard"
)]
pub async fn statistics(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<DashboardResponse>, StatusCode> {
    let analytics = AnalyticsService::new(state.db.clone());
    analytics
        .get_dashboard_data(&domain, &query)
        .await
        .map(Json)
        .map_err(|error| {
            error!(?error, %domain, ?query, "failed to get statistics");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

#[utoipa::path(
    get,
    path = "/{domain}/api/filter-values",
    params(("domain" = String, Path, description = "Tracked hostname"), DashboardQuery),
    responses((status = 200, body = FilterValuesResponse)),
    tag = "dashboard"
)]
pub async fn filter_values(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<FilterValuesResponse>, StatusCode> {
    let analytics = AnalyticsService::new(state.db.clone());
    analytics
        .get_filter_values(&domain, &query)
        .await
        .map(Json)
        .map_err(|error| {
            error!(?error, %domain, ?query, "failed to get filter values");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

fn forwarded_ip(headers: &HeaderMap, addr: SocketAddr) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next().map(str::trim))
        .map(ToString::to_string)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| addr.ip().to_string())
}

fn demo_geo_override(headers: &HeaderMap, enabled: bool) -> Option<GeoLocation> {
    if !enabled {
        return None;
    }

    Some(GeoLocation {
        country: header_value(headers, "x-atomlytics-demo-country")?,
        region: header_value(headers, "x-atomlytics-demo-region")?,
        city: header_value(headers, "x-atomlytics-demo-city")?,
    })
}

fn header_value(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
